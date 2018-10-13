import json
import os
import tempfile
import time
import xmlrpc.client
from threading import Thread

import websocket

from magnet_crawler.database import RedisClient, REDIS_USED_KEY, REDIS_AVAIL_KEY, SqliteClient, SQLITE_DATABASE_NAME
from magnet_crawler.parse_torrent import TorrentParser
from magnet_crawler.utils import get_logger

RPC_SERVER = "http://localhost:6800/rpc"
RPC_WEBSOCKET = "ws://localhost:6800/jsonrpc"
RPC_SECRET = "token:abcdefg"
# aria2 下载timeout
BT_STOP_TIMEOUT = 600
# aria2 最大下载数量
MAX_DOWNLOADS = 32
# aria2 下载路径
DIR_PATH = os.path.abspath('./torrents')
# 从 redis 每次取出 magnet 的数量
FETCH_MAGNET_COUNT = 32
# 完成一轮下载的等待时间
WAITING_NEXT_TIME = 120
# 每次投入下载的等待时间
SINGLE_DOWNLOAD_WAIT_TIME = 10


def magnet_to_torrent(magnet):
    import libtorrent as lt
    ses = lt.session()
    tempdir = tempfile.mkdtemp()
    params = {
        'save_path': tempdir,
        'storage_mode': lt.storage_mode_t(2),
        'paused': False,
        'auto_managed': True,
        'duplicate_is_error': True
    }
    handle = lt.add_magnet_uri(ses, magnet, params)
    while not handle.has_metadata():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print("Aborting...")
            ses.pause()
    ses.pause()
    print("Done")

    torinfo = handle.get_torrent_info()
    print(torinfo)
    torfile = lt.create_torrent(torinfo)

    output = os.path.abspath(torinfo.name() + ".torrent")


class Aria2MagnetConverter:
    def __init__(self, server, secret=None, **kwargs):
        self.client = xmlrpc.client.ServerProxy(server)
        self.secret = secret
        self.redis_client = RedisClient()
        self.logger = get_logger(kwargs.get('logger_name', 'ARIA2'))
        self.download_info = dict({'all': dict(),
                                   'start': dict(),
                                   'complete': dict(),
                                   'error': dict(),
                                   })
        self.sqlite = SqliteClient(SQLITE_DATABASE_NAME)
        if not os.path.exists(DIR_PATH):
            os.mkdir(DIR_PATH)

    def magnet_to_torrent(self, magnet, dir_path=None, **kwargs):
        # TODO: 从github拉取tracker
        ops = {
            'bt-metadata-only': 'true',  # 只下载种子
            'bt-stop-timeout': str(BT_STOP_TIMEOUT),  # 下载停止超时
            # 'bt-tracker': "udp://tracker.coppersurfer.tk:6969/announce",
        }
        if dir_path:
            ops.update(dir=dir_path)
        if kwargs:
            ops.update(kwargs)
        r = None
        try:
            r = self.client.aria2.addUri(self.secret, [magnet], ops)
            # client.aria2.addTorrent(RPC_SECRET, xmlrpc.client.Binary(open('../test2.torrent', 'rb').read()))
        except Exception:
            self.logger.exception(Exception)

        return r

    def magnet_to_torrent_forever(self):
        self.logger.warning(
            'set max-download={}, start to download torrent and store to database...'.format(MAX_DOWNLOADS))
        # TODO：查询当前下载数量，然后再投放下载任务
        global_ops = {
            'max-concurrent-downloads': str(MAX_DOWNLOADS),
        }
        try:
            if self.secret:
                r = self.client.aria2.changeGlobalOption(self.secret, global_ops)
            else:
                r = self.client.aria2.changeGlobalOption(global_ops)
            if r != 'OK':
                raise Exception('设置失败')
        except Exception:
            self.logger.exception(Exception)

        while True:
            for mgn in self.get_magnets(FETCH_MAGNET_COUNT):
                gid = self.magnet_to_torrent(mgn, DIR_PATH)
                self.logger.info('sending  <{}>  <gid, {}>'.format(mgn.decode(), gid))
                self.save_magnet(mgn, REDIS_USED_KEY)
                self.download_info.get('all').update({gid: mgn})
                time.sleep(SINGLE_DOWNLOAD_WAIT_TIME)
            time.sleep(WAITING_NEXT_TIME)

    def get_magnets(self, count):
        return self.redis_client.get(count)

    def save_magnet(self, magnet, key):
        self.redis_client.add(magnet, key)

    def receive_aria2_notifications(self):
        socket_client = websocket.WebSocket()
        socket_client.connect(RPC_WEBSOCKET)
        # jsonreq = json.dumps({'jsonrpc': '2.0', 'id': 'qwer',
        #                       'method': 'aria2.addUri',
        #                       'params': [RPC_SECRET, ['http://example.org/file'], {}],
        #                       })
        # r = socket_client.send(jsonreq)
        # return an int
        while True:
            resp = socket_client.recv()
            resp = json.loads(resp)
            self.handle_aria2_notifications(resp)

    def handle_aria2_notifications(self, data):
        # {'jsonrpc': '2.0', 'method': 'aria2.onDownloadStart', 'params': [{'gid': '88d5dff6df0c610f'}]}
        # print(data)
        method = data.get('method')
        gid = data.get('params')[0].get('gid')
        # sleep 可以保证 magnet_to_torrent 把 <gid, magnet> 储存在 download_info 里
        # 保证了下面可以正确取到 magnet
        time.sleep(1)
        magnet = self.download_info['all'].get(gid, b'')
        # 如果是之前就有的下载任务, 直接去查找下载信息得到 magnet
        if not magnet:
            magnet = self.extract_magnet_from_status(gid)
        if method == 'aria2.onDownloadStart':
            self.logger.info('start  <{}>  <gid, {}>'.format(magnet.decode(), gid))
            self.download_info.get('start').update({gid: magnet})
            # 加入已使用 magnet
            self.redis_client.add(magnet, REDIS_USED_KEY)
        elif method == 'aria2.onDownloadComplete':
            self.logger.info('complete  <{}>  <gid, {}>'.format(magnet.decode(), gid))
            self.download_info.get('complete').update({gid: magnet})
            # 加入可用 magnet
            self.redis_client.add(magnet, REDIS_AVAIL_KEY)
            # 储存到数据库
            self.save_to_sqlite(magnet.decode())
        elif method in ['aria2.onDownloadError', 'aria2.onDownloadStop']:
            self.logger.warning('error  <{}>  <gid, {}>'.format(magnet.decode(), gid))
            self.download_info.get('error').update({gid: magnet})
            # 因为 stop 的时候，aria2重启时还会开始这个任务，所以要主动删除信息
            self.remove_download_result(gid)
        else:
            pass

    def remove_download_result(self, gid):
        r = self.client.aria2.removeDownloadResult(RPC_SECRET, gid)
        if r != 'OK':
            self.logger.warning('没有成功删除下载信息！')

    def purge_download_result(self):
        r = self.client.aria2.purgeDownloadResult(RPC_SECRET)
        if r != 'OK':
            self.logger.warning('没有成功清除所有下载信息！')

    def save_to_sqlite(self, magnet):
        if not magnet:
            return
        torrent = os.path.join(DIR_PATH, magnet[-40:] + '.torrent')
        if os.path.exists(torrent):
            self.logger.info('save {} to database'.format(magnet))
            parser = TorrentParser(torrent)
            data = parser.get_torrent_info()
            self.sqlite.insert(magnet, data)
        else:
            self.logger.error('不存在该文件 {}'.format(torrent))

    def extract_magnet_from_status(self, gid):
        r = self.client.aria2.tellStatus(RPC_SECRET, gid, ['infoHash'])
        info_hash = r.get('infoHash', None)
        magnet = None
        if info_hash:
            magnet = 'magnet:?xt=urn:btih:' + info_hash.upper()
            magnet = magnet.encode()
        # 也可以这样
        # r = self.client.aria2.getFiles(RPC_SECRET, gid)[0]
        # path = r.get('path', None)
        # magnet = None
        # if path:
        #     magnet = 'magnet:?xt=urn:btih:' + path[-40:]
        #     magnet = magnet.encode()
        return magnet


def start_magnet_converter():
    converter = Aria2MagnetConverter(RPC_SERVER, secret=RPC_SECRET)
    threads = [
        Thread(target=converter.magnet_to_torrent_forever),
        Thread(target=converter.receive_aria2_notifications),
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    start_magnet_converter()
