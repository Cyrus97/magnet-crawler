import socket
import time
from bencoder import bdecode, bencode
from collections import deque
from multiprocessing import Process
from os import cpu_count
from threading import Thread

from magnet_crawler.database import RedisClient
from magnet_crawler.utils import get_random_id, parse_nodes, parse_info_hash, get_logger

BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
]
MAX_NODES_SIZE = 10000
BUFSIZE = 10240
# 发送间隔时间
SLEEP_TIME = 1e-6
MAGNET_TEMPLATE = "magnet:?xt=urn:btih:{}"
SERVER_HOST = '0.0.0.0'
DEFAULT_SERVER_PORT = 10086
DEFAULT_SERVER_COUNT = cpu_count()
TIMER_WAIT_TIME = 60


class DHTNode:
    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTServer:
    def __init__(self, bind_ip, bind_port, name):
        """

        :param bind_ip: 绑定的ip
        :param bind_port: 绑定的端口
        :param name: 该server的名字
        """
        # 自己也是一个node
        self.node = DHTNode(get_random_id(20), bind_ip, bind_port)
        # 使用udp
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.udp_socket.bind((bind_ip, bind_port))
        # 存放发现的nodes
        self.nodes = deque(maxlen=MAX_NODES_SIZE)
        self.magnets = set()
        self.redis_client = RedisClient()
        self.logger = get_logger(name)
        self.logger.info("I'am {}, I'm bound at port:{}.".format(name, bind_port))

    def join_dht(self):
        """从本地提供的节点加入 DHT 网络"""
        for addr in BOOTSTRAP_NODES:
            self.send_find_node_request(addr)
            time.sleep(SLEEP_TIME)

    def send_find_node_request(self, address, nid=None):
        """
        发送 find_node 请求，以便收到更多的 DHT 节点

        对发送数据的说明
        ===============

        't': transaction ID, 由请求节点产生，长度不定，目的是定位当前信息的唯一性，1byte-->对应2^8个请求

        'y': 'q', 代表当前是请求

        'q': 'find_node', 请求方法(get_peers, announce_peer)

        'a': {
                'id': node_id, 请求节点的 id

                'target': node_id, 正在查找的节点 id
            }

        """
        nid = nid if nid else self.node.nid
        tid = get_random_id(4)
        target = get_random_id(20)
        data = {
            't': tid,
            'y': 'q',
            'q': 'find_node',
            'a': {
                'id': nid,
                'target': target
            }
        }
        self.send_krpc(data, address)

    def send_krpc(self, data, address):
        """发送 krpc 信息"""
        self.logger.debug("I'm sending to {}".format(address))
        try:
            self.udp_socket.sendto(bencode(data), address)
        except Exception:
            self.logger.exception(Exception)

    def handle_receive_things(self, data, address):
        """处理接收到的所有信息"""
        try:
            y = data.get(b'y')
            # 关键字 y = 'r', 表示当前是回复
            # y = 'q', 表示当前是请求
            if y == b'r':
                if data.get(b'r'):
                    self.handle_find_node_response(data)
            elif y == b'q':
                # 关键字 q , 表示当前请求的方法名
                q = data.get(b'q')
                if q == b'get_peers':
                    if data.get(b'a'):
                        self.handle_get_peers_request(data, address)
                elif q == b'announce_peer':
                    if data.get(b'a'):
                        self.handle_announce_peer_request(data, address)
        except KeyError:
            pass
            # self.logger.exception(KeyError)

    def handle_find_node_response(self, data):
        """
        处理 find_node 的回复

        ‘r': {
                'id': 发送方的 node id

                'nodes': 离 target 最近的 k 个节点
            }
        """
        self.logger.debug("I'm handling find_node_response")
        try:
            tid = data.get(b't')
            nid = data.get(b'r').get(b'id')
            nodes = data.get(b'r').get(b'nodes')
            nodes = parse_nodes(nodes)
            for node in nodes:
                nid, ip, port = node
                if len(nid) == 20 and ip != SERVER_HOST:
                    self.nodes.append(DHTNode(nid, ip, port))
        except KeyError:
            pass
            # self.logger.exception(KeyError)

    def handle_get_peers_request(self, data, address):
        """
        处理外部发来的 get_peers 请求，使用 info_hash 转为 magnet

        'a': {
                'id': node_id, 请求节点的 id

                'info_hash': 请求的资源的 info_hash
            }
        """
        self.logger.debug("I'm handling get_peers_request")
        try:
            tid = data.get(b't')
            nid = data.get(b'a').get(b'id')
            info_hash = data.get(b'a').get(b'info_hash')
            magnet = parse_info_hash(info_hash)
            # TODO: 储存 info_hash, 并回复
            self.save_magnet(magnet)
        except KeyError:
            pass
            # self.logger.exception(KeyError)

    def handle_announce_peer_request(self, data, address):
        """
        处理外部发来的 announce_peer 请求，使用 info_hash 转为 magnet

        'a': {
                'id': node_id, 请求节点的 id

                'info_hash': 请求的资源的 info_hash
            }
        """
        self.logger.debug("I'm handling announce_peer_request")
        print(data)
        try:
            tid = data.get(b't')
            nid = data.get(b'a').get(b'id')
            info_hash = data.get(b'a').get(b'info_hash')
            magnet = parse_info_hash(info_hash)
            # TODO: 储存 info_hash, 并回复
            self.save_magnet(magnet)
        except KeyError:
            pass
            # self.logger.exception(KeyError)

    def receive_forever(self):
        """一直接收外部发来的信息"""
        self.logger.info('start receive forever...')
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(BUFSIZE)
                self.handle_receive_things(bdecode(data), addr)
            except Exception:
                pass
                # self.logger.exception(Exception)

    def send_forever(self):
        """一直对外发送信息，即发送 find_node"""
        self.logger.info('start send forever...')
        while True:
            try:
                node = self.nodes.popleft()
                self.send_find_node_request((node.ip, node.port), node.nid)
                time.sleep(SLEEP_TIME)
            except IndexError:
                self.join_dht()

    def save_magnet(self, magnet):
        self.logger.info(MAGNET_TEMPLATE.format(magnet))
        self.magnets.add(MAGNET_TEMPLATE.format(magnet))
        self.redis_client.add(MAGNET_TEMPLATE.format(magnet))

    def reporter(self):
        """定时报告当前状况"""
        while True:
            time.sleep(TIMER_WAIT_TIME)
            self.join_dht()
            self.logger.info('当前有{}个节点, 有{}个磁力链接'.format(len(self.nodes), len(self.magnets)))


def start_server(index=0, bind_port=DEFAULT_SERVER_PORT):
    dht_s = DHTServer(SERVER_HOST, bind_port, 'SERVER{}'.format(index))
    threads = [
        Thread(target=dht_s.send_forever),
        Thread(target=dht_s.receive_forever),
        Thread(target=dht_s.reporter)
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def start_multi_server(count=DEFAULT_SERVER_COUNT, origin_bind_port=DEFAULT_SERVER_PORT):
    # signal.signal(signal.SIGINT, handler)
    # signal.signal(signal.SIGTERM, handler)

    processes = []
    try:
        for i in range(count):
            p = Process(target=start_server, args=(i, origin_bind_port + i,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print('退出')


if __name__ == '__main__':
    start_multi_server()
