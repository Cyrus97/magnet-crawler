import logging
import socket
import time
from bencoder import bdecode, bencode
from collections import deque

from magnet_crawler.utils import get_random_id, parse_nodes, parse_info_hash

BOOTSTRAP_NODES = [
    # "udp://tracker.open-internet.nl:6969/announce",
    # "udp://tracker.coppersurfer.tk:6969/announce",
    # "udp://exodus.desync.com:6969/announce",
    # "udp://tracker.opentrackr.org:1337/announce",
    # "udp://tracker.internetwarriors.net:1337/announce",
    # "udp://9.rarbg.to:2710/announce",
    # "udp://public.popcorn-tracker.org:6969/announce",
    # "udp://tracker.vanitycore.co:6969/announce",
    # "https://1.track.ga:443/announce",
    # "udp://tracker.tiny-vps.com:6969/announce",
    # "udp://tracker.cypherpunks.ru:6969/announce",
    # "udp://thetracker.org:80/announce",
    # "udp://tracker.torrent.eu.org:451/announce",
    # "udp://retracker.lanta-net.ru:2710/announce",
    # "udp://bt.xxx-tracker.com:2710/announce",
    # "http://retracker.telecom.by:80/announce",
    # "http://retracker.mgts.by:80/announce",
    # "http://0d.kebhana.mx:443/announce",
    # "udp://torr.ws:2710/announce",
    # "udp://open.stealth.si:80/announce",
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
]
MAX_NODES_SIZE = 10000
BUFSIZE = 10240
MAGNET_TEMPLATE = "magnet:?xt=urn:btih:{}"


class DHTNode:
    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTServer:
    def __init__(self, bind_ip, bind_port):
        # 自己也是一个node
        self.node = DHTNode(get_random_id(20), bind_ip, bind_port)
        # 使用udp
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.udp_socket.bind((bind_ip, bind_port))
        # 存放发现的nodes
        self.nodes = deque(maxlen=MAX_NODES_SIZE)
        self.magnets = []

    def join_dht(self):
        for addr in BOOTSTRAP_NODES:
            self.send_find_node_request(addr)
            time.sleep(0.01)

    def send_find_node_request(self, address, nid=None):
        """发送 find_node 请求，以便收到更多的 DHT 节点"""
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
        try:
            # print("I'm sending to {}".format(address))
            self.udp_socket.sendto(bencode(data), address)
        except Exception:
            logging.exception(Exception)

    def handle_receive_things(self, data, address):
        """处理接收到的所以信息"""
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
            logging.exception(KeyError)

    def handle_find_node_response(self, data):
        """处理 find_node 的回复"""
        # print("I'm handling find_node_response")
        try:
            tid = data.get(b't')
            nid = data.get(b'r').get(b'id')
            nodes = data.get(b'r').get(b'nodes')
            nodes = parse_nodes(nodes)
            for node in nodes:
                nid, ip, port = node
                if len(nid) == 20:
                    self.nodes.append(DHTNode(nid, ip, port))
        except KeyError:
            logging.exception(KeyError)

    def handle_get_peers_request(self, data, address):
        """处理外部发来的 get_peers 请求，储存 infohash """
        print("I'm handling get_peers_request")
        print(data)
        try:
            tid = data.get(b't')
            nid = data.get(b'a').get(b'id')
            info_hash = data.get(b'a').get(b'info_hash')
            print(info_hash)
            magnet = parse_info_hash(info_hash)
            # TODO: 储存 info_hash, 并回复
            self.save_magnet(magnet)
        except KeyError:
            logging.exception(KeyError)

    def handle_announce_peer_request(self, data, address):
        """处理外部发来的 announce_peer 请求， 储存 infohash """
        print("I'm handling announce_peer_request")
        print(data)
        try:
            tid = data.get(b't')
            nid = data.get(b'a').get(b'id')
            info_hash = data.get(b'a').get(b'info_hash')
            print(info_hash)
            magnet = parse_info_hash(info_hash)
            # TODO: 储存 info_hash, 并回复
            self.save_magnet(magnet)
        except KeyError:
            logging.exception(KeyError)

    def receive_forever(self):
        """一直接收外部发来的信息"""
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(BUFSIZE)
                # print(threading.current_thread().name + "I'm received")
                # print(bdecode(data))
                self.handle_receive_things(bdecode(data), addr)
            except Exception:
                logging.exception(Exception)

    def send_forever(self):
        """一直对外发送信息，即 find_node """
        while True:
            try:
                node = self.nodes.popleft()
                # print(threading.current_thread().name + 'send find_node request')
                self.send_find_node_request((node.ip, node.port), node.nid)
                time.sleep(0.01)
            except IndexError:
                self.join_dht()

    def save_magnet(self, magnet):
        print(MAGNET_TEMPLATE.format(magnet))
        self.magnets.append(MAGNET_TEMPLATE.format(magnet))

    def timer(self):
        """定时报告当前状况"""
        t = 0
        while True:
            if t % 60 == 0:
                print('当前有{}个节点'.format(len(self.nodes)))
                print('当前有{}个磁力链接'.format(len(self.magnets)))
                t = 0
            t = t + 1
            time.sleep(1)
