import logging
import socket
import threading
import time
from collections import deque

from bencode import bdecode, bencode

from magnet_crawler.utils import get_random_id

BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("router.bitcomet.com", 6881),
    ("dht.transmissionbt.com", 6881)
)
MAX_NODES_SIZE = 10000
BUFSIZE = 1024


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
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((bind_ip, bind_port))
        # 存放发现的nodes
        self.nodes = deque(maxlen=MAX_NODES_SIZE)

    def join_dht(self):
        for addr in BOOTSTRAP_NODES:
            self.send_find_node_request(addr)
            time.sleep(1)

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
            print('sending')
            self.udp_socket.sendto(bencode(data).encode(encoding='utf8'), address)
            print("I'm sending to {}".format(address))
        except Exception:
            pass

    def handle_receive_things(self, data, address):
        """处理接收到的所以信息"""
        try:
            y = data.get('y')
            # 关键字 y = 'r', 表示当前是回复
            # y = 'q', 表示当前是请求
            if y == 'r':
                if data.get('r'):
                    self.handle_find_node_response(data)
            elif y == 'q':
                # 关键字 q , 表示当前请求的方法名
                q = data.get('q')
                if q == 'get_peers':
                    if data.get('a'):
                        self.handle_get_peers_request(data, address)
                elif q == 'announce_peer':
                    if data.get('a'):
                        self.handle_announce_peer_request(data, address)
        except KeyError:
            pass

    def handle_find_node_response(self, data):
        """处理 find_node 的回复"""
        try:
            tid = data.get('t')
            nid = data.get('r').get('id')
            nodes = data.get('r').get('nodes')
            print(nodes)
            # TODO: 储存 nodes
        except KeyError:
            pass

    def handle_get_peers_request(self, data, address):
        """处理外部发来的 get_peers 请求，储存 infohash """
        try:
            tid = data.get('t')
            nid = data.get('a').get('id')
            info_hash = data.get('a').get('info_hash')
            print(info_hash)
            # TODO: 储存 info_hash, 并回复
        except KeyError:
            pass

    def handle_announce_peer_request(self, data, address):
        """处理外部发来的 announce_peer 请求， 储存 infohash """
        try:
            tid = data.get('t')
            nid = data.get('a').get('id')
            info_hash = data.get('a').get('info_hash')
            print(info_hash)
            # TODO: 储存 info_hash, 并回复
        except KeyError:
            pass

    def receive_forever(self):
        """一直接收外部发来的信息"""
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(BUFSIZE)
                print(threading.current_thread().name + "I'm received")
                self.handle_receive_things(bdecode(data), addr)
            except:
                pass

    def send_forever(self):
        """一直对外发送信息，即 find_node """
        while True:
            try:
                node = self.nodes.popleft()
                print(threading.current_thread().name + 'send find_node request')
                self.send_find_node_request((node.ip, node.port), node.nid)
                time.sleep(1)
            except IndexError:
                self.join_dht()


