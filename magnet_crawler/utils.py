import random
import string
from _socket import inet_ntoa

from struct import unpack

# 每个node节点信息的长度
COMPACT_NODE_INFO_LENGTH = 26
# 每个node节点长度
COMPACT_NODE_LENGTH = 20


def get_random_id(length):
    str_list = [random.choice(string.digits + string.ascii_letters) for i in range(length)]
    random_str = ''.join(str_list)
    return random_str


def parse_nodes(data):
    nodes = []
    if data:
        length = len(data)
        # 每个node信息，20：nid 4：ip 2：port
        for i in range(0, length, COMPACT_NODE_INFO_LENGTH):
            nid = data[i:i + 20]
            ip = inet_ntoa(data[i + 20:i + 24])
            port = unpack("!H", data[i + 24:i + 26])[0]
            nodes.append((nid, ip, port))

    return nodes


def parse_info_hash(data):
    # info_hash 以16进制储存
    magnet = data.hex().upper()
    # info_hash = codecs.getencoder("hex")(data)[0].decode().upper()
    return magnet


if __name__ == '__main__':
    print(get_random_id(4))
