import bencoder
import json
from pprint import pprint

from magnet_crawler.database import SqliteClient, SQLITE_DATABASE_NAME


def parse_torrent():
    with open('../test.torrent', 'rb') as f:
        info = bencoder.bdecode(f.read())
    print(info.keys())
    # for key, value in info.get(b'info').items():
    #     print(key)
    #     print(value)

    with open('../test2.torrent', 'rb') as f:
        info = bencoder.bdecode(f.read())
    print(info.keys())
    print(info[b'nodes'])
    # for key, value in info.get(b'info').items():
    #     print(key)
    #     print(value)


class TorrentParser:
    def __init__(self, torrent):
        self.torrent = self.decode_torrent(torrent)  # all data is byte
        self.info = dict()
        self.encoding = self.torrent.get(b'encoding', b'utf-8').decode()
        self.sqlite3 = SqliteClient(SQLITE_DATABASE_NAME)

    def decode_torrent(self, torrent):
        with open(torrent, 'rb') as f:
            content = bencoder.bdecode(f.read())
        return content

    def get_creation_info(self):
        created_by = self.torrent.get(b'created by', b'').decode()
        creation_date = self.torrent.get(b'creation date', None)  # timestamp
        data = {'created by': created_by,
                'creation date': creation_date}
        return data

    def is_dir(self):
        return b'files' in self.torrent.get(b'info').keys()

    def get_files_info(self):
        files = []
        # 如果没有 'files', 表示下载的是单文件, name 是文件名称
        # 如果有, 'name' 是文件夹名称, 文件全部在 'files' 里
        tor_info = self.torrent[b'info']
        # 作为种子文件的名称
        if b'name.utf-8' in tor_info.keys():
            name = tor_info.get(b'name.utf-8').decode()
        elif b'name' in tor_info.keys():
            name = self.decode_all(tor_info.get(b'name'), self.encoding)
        else:
            name = None
        # name = tor_info.get(b'name.utf-8', tor_info.get(b'name', b'').decode(self.encoding).encode()).decode()
        # print(name)
        self.info.update(name=name)

        if self.is_dir():
            for file in tor_info.get(b'files', []):
                # TODO: 可能有多层文件夹
                length = file.get(b'length', None)
                if b'path.utf-8' in file.keys():
                    name = file.get(b'path.utf-8')[0].decode()
                elif b'path' in file.keys():
                    # 处理多出的一层文件夹
                    if len(file.get(b'path')) == 1:
                        name = self.decode_all(file.get(b'path')[0], self.encoding)
                    else:
                        name = self.decode_all(file.get(b'path')[1], self.encoding)
                else:
                    name = None
                new_file = {
                    'length': length,
                    'name': name,
                }

                # print(new_file)
                files.append(new_file)
        else:
            length = tor_info.get(b'length', None)
            if b'name.utf-8' in tor_info.keys():
                name = tor_info.get(b'name.utf-8').decode()
            elif b'name' in tor_info.keys():
                name = self.decode_all(tor_info.get(b'name'), self.encoding)
            else:
                name = None
            file = {
                'length': length,
                'name': name,
            }
            files.append(file)

        return files

    def decode_all(self, data, encoding=None):
        encoding = encoding if encoding else self.encoding
        dec_data = data
        try:
            dec_data = data.decode(encoding)
        except UnicodeDecodeError as e:
            if 'utf-8' in str(e):
                dec_data = data.decode('gbk')
            elif 'gbk' in str(e):
                dec_data = data.decode('utf-8')
        return dec_data

    def filter_file(self):
        pass

    def get_torrent_info(self):
        self.info.update(self.get_creation_info())
        self.info.update(files=self.get_files_info())
        return self.info


if __name__ == '__main__':
    # parse_torrent()
    tor_parser = TorrentParser('torrents/7c2fa8f559d38e61e8f23d9a2b2728e11173948f.torrent')
    # print(tor_parser.torrent[b'encoding'])
    # print(tor_parser.torrent[b'info'][b'pieces'].decode())
    # print(tor_parser.torrent[b'info'].keys())
    # pprint(tor_parser.get_files_info())
    pprint(tor_parser.get_torrent_info())
    print(json.loads(json.dumps(tor_parser.get_torrent_info())))