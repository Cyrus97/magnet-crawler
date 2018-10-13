import argparse
from multiprocessing import Process

from magnet_crawler.crawler import start_multi_server, DEFAULT_SERVER_COUNT, DEFAULT_SERVER_PORT
from magnet_crawler.database import create_tables
from magnet_crawler.magnet2torrent import start_magnet_converter


def start_all(crawler_args, converter_args):
    processes = [
        Process(target=start_multi_server, args=crawler_args),
        Process(target=start_magnet_converter, args=converter_args),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run for magnet-crawler')

    parser.add_argument("runserver", nargs='?', help='启动')
    parser.add_argument("createdatabase", nargs='?', help='创建数据库', default='magnet.db')
    parser.add_argument("-c", "--count", help="指定爬虫进程数", default=DEFAULT_SERVER_COUNT)
    parser.add_argument("-p", "--port", type=int, help="指定爬虫绑定端口起始位置", default=DEFAULT_SERVER_PORT)
    parser.add_argument("--only-crawler", help="只运行爬虫", action="store_true", dest='crawler')
    parser.add_argument("--only-convert", help="只运行 magnet 转换", action="store_true", dest='convert')

    args = parser.parse_args()

    if args.runserver == 'runserver':
        if args.crawler:
            # 只启动爬虫
            start_multi_server(args.count, args.port)
        elif args.convert:
            # 只启动转换
            start_magnet_converter()
        else:
            # 全部启动
            crawler_args = (args.count, args.port,)
            converter_args = ()
            start_all(crawler_args, converter_args)
    elif args.runserver == 'createdatabase':
        create_tables(args.createdatabase)
