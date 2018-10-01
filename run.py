from threading import Thread

from magnet_crawler.server import DHTServer

if __name__ == '__main__':
    dhts = DHTServer('127.0.0.1', 10086)
    threads = [
        Thread(target=dhts.send_forever),
        Thread(target=dhts.receive_forever)
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()