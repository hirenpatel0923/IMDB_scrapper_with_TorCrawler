import time
from TorCrawler import TorCrawler

c = TorCrawler()

while True:
    c.check_ip()
    c.rotate()