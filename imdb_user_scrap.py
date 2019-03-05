import requests, csv
from bs4 import BeautifulSoup
import pandas as pd
import time
import multiprocessing 
import concurrent.futures
from TorCrawler import TorCrawler
from get_movie_details import get_basic_details

def seq(url, crawler, session):
    crawler.rotate()
    r = session.get(url)
    return r.status_code

def worker(workerQueue, session, lst):
    crawler = TorCrawler()
    lst = ["http://www.imdb.com/title/" + i for i in lst]
    with concurrent.futures.ThreadPoolExecutor(5) as thread_ex:
        f_t_u = {thread_ex.submit(seq, url, crawler, session): url for url in lst}
        for x in concurrent.futures.as_completed(f_t_u):
            code = x.result()
            try:
                if code == 200:
                    page = session.get(f_t_u[x])
                    page_soup = BeautifulSoup(page.text, 'html.parser')
                    get_basic_details(multiprocessing.current_process().name, page_soup, f_t_u[x])
            except Exception as e:
                print(e)
                continue
    

if __name__ == '__main__':
    session = requests.Session()
    session.proxies = {}
    session.proxies['http'] = 'socks5h://localhost:9150'
    session.proxies['https'] = 'socks5h://localhost:9150'

    df = pd.read_csv('data.csv')
    df = df.loc[df['region'] == 'US']
    df = df.drop_duplicates(['titleId'])


    start_cpu = time.clock()
    start = time.time()
    workerQueue = multiprocessing.Queue()
    procs = []
    
    base = 80000
    diff = 2000

    for i in range(10):
        start = base + (diff * i)
        end = start + diff
        lst = df['titleId'][start:end]
        p = multiprocessing.Process(target=worker, args=(workerQueue, session, lst))
        procs.append(p)
        p.start()
 
    workerQueue.close()
    workerQueue.join_thread()

    for proc in procs:
        proc.join()
    stop = time.time()
    print('CPU time:', time.clock() - start_cpu)
    print('Execution time:', stop - start)

    