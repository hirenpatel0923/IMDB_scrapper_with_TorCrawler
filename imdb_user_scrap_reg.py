import requests, csv
import time
import multiprocessing 
import concurrent.futures
from TorCrawler import TorCrawler

def seq(url, crawler, session):
    crawler.rotate()
    r = session.get(url)
    return r.status_code

def worker(workerQueue, session, lst):
    crawler = TorCrawler()
    lst = ["http://www.imdb.com/title/tt" + '%08d' %(i) +"/reviews?spoiler=hide&sort=reviewVolume&dir=desc&ratingFilter=0" for i in lst]
    with concurrent.futures.ThreadPoolExecutor(5) as thread_ex:
        f_t_u = {thread_ex.submit(seq, url, crawler, session): url for url in lst}
        for x in concurrent.futures.as_completed(f_t_u):
            #time.sleep(2)
            try:
                code = x.result()
                if code == 200:
                    with open(multiprocessing.current_process().name + '.csv', 'a') as file:
                        print(multiprocessing.current_process().name, ' => ', f_t_u[x])
                        file.write(f_t_u[x] + "\n")
                elif code == 503:
                    with open(multiprocessing.current_process().name + '-503.csv', 'a') as file:
                        print(multiprocessing.current_process().name, ' => ', code)
                        file.write(f_t_u[x] + "\n")
                else:
                    print(multiprocessing.current_process().name,' => ',code)
            except requests.HTTPError as e:
                #print(e)
                continue
            except requests.ConnectTimeout as e:
                #print(e)
                continue
            except requests.ConnectionError as e:
                #print(e)
                continue
    

if __name__ == '__main__':
    session = requests.Session()
    session.proxies = {}
    session.proxies['http'] = 'socks5h://localhost:9150'
    session.proxies['https'] = 'socks5h://localhost:9150'

    start_cpu = time.clock()
    start = time.time()
    workerQueue = multiprocessing.Queue()
    procs = []
    
    base = 40000000
    diff = 20000

    for i in range(10):
        start = base + (diff * i)
        end = start + diff
        lst = [x for x in range(start, end)]
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

    