import sys
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

now = lambda : time.time() * 1000

def mission(jobseq, jobid):
    resp_time = random.randint(2,4)
    time.sleep(resp_time)
    return jobseq, jobid, resp_time

def _concurrent_submit(jobseq, job_count):
    results = []
    with ThreadPoolExecutor(max_workers=job_count) as executor:
        futures = [ executor.submit(mission, jobseq, jobid+1) for jobid in range(job_count) ]
        results = [ future.result() for future in futures ]
    return {
        "jobseq": jobseq,
        "results": results
    }

class Ditto(object):
    def __init__(self):
        self._amplified_factor = 1.5
        self._dynamic_workers = 10
        self._executor = ThreadPoolExecutor(max_workers=self._dynamic_workers)
        self._queue = queue.Queue()
        self._isSubmitDone = False
        self.results = []
        self._cunsumer = threading.Thread(target=self._consume)
        self._cunsumer.start()

    def _adjust_worker_count(self):
        if len(self._executor._threads) == self._executor._max_workers:
            self._dynamic_workers = int(self._dynamic_workers * self._amplified_factor)
            self._executor._max_workers = self._dynamic_workers

    def _consume(self):
        while True:
            try:
                if not self._queue.empty():
                    result = self._queue.get().result()
                    self.results.append(result)
                    print(f"[_consume] jobseq {result['jobseq']} done. (qsize: {self._queue.qsize()}, # used workers: {len(self._executor._threads)}, max workers: {self._executor._max_workers} )")
                elif self._isSubmitDone:
                    break
                else:
                    time.sleep(0.5) # prevent CPU up to 100%                    
            except Exception as e:
                print(e)
                break
    
    def _produce(self, jobseq, job_count):
        result = self._executor.submit(_concurrent_submit, jobseq, job_count)
        self._queue.put(result)
        self._adjust_worker_count()
        print(f"[_produce][{jobseq}] submit done. (qsize: {self._queue.qsize()}, # used workers: {len(self._executor._threads)}, max workers: {self._executor._max_workers} )")

    def run(self, jobs):
        for jobseq, job_count in enumerate(jobs):
            self._produce(jobseq, job_count)
            time.sleep(1)

        self._isSubmitDone = True
        self._cunsumer.join()
        print("-=-=-=-=")

if __name__ == "__main__":
    try:
        total = 20
        jobs = [ random.randint(10, 50) for _ in range(total)]
        ditto = Ditto()
        time.sleep(1)
        ditto.run(jobs)
        print("done job")
        print(len(ditto.results))
    except KeyboardInterrupt:
        sys.exit(1)