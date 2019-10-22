import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

now = lambda : time.time() * 1000

def mission(jobseq, jobid):
    # consume = 0.5
    # consume = random.random()
    consume = random.randint(2,4)
    # print(f"{jobseq} {jobid} ... consume {consume}")
    time.sleep(consume)
    # print(f"{jobseq} {jobid} done.")
    return jobseq, jobid, consume

def _concurrent_submit(jobseq, job_count):
    print(f"[{jobseq}][{job_count}] entry _concurrent_submit")
    results = []
    t0 = now()
    with ThreadPoolExecutor(max_workers=job_count) as executor:
        print(f"[{jobseq}][{job_count}] entry ThreadPoolExecutor")
        t1 = now()
        futures = [ executor.submit(mission, jobseq, jobid+1) for jobid in range(job_count) ]
        print(f"[{jobseq}][{job_count}] before t2")
        t2 = now()
        results = [ future.result() for future in futures ]
        print(f"[{jobseq}][{job_count}] before t3")
        t3 = now()
        avg = (t2 - t1) / job_count
        print(f"[{jobseq}][{job_count}] {t1-t0:.4f} {t2-t1:.4f} {job_count} {avg:.4f} ms =====")
    return results

class Ditto(object):
    def __init__(self):
        self._max_workers = 10
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._queue = queue.SimpleQueue()
        self._cunsumer = threading.Thread(target=self._consume)
        self._cunsumer.start()
        self._hasJob = True
        self.results = []

    def _consume(self):
        print("entry _consume")
        while True:
            try:
                if self._queue.qsize():
                    print(f"got {self._queue.qsize()}")
                    result = self._queue.get()
                    self.results.append(result.result())
                if not self._hasJob:
                    break
            except Exception as e:
                print(e)
                break

    def run(self, jobs):
        for jobseq, job_count in enumerate(jobs):
            result = self._executor.submit(_concurrent_submit, jobseq, job_count)
            self._queue.put(result)
            # result.result()
            print(f"[{jobseq}] submit package done. (qsize: {self._queue.qsize()})")
            time.sleep(1)

        self._hasJob = False
        self._cunsumer.join()
        print("-=-=-=-=")

if __name__ == "__main__":
    jobs = [ random.randint(10, 50) for _ in range(5)]
    ditto = Ditto()
    time.sleep(1)
    ditto.run(jobs)
    print("done job")
    
    print(ditto.results)
    print(len(ditto.results))