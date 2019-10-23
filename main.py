import json
import logging
import queue
import random
import sys
import threading
import time
import traceback
from argparse import ArgumentParser
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests

now = lambda : time.time()

LOGGER = logging.getLogger(name='ditto')
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO) 

# def mission_demo(jobseq, jobid):
#     resp_time = random.randint(2,4)
#     time.sleep(resp_time)
#     return jobseq, jobid, resp_time

def _concurrent_submit(jobseq, job_count, mission):
    results = []
    with ThreadPoolExecutor(max_workers=job_count) as executor:
        futures = [ executor.submit(mission, jobseq, jobid+1) for jobid in range(job_count) ]
        results = [ future.result() for future in futures ]
    return {
        "jobseq": jobseq,
        "results": results
    }

class Ditto(object):
    def __init__(self, static_mission):
        self._static_mission = static_mission
        self._amplified_factor = 1.5
        self._dynamic_workers = 10
        self._executor = ThreadPoolExecutor(max_workers=self._dynamic_workers)
        self._queue = queue.Queue()
        self._isSubmitDone = False
        self.results = []
        self.counter = Counter()
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
                    self.counter.update(result['results'])
                    LOGGER.info(f"<<-- [_consume] jobseq {result['jobseq']} done. (qsize: {self._queue.qsize()}, # used workers: {len(self._executor._threads)}, max workers: {self._executor._max_workers} )")
                elif self._isSubmitDone:
                    break
                else:
                    time.sleep(0.5) # prevent CPU up to 100%                    
            except Exception as e:
                LOGGER.warning(e, exc_info=True)
                break
    
    def _produce(self, jobseq, job_count):
        result = self._executor.submit(_concurrent_submit, jobseq, job_count, self._static_mission)
        self._queue.put(result)
        self._adjust_worker_count()
        LOGGER.info(f"---> [_produce][{jobseq}] submit done. (qsize: {self._queue.qsize()}, # used workers: {len(self._executor._threads)}, max workers: {self._executor._max_workers} )")

    def run(self, jobs):
        for jobseq, job_count in enumerate(jobs):
            if job_count: self._produce(jobseq, job_count)
            time.sleep(1)

        self._isSubmitDone = True
        self._cunsumer.join()

def make_static_mission(scenario):
    URL = scenario['url']
    USE_BODY = False
    DATA = None
    if scenario['method'].lower() == 'get':
        REQ = requests.get
    elif scenario['method'].lower() == 'post':
        REQ = requests.post
        USE_BODY = True
        DATA = scenario.get('body')
    else:
        raise ValueError('given method ({}) is not supported.'.format(scenario['method']))
    
    def mission(jobseq, jobid):
        header = scenario.get('header', {})
        if USE_BODY:
            header.update({ "Content-Type": "application/json" })
        try:
            r = REQ(URL, headers=header, json=DATA)
            return r.status_code
        except Exception as err:
            LOGGER.debug(traceback.format_exc())
            return type(err).__name__
    return mission

if __name__ == "__main__":    
    try:
        parser = ArgumentParser()
        parser.add_argument("apidef", help="API definition file, see 'example.json' for example.")
        argv = parser.parse_args()

        scenario = json.load(open(argv.apidef, errors='ignore'))
        jobs = scenario['reqpersec']
        fn = make_static_mission(scenario)

        ditto = Ditto(static_mission=fn)
        
        LOGGER.info("---- [START]")
        t0 = now()
        ditto.run(jobs)
        LOGGER.info("---- [END]")
        dt = now() - t0
        LOGGER.info("---- Job done. entire spent: {:.1f} s".format(dt))
        LOGGER.info("---- Result stats:\n{}".format(json.dumps(ditto.counter, indent=4)))
    except Exception:
        LOGGER.error(traceback.format_exc())
        sys.exit(1)
