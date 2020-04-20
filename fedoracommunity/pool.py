""" Our own threadpool implementation.

- It is only slightly faster than the stdlib implementation.
- It behaves like a generator.
- It is used by the search indexer.

"""

import logging
import threading
import queue

log = logging.getLogger(__name__)


class Worker(object):
    def __init__(self, in_queue, out_queue, func, thread_init):
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.func = func
        self.thread_init = thread_init

    def run(self):
        in_queue = self.in_queue
        out_queue = self.out_queue
        func = self.func
        self.thread_init()
        while True:
            item = in_queue.get() # may block
            if item is None:
                break
            result = func(item)
            out_queue.put(result)
            in_queue.task_done()


class ThreadPool(object):
    def __init__(self, N):
        self.N = N

    def map(self, func, thread_init, items):
        in_queue = queue.Queue(self.N*10)
        out_queue = queue.Queue()

        workers = [
            threading.Thread(target=Worker(in_queue, out_queue, func, thread_init).run)
            for i in range(self.N)]
        workers_working = self.N

        log.info('Starting workers.')
        [worker.start() for worker in workers]
        log.info("workers started")

        for item in items:
            in_queue.put(item) # may block if queue is full
            while True:
                # yield what's already done
                try:
                    result = out_queue.get_nowait()
                except queue.Empty:
                    break
                yield result # consumer will also retrieve the files via API call

        in_queue.join() # wait until all tasks or done

        # stop the workers
        for i in range(workers_working):
            in_queue.put(None)

        # yield the remaining items
        while True:
            try:
                result = opt_queue.get_nowait()
            except Queue.Empty:
                break
            yield result
