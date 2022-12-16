from pathos.multiprocessing import Pool
from tools.log.logger import Logger
from typing import Callable
import os
import signal


# to ignore events being sent to process children. The parent must
# take care of the event handling, e.g., keyboard interrupts
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)



class WorkerPool(Logger):

    '''
    worker pool gets a worker routine function and spawns a certain 
    number of processes with that routine.

    worker_routine: the worker function to be spawn and executed
    number_of_workers: number of workers to be created and executed

    after instantiation call the execute method and pass along the argument
    you want to send to the worker. 
    '''

    def __init__(self, worker_routine: Callable, number_of_workers: int=os.cpu_count()) -> None:

        # instantiate a process-safe logger
        Logger.__init__(self, "worker_pool")

        # validate the inputs before proceeding
        if not isinstance(number_of_workers, int):
            raise TypeError("number of workers is not correct")

        if not callable(worker_routine):
            raise TypeError("no worker specified")

        # limit the number of worker to the number of cpus 
        self.__number_of_workers = min(number_of_workers, os.cpu_count())
        self.__worker_routine = worker_routine

        if(self.__number_of_workers < number_of_workers):
            self.logger.warning(f"number of cpus limit dictated {self.__number_of_workers} workers instead of {number_of_workers} you provided")
        


    def execute(self, *args, **kwargs):
        
        self.logger.info(f"starting worker pool with {os.cpu_count()} cpus and {self.__number_of_workers} workers")

        # augment worker's arguments with its id
        def worker_routine_wrapper(worker_number):
            kwargs["worker_number"] = worker_number
            self.__worker_routine(*args, **kwargs)


        # Use initializer to ignore SIGINT in child processes
        with Pool(initializer=init_worker) as mp_pool:
            
            try:
            
                mp_pool.map(
                        worker_routine_wrapper,
                        range(self.__number_of_workers)
                    )
            
            except KeyboardInterrupt:
                self.logger.info("worker pool shutting down")
                return




