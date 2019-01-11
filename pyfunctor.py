"""
	To Do:
		1. explore stream-in stream-out data
		2. cacheability of results
		3. type-checking

	Requirements for parallel execution:
		1. __name__ == "__main__" is reqd
		2. Functors should be defined at module top level
"""

from tqdm import tqdm
from functools import partial
from multiprocessing import Pool
from contextlib import contextmanager


"""
	Config
"""
NUM_THREADS = 2
DEBUG = False


@contextmanager
def PoolContextWrapper(thing):
    try:
        yield thing
    finally:
        thing.terminate()


"""
	Abstract Data Representation that can be manipulated using custom Functors
"""
class AbstractSpace:

	def __init__(self, data):
		self.data = data

	def values(self):
		return self.data

	def serial_apply(self, func, *args, **kwargs):
		partial_func = partial(func, *args, **kwargs)
		self.data = map(partial_func, tqdm(self.data))

	def parallel_apply(self, func, *args, **kwargs):
		partial_func = partial(func, *args, **kwargs)
		with PoolContextWrapper(Pool(processes=NUM_THREADS)) as pool:
			self.data = list(tqdm(pool.imap(partial_func, self.data), total=len(self.data)))


"""
	Builds Functors for manipulating AbstractSpace
"""
def functorize(func):
	
	def wrapper(self, *args, **kwargs):
		if DEBUG:
			self.serial_apply(func, *args, **kwargs)
		else:
			self.parallel_apply(func, *args, **kwargs)
		return self

	setattr(AbstractSpace, func.__name__, wrapper)
	return func