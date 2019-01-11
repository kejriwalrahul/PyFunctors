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
from functools import partial, wraps
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

	def apply(self, func, *args, **kwargs):
		if DEBUG:
			self.serial_apply(func, *args, **kwargs)
		else:
			self.parallel_apply(func, *args, **kwargs)
		return self

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
	
	@wraps(func)
	def wrapper(self, *args, **kwargs):
		return self.apply(func, *args, **kwargs)

	setattr(AbstractSpace, func.__name__, wrapper)
	return func


"""
	Builds Functor Compositions
"""
def pipeliner(func):
	def wrapper(self, *args):
		return func(*args, el=self)
	setattr(AbstractSpace, func.__name__, wrapper)

"""
	Try-Catch Wrapper to prevent errors from stopping entire processing
"""
def sanitizor(func):

	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			return func(*args, **kwargs)
		except:
			return None

	return wrapper