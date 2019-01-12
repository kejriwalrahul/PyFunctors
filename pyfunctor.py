"""
	To Do:
		1. explore stream-in stream-out data

	Requirements for parallel execution:
		1. __name__ == "__main__" is reqd
		2. Functors should be defined at module top level
"""

import os, errno
import pickle
import ctypes
from tqdm import tqdm
from functools import partial, wraps
from multiprocessing import Pool
from contextlib import contextmanager


"""
	Config
"""
NUM_THREADS = 2
CACHEDIR = "data_cache/"


"""
	Helpers
"""

@contextmanager
def PoolContextWrapper(thing):
    try:
        yield thing
    finally:
        thing.terminate()

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

hash_file_path = lambda x: CACHEDIR + str(x) + ".pkl"

def hex_hash(val):
	return hex(ctypes.c_size_t(hash(val)).value)

def next_hash(hashval, functor_name):
	return hex_hash(str(hashval) + functor_name)

"""
	Abstract Data Representation that can be manipulated using custom Functors
"""
class AbstractSpace:

	def __init__(self, data, hashval="", parallelize=True):
		self.data = data
		self.hash = hex_hash(hashval)
		self.parallelize = parallelize

	def values(self):
		return self.data

	def apply(self, func, *args, **kwargs):
		if not self.parallelize:
			self.__serial_apply(func, *args, **kwargs)
		else:
			self.__parallel_apply(func, *args, **kwargs)
		self.hash = next_hash(self.hash, func.__name__)
		return self

	def __serial_apply(self, func, *args, **kwargs):
		partial_func = partial(func, *args, **kwargs)
		self.data = map(partial_func, tqdm(self.data))

	def __parallel_apply(self, func, *args, **kwargs):
		partial_func = partial(func, *args, **kwargs)
		with PoolContextWrapper(Pool(processes=NUM_THREADS)) as pool:
			self.data = list(tqdm(pool.imap(partial_func, self.data), total=len(self.data)))

	def cache_data(self, hashval, functor_name):
		mkdir_p(CACHEDIR)
		hpath = hash_file_path(next_hash(hashval, functor_name))
		with open(hpath, "wb") as cache_file:
			pickle.dump((self.data, self.hash), cache_file)

	def read_cache(self, functor_name):
		hpath = hash_file_path(next_hash(self.hash, functor_name))
		if os.path.exists(hpath):
			with open(hpath, "rb") as cache_file:
				self.data, self.hash = pickle.load(cache_file)
				return True
		return False


"""
	Builds Functors for manipulating AbstractSpace
"""
def functorize(func):

	func = sanitizor(func)
	
	@wraps(func)
	def wrapper(self, *args, **kwargs):
		return self.apply(func, *args, **kwargs)

	setattr(AbstractSpace, func.__name__, wrapper)
	return func


"""
	Builds Functor Compositions
"""
def pipeliner(func):

	@wraps(func)
	def wrapper(self, *args):
		return func(*args, el=self)

	setattr(AbstractSpace, func.__name__, wrapper)


"""
	Builds Functor Compositions which cache output results for reuse
"""
def cache_pipeliner(func):

	@wraps(func)
	def wrapper(self, *args):
		if not self.read_cache(func.__name__):
			orig_hash = self.hash
			return_val = func(*args, el=self)
			self.cache_data(orig_hash, func.__name__)
			return return_val
		else:
			return self			

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