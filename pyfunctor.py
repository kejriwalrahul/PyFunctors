"""
	To Do:
		1. explore stream-in stream-out data

	Requirements for parallel execution:
		1. __name__ == "__main__" is reqd
		2. Functors should be defined at module top level
		3. stripe_data must always be outer-decorators for pipeliners 
"""

import os, errno
import io
import pickle
import ctypes
from tqdm import tqdm
from functools import partial, wraps
from multiprocessing import Pool
from contextlib import contextmanager


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

hash_file_path = lambda x, y: x + str(y) + ".pkl"

def hex_hash(val):
	return hex(ctypes.c_size_t(hash(val)).value)

def next_hash(hashval, functor_name):
	return hex_hash(str(hashval) + functor_name)

"""
	Abstract Data Representation that can be manipulated using custom Functors
"""
class AbstractSpace:

	def __init__(self, data, hashval="", parallelize=True, num_threads=None, cache_dir="data_cache/"):
		self.data = data
		self.hash = hex_hash(hashval)
		self.parallelize = parallelize
		self.num_threads = num_threads
		self.cache_dir = cache_dir

	def values(self):
		return self.data

	def apply(self, func, *args, **kwargs):
		if not self.parallelize:
			self.__serial_apply(func, *args, **kwargs)
		else:
			self.__parallel_apply(func, *args, **kwargs)
		self.hash = next_hash(self.hash, func.__name__)
		return self

	def filter_space(self, filter_function):
		# Note: effects not counted towards caching
		self.data = filter(filter_function, tqdm(self.data)) 
		return self

	def flatten(self):
		# Note: effects not counted towards caching
		self.data = [subel for el in self.data if el != None for subel in el]
		return self

	def write_to_file(self, filename, data_formatter):
		# Note: effects not counted towards caching
		with io.open(filename, "w", encoding="utf8") as fout:
			for el in tqdm(self.data):
				print el
				fout.write(u"%s\n" % data_formatter(el).rstrip())

	def __serial_apply(self, func, *args, **kwargs):
		print "Serially Applying ", func.__name__
		partial_func = partial(func, *args, **kwargs)
		self.data = map(partial_func, tqdm(self.data))

	def __parallel_apply(self, func, *args, **kwargs):
		print "Parallely Applying ", func.__name__, "on %s threads" % str(self.num_threads)
		partial_func = partial(func, *args, **kwargs)
		with PoolContextWrapper(Pool(processes=self.num_threads)) as pool:
			self.data = list(tqdm(pool.imap(partial_func, self.data), total=len(self.data)))

	def cache_data(self, hashval, functor_name):
		mkdir_p(self.cache_dir)
		hpath = hash_file_path(self.cache_dir, next_hash(hashval, functor_name))
		with open(hpath, "wb") as cache_file:
			pickle.dump((self.data, self.hash), cache_file)

	def read_cache(self, functor_name):
		hpath = hash_file_path(self.cache_dir, next_hash(self.hash, functor_name))
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
	def wrapper(self, *args, **kwargs):
		return func(*args, el=self, **kwargs)

	setattr(AbstractSpace, func.__name__, wrapper)
	return wrapper


"""
	Builds Functor Compositions which cache output results for reuse
"""
def cache_pipeliner(func):

	@wraps(func)
	def wrapper(self, *args, **kwargs):
		if not self.read_cache(func.__name__):
			orig_hash = self.hash
			return_val = func(*args, el=self, **kwargs)
			self.cache_data(orig_hash, func.__name__)
			return return_val
		else:
			return self	

	setattr(AbstractSpace, func.__name__, wrapper)	
	return wrapper


"""
	Stripe data inputs to pipeline in mini-batches
"""
def stripe_data(batch_size=1000):

	def decorator(func):

		@wraps(func)
		def wrapper(self, *args, **kwargs):

			# Maintain data context
			alldata_in = self.data
			alldata_out = []
			
			# Stripe data and process
			num_iters = 1 + float(len(alldata_in)) / batch_size
			for i in range(int(num_iters)):
				self.data = alldata_in[i*batch_size : (i+1)*batch_size]
				func(self, *args, **kwargs)
				alldata_out += self.data
			
			self.data = alldata_out

			# Return AbstractSpace object for chaining
			return self

		setattr(AbstractSpace, func.__name__, wrapper)	
		return wrapper

	return decorator


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