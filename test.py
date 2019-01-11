from pyfunctor import AbstractSpace, functorize

# import pyfunctor
# pyfunctor.DEBUG = True

@functorize
def adder(x, el):
	return el + x

@functorize
def multiplier(y, el):
	return el * y

if __name__ == "__main__":

	space = AbstractSpace([1,2,3])
	print space.multiplier(5).adder(3).values()