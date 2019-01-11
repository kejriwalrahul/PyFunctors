from pyfunctor import AbstractSpace, functorize, sanitizor

# import pyfunctor
# pyfunctor.DEBUG = True

@functorize
@sanitizor
def adder(x, el):
	if el == 5:
		raise NotImplementedError
	return el + x

@functorize
@sanitizor
def multiplier(y, el):
	return el * y

if __name__ == "__main__":

	space = AbstractSpace([1,2,3])
	print space.multiplier(5).adder(3).values()