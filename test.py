from pyfunctor import AbstractSpace, functorize, sanitizor, pipeliner

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

@pipeliner
def LinearTransformPipeline(x, y, el):
	return el.multiplier(x).adder(y)

if __name__ == "__main__":
 
	space = AbstractSpace([1,2,3])
	print space.LinearTransformPipeline(5,3).values()