from pyfunctor import AbstractSpace, functorize, pipeliner, cache_pipeliner, stripe_data
import math

@functorize
def adder(x, el):
	if el == 5:
		raise NotImplementedError
	return el + x

@functorize
def multiplier(y, el):
	return el * y

@functorize
def sigmoid(el):
	return 1.0 / (1 + math.exp(el))

@pipeliner
def LinearTransformPipeline(x, y, el):
	return el.multiplier(x).adder(y)

@stripe_data(batch_size=3)
@cache_pipeliner
def SigmoidNeuronTransform(x, y, el):
	return el.LinearTransformPipeline(x, y).sigmoid()

if __name__ == "__main__":
 
	space = AbstractSpace([1, 2, 3, 4, 5], parallelize=True, hashval="testvals")
	print space.SigmoidNeuronTransform(5,3).values()