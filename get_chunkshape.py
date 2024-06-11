import numpy as np
import math

def get_chunkshape(shape, volume, word_size=4, logging=False, scale_tol=0.8):
    """
    Given a shape tuple, and byte size for the elements, calculate a suitable chunk shape
    for a given volume (in bytes). (We use word instead of dtype in case the user
    changes the data type within the writing operation.)
    """

    def constrained_largest_divisor(number, constraint):
        """ 
        Find the largest divisor of number which is less than the constraint
        """
        for i in range(int(constraint), 1, -1):
            if number % i == 0:
                return i
        return 1

    def revise(dimension, guess):
        """ 
        We need the largest integer (down) less than guess 
        which is a factor of dimension, and we need
        to know how much smaller than guess it is,
        so that other dimensions can be scaled out.
        """
        old_guess = guess
        # there must be a more elegant way of doing this
        guess = constrained_largest_divisor(dimension,old_guess)
        scale_factor = old_guess/guess
        return scale_factor, guess

    v = volume/word_size 
    size = np.prod(shape)
    
    n_chunks = int(size/v)
    root = v**(1/shape.size)
    dlen = np.sum(shape)

    # first get a scaled set of initial guess divisors
    initial_root=np.full(shape.size, root)
    ratios = [x/min(shape) for x in shape]
    other_root = 1.0/(shape.size-1)
    indices = list(range(shape.size))
    for i in indices:
        factor = ratios[i]**other_root
        initial_root[i] = initial_root[i]*ratios[i]
        for j in indices:
            if j==i:
                continue
            initial_root[j] = initial_root[j]/factor
    
    weights_scaling = np.ones(shape.size)

    results = []
    remaining = 1
    for i in indices:
        # can't use zip because we are modifying weights in the loop
        d = shape[i]
        initial_guess = math.ceil(initial_root[i]*weights_scaling[i])
        if d%initial_guess == 0:
            results.append(initial_guess)
        else:
            scale_factor, next_guess = revise(d, initial_guess) 
            results.append(next_guess)
            if remaining < shape.size:
                scale_factor = scale_factor ** (1/(shape.size-remaining))
                weights_scaling[remaining:] = np.full(shape.size-remaining,scale_factor)
        remaining += 1 
        # fix up the last indice as we could have drifted quite small
        if i == indices[-1]:
            size_so_far = np.prod(np.array(results))
            scale_miss = size_so_far/v
            if scale_miss < scale_tol:
                constraint = results[-1]/(scale_miss)
                scaled_up=constrained_largest_divisor(shape[-1],constraint)
                results[-1]=scaled_up

    if logging:
        actual_n_chunks = int(np.prod(np.divide(shape,np.array(results))))
        cvolume =  int(np.prod(np.array(results)) * 4)
        print(f'Chunk size {results} - wanted {int(n_chunks)}/{int(volume)}B will get {actual_n_chunks}/{cvolume}B')
    return results

def test_n1280( volume = 1e6, logging=True):
    shape = np.array([720, 1920, 2560])
    result = get_chunkshape(shape, volume, logging=logging)
    for x,y in zip(shape, result):
        try:
            assert x%y == 0
        except:
            raise ValueError(f'Chunk size {result} does not fit into {shape}')
    size = np.prod(np.array(result)) * 4 

def test_n1280b( volume = 1e6, logging=True):
    shape = np.array([719, 1920, 2560])
    result = get_chunkshape(shape, volume, logging=logging)
    for x,y in zip(shape, result):
        try:
            assert x%y == 0
        except:
            raise ValueError(f'Chunk size {result} does not fit into {shape}')
    size = np.prod(np.array(result)) * 4 


if __name__ == "__main__":
    test_n1280(1e6)
    test_n1280(2e6)
    test_n1280(4e6)
    test_n1280b(2e6)
    
