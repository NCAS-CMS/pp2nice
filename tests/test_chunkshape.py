import numpy as np
from pp2nice import get_chunkshape

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


    
