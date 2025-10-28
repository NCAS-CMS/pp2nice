from pp2nice.get_chunkshape import get_chunkshape
import numpy as np

n1280_a = np.array([720, 1920, 2560])
n1280_b = np.array([719, 1920, 2560])

shapes = [n1280_a, n1280_b]
sizes = [1e6, 2e6, 4e6]

def test_chunkshapes():

    for shape in shapes:
        for volume in sizes:
            result = get_chunkshape(shape, volume, logging=True)
            for x,y in zip(shape, result):
                try:
                    assert x%y == 0
                except:
                    raise ValueError(f'Chunk size {result} does not fit into {shape}')
            