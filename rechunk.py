import time
from pp_to_nice_netcdf import SlurmLogger
import cf
from netCDF4 import Dataset

logging = SlurmLogger()

def get_chunking_hack(filename, variable):

    nc = Dataset(filename)
    var = nc.variable[variable]
    return var.chunking()


def rechunk(infile, field_number, outfile, new_chunk_shape, **kw):
    """ 
    Read a file, rechunk a specific field, and write it out with
    the appropriate keywords
    """

    t1 = time()
    g = cf.read(infile)[field_number]
    #g.data.nc_hdf5_chunksizes()
    ncvar = g.nc_get_variable()
    old_chunk_shape = get_chunking_hack(infile,ncvar)
    
    logging.info(f'Assuming dimension order T, Z, Y, X for chunkshape {old_chunk_shape}')
    
    dask_chunks = {k:v for k,v in zip(['T','Z','Y','X'],old_chunk_shape)}    
    f = cf.read(infile, chunks=dask_chunks)[field_number] 
    t2 = time()
    
    logging.info(f'Lazy read (twice) of {infile} (chunk shape = {old_chunk_shape}) in {t2-t1:.2f}s')

    f.data.nc_set_hdf5_chunksizes=new_chunk_shape
    cf.write(f, outfile, **kw)
    t3 = time()

    logging.info(f'Wrote {outfile} with chunk_shape {new_chunk_shape} in {t3-t2:.2f}s')

if __name__=="__main__":
    infile= 'tmp-infile-for-repack.nc'
    new_shape = [8, 1, 113, 1280]
    number = 0
    outfile = 'tmp-outfile-from-rechunk.nc'
    rechunk(infile, number, outfile, new_shape, compress=4, shuffle=True)
     