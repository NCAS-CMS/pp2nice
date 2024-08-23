from pp_to_nice_netcdf import SlurmLogger, rechunk

logging = SlurmLogger()

if __name__=="__main__":
    infile= 'tmp-input-for-repack.nc'
    new_shape = [8, 1, 113, 1280]
    number = 0
    outfile = 'tmp-outfile-from-rechunk.nc'
    rechunk(infile, number, outfile, new_shape, compress=4, shuffle=True)
     