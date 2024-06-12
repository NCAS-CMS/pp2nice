# pp2nice

This package provides some tools for working with pp data, and in particular
for converting it to netcdf and potentially moving it to an object store.

There is support for

1. Converting entire streams of pp output to netCDF, and in doing so
2. Optionally moving the netCDF files to object store as you go, rather than leaving them on disk.

The netCDF that is produced is "CMOR-lite", that is, it has some useful metadata, it is completely
CF compliant, and the filenames are "DRS-like".  The objective is that CMIP6 users will be able to
find their way around this data, and some of the more obvious issues with provenance are dealt with.

## Usage

As the output data is going to be compressed and chunked, and the chunking will be different than
that of the pp files, this is a slow process. It is expected you will use batch computing, though
you should test a couple off conversions before you set everything going to make sure you are happy
with the output metadata, and in particular, the output file names.

1. Copy the example file `eg_n1280.py` to the directory where you are going to control things from and edit it according to _your_ requirements. It is important that you go through it and change the values for everything down to "executable code follows". Then you can run it, with
`python your_copied_and_edited_file.py` which should produce a new json file with whatever name you chose for `config_name`.
     - Pay careful attention to the subdirs list, that's where you pick the streams you are converting, and how many fields you want in your output netcdf files.
2. Now copy `runner.py` to the same place, make sure the config file matches what you have just put in your config_file, and choose your S3 alias and bucket. (The alias should match something you have in your minio config file at `~/.mc/config.json`, the bucket can be a new one if necessary).
3. 

