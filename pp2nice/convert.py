import cf
import datetime
import json
import os
import subprocess
from uuid import uuid4

from pp2nice.logger_utils import get_logger
from pp2nice.upload import move_to_s3
from pp2nice.common_concept import CommonConcepts
from pp2nice.get_chunkshape import get_optimal_chunkshape, get_chunking_hack


def fix_axes(f, logger=None):
    """Add missing 1-dimensional height axes to PP data."""
    logger = logger or get_logger()
    data_axes = f.get_data_axes()
    z_axis = f.domain_axis_key('Z', default=None)
    if z_axis:
        if z_axis in data_axes:
            return f
        # assume we want Z before Y
        try:
            z_position = f.get_data_axes().index(f.domain_axis_key('Y'))
        except ValueError:
            z_position = 0
        f.insert_dimension('Z', position=z_position, inplace=True)
        logger.info(f'New coordinates {f.get_data_axes()}')
    return f


def pp2chunkednc(f, tmpfile, outfile, new_chunk_shape, logger=None, **kw):
    """Write a field to a temporary NetCDF with minimal chunking, then repack."""
    logger = logger or get_logger()
    new_chunk = list(f.data.shape)
    new_chunk[0] = 1
    logger.info(f'Using a temp file with temp chunking {new_chunk}')
    
    f.data.nc_set_hdf5_chunksizes(new_chunk)
    h5_keywords = {k: v for k, v in kw.items() if k in ['shuffle', 'compress']}
    other_keywords = {k: v for k, v in kw.items() if k not in ['shuffle', 'compress']}
    
    cf.write(f, tmpfile, compress=0, shuffle=False, **other_keywords)
    h5repack(tmpfile, outfile, new_chunk_shape, f.nc_get_variable(), h5_keywords, logger=logger)


def h5repack(infile, outfile, new_chunk_shape, var_name, h5_keywords, logger=None):
    """Repack HDF5 file using h5repack CLI."""
    logger = logger or get_logger()
    cmd_list = ['h5repack', '-i', infile, '-o', outfile]

    for k, v in h5_keywords.items():
        if k == 'compress':
            cmd_list.extend(['-f', f'{var_name}:GZIP={v}'])
        elif k == 'shuffle':
            cmd_list.extend(['-f', f'{var_name}:SHUF'])
        else:
            raise ValueError(f'Unknown h5repack option {k} ({h5_keywords})')

    chunking = 'x'.join(map(str, new_chunk_shape))
    cmd_list.extend(['-l', f'{var_name}:CHUNK={chunking}'])
    logger.info(f'Command: {cmd_list}')

    exe = subprocess.run(cmd_list, capture_output=True, text=True)
    if exe.returncode != 0 or exe.stderr:
        logger.info(exe.stderr)
        raise RuntimeError(f'Unable to repack {infile}')
    logger.info(exe.stdout)


def rechunk(infile, field_number, outfile, new_chunk_shape, logger=None, **kw):
    """Read a file, rechunk a specific field, and write it out."""
    logger = logger or get_logger()
    t1 = datetime.datetime.now().timestamp()
    g = cf.read(infile)[field_number]

    ncvar = g.nc_get_variable()
    old_chunk_shape = get_chunking_hack(infile, ncvar)

    if len(old_chunk_shape) == 3:
        logger.info(f'Assuming dimension order T, Y, X for chunkshape {old_chunk_shape}')
        dask_chunks = {k: v for k, v in zip(['T', 'Y', 'X'], old_chunk_shape)}
    elif len(old_chunk_shape) == 4:
        logger.info(f'Assuming dimension order T, Z, Y, X for chunkshape {old_chunk_shape}')
        dask_chunks = {k: v for k, v in zip(['T', 'Z', 'Y', 'X'], old_chunk_shape)}
    else:
        logger.info(f'Unexpected chunk shape {old_chunk_shape}, using auto for read')
        dask_chunks = 'auto'

    f = cf.read(infile, chunks=dask_chunks)[field_number]
    t2 = datetime.datetime.now().timestamp()
    logger.info(f'Lazy read of {infile} in {t2-t1:.2f}s')

    f.data.nc_set_hdf5_chunksizes(new_chunk_shape)
    cf.write(f, outfile, **kw)
    t3 = datetime.datetime.now().timestamp()
    logger.info(f'Wrote {outfile} with chunk_shape {new_chunk_shape} in {t3-t2:.2f}s')


def make_filename(identity, attributes, frequency, starting, length):
    """Create a suitable filename for the field."""
    header = "_".join([attributes[k] for k in ['source_id', 'experiment', 'variant_label']])
    sdate = starting.datetime_array[0].isoformat()
    if 'hr' in frequency:
        sdate = sdate[0:-6] + sdate[-5:-3]
    else:
        sdate = sdate[0:10]
    return f"{identity}_{header}_{frequency}_{sdate}_N{length}.nc"


def get_frequency_attribute(f):
    """Extract frequency attribute from time coordinate."""
    tc = f.coordinate('T')
    tu = tc.units
    try:
        cm = f.cell_method('T').method
    except ValueError:
        raise NotImplementedError('Multiple cell methods on time?')

    if len(tc.data) > 1:
        ti = tc.data[1] - tc.data[0]
        if tu.startswith('days'):
            ti = round(float(ti * 24))
        else:
            raise NotImplementedError('Unexpected time units')
        if ti < 24:
            ti = f'{ti}hr'
            if cm == 'point':
                ti += 'Pt'
        elif ti == 24:
            ti = 'day'
            if cm != 'mean':
                raise NotImplementedError(f'Need to handle daily data with {cm} method')
        elif ti == 720:
            ti = 'mon'
            if cm != 'mean':
                raise NotImplementedError(f'Need to handle monthly data with {cm} method')
        elif ti == 360:
            ti = 'yr'
            if cm == 'point':
                ti += 'Pt'
        else:
            raise NotImplementedError(f'Unexpected time interval: {ti}hours')
    else:
        cellsize = tc[0].cellsize
        ti = float(cellsize.data)
    return ti


def pp2nc_from_config(cc, config_file, task_number,
                      target=None, bucket=None,
                      dummy_run=False,
                      logger=None):
    """Convert PP files to NetCDF using configuration JSON."""
    logger = logger or get_logger()
    with open(config_file, 'r') as f:
        configuration = json.load(f)

    mytask = configuration['tasks'][task_number]
    simulation = mytask[0]
    myfiles = mytask[1]

    global_attributes = configuration['experiment_detail']
    global_attributes['runid'] = simulation
    global_attributes['tracking_id'] = str(uuid4())
    global_attributes['variant_label'] = configuration['simulations'][simulation]

    urldetails = [configuration['experiment_detail'][x] for x in
                  ['project', 'experiment', 'further_info_url_base']]
    global_attributes['further_info_url'] = f'{urldetails[2]}/{urldetails[0]}/{urldetails[1]}'
    today = datetime.date.today().strftime('%Y-%m-%d')
    global_attributes['processing'] = f'pp_to_nice_netcdf:{today}.'
    del global_attributes['further_info_url_base']

    logger.info(f'Reading {myfiles}')
    fields = cf.read(myfiles)

    for f in fields:
        f = fix_axes(f, logger=logger)
        fkey = get_frequency_attribute(f)
        tc = f.coordinate('T').data

        # identify concept
        common_concept_name = cc.identify(f)
        if not common_concept_name.startswith('UM'):
            f.set_property('common_name', f'cmip6:{common_concept_name}')
            chunk_shape = get_optimal_chunkshape(f, configuration['storage_options']['chunksize'])
            f.data.nc_set_hdf5_chunksizes(chunk_shape)

        user_metadata = configuration['user_metadata']
        user_metadata['chunk_shape'] = f'{str(chunk_shape)}/{str(f.shape)}'
        user_metadata['domain'] = f.domain.__repr__()[13:-2]
        for k, v in global_attributes.items():
            user_metadata[k] = v

        filename = make_filename(common_concept_name, global_attributes, fkey, tc[0], len(tc))
        logger.info(f'Writing {filename}')

        if dummy_run:
            print(user_metadata)
            continue

        compress = configuration['storage_options']['compress']
        shuffle = configuration['storage_options']['shuffle']

        current_chunking = f.data.nc_hdf5_chunksizes()
        logger.info(f'Writing array [{f.data.shape}] with chunk shape {current_chunking}.')

        tmpfile = ""
        if current_chunking[0] != 1:
            tmpfile = filename[0:-3] + '-tmp.nc'
            pp2chunkednc(f, tmpfile, filename, current_chunking,
                         compress=compress, shuffle=shuffle, logger=logger)
        else:
            cf.write(f, filename, compress=compress, shuffle=shuffle, file_descriptors=global_attributes)

        if tmpfile:
            os.remove(tmpfile)

        if bucket is not None and target is not None:
            try:
                move_to_s3(filename, target, bucket, user_metadata=user_metadata)
            except Exception as e:
                print(f'Failed transfer to S3: {e}')
                with open('user_metadata_at_crash.json', 'w') as ff:
                    json.dump(user_metadata, ff)
                raise