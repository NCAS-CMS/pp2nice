import cf
from time import time
import datetime
from uuid import uuid4
import json
import os
import numpy as np
from upload import move_to_s3

from common_concept import CommonConcepts
from get_chunkshape import get_optimal_chunkshape


def pp2chunkednc(f, tmpfile, outfile, new_chunk_shape, logging=True, **kw):
    """     
    In this case, <f> is a field which has already been assigned a new chunk shape,
    but it has not yet been read to memory. 

    We have to deal with an horrific issue with reading pp data into chunked netcdf. 
    Effectively we have to read the entire data many times and slice in memory. 
    It's much faster to simply write the data out with simpler chunks and rechunk 
    from the netcdf version  - though it's still slow!

    """
    new_chunk = list(f.data.shape)
    new_chunk[0] = 1
    if logging:
        print(f'<pp2chunkednc> Using a temp file with temp chunking {new_chunk}')
    t1 = time()
    f.data.nc_set_hdf5_chunksizes(new_chunk)
    # we try not compressing the temporary data in the hope it will speed things up
    cf.write(f, tmpfile, compress=0, shuffle=False)
    t2 = time()
    if logging:
        print(f'<pp2chunkednc> Temp file ({tmpfile}) written in {t2-t1:.2f}s')
    rechunk(tmpfile, 0, outfile, new_chunk_shape, logging=logging, **kw)



def rechunk(infile, field_number, outfile, new_chunk_shape, logging=True, **kw):
    """ 
    Read a file, rechunk a specific field, and write it out with
    the appropriate keywords
    """

    t1 = time()
    f = cf.read(infile)[field_number]
    t2 = time()
    old_chunk_shape = f.data.nc_hdf5_chunksizes()
    if logging:
        print(f'<rechunk> Lazy read of {infile} (chunk shape = {old_chunk_shape}) in {t2-t1:.2f}s')

    f.data.nc_set_hdf5_chunksizes=new_chunk_shape
    cf.write(f, outfile, **kw)
    t3 = time()
    if logging:
        print(f'<rechunk> Wrote {outfile} with chunk_shape {new_chunk_shape} in {t3-t2:.2f}s')



def make_filename(identity, attributes,frequency,starting,length):
    """ 
    Create a suitable filename for the field.
    Note assumption that all fields have the same time duration
    """

    header = "_".join([attributes[k] for k in ['source_id','experiment','variant_label']])
  
    sdate = starting.datetime_array[0].isoformat()
    if 'hr' in frequency:
        sdate=sdate[0:-6]+sdate[-5:-3]
    else:
        sdate = sdate[0:10]
    fname = f"{identity}_{header}_{frequency}_{sdate}_N{length}.nc"
    return fname

def get_frequency_attribute(f):
    """ 
    Extract frequency attribute from time interval in data and cell methods 
    Ugly as sin, this!
    """
    
    tc = f.coordinate('T')
    tu = tc.units
    try:
        cm = f.cell_method('T').method
    except ValueError:
        raise NotImplementedError('Multiple cell methods on time?')

    if len(tc.data) > 1:
        ti = tc.data[1]-tc.data[0]
        if tu.startswith('days'):
            ti = round(float(ti*24))
        else:
            raise NotImplementedError('Unexpected time units')
        if ti < 24:
            ti = f'{ti}hr'
            if cm == 'point':
                ti+='Pt'
        elif ti == 24:
            ti = 'day'
            if cm != 'mean':
                raise NotImplementedError(f'Need to handle daily data with {cm} method')
        elif ti == 720:
            ti = 'mon'
            if cm != 'mean':
                raise NotImplementedError(f'Need to handle monthly data with {cm} method')
        else:
            raise NotImplementedError(f'Unexpected time interval: {ti}hours')
    else:
        cellsize = tc[0].cellsize
        ti = float(cellsize.data)
        if ti == 360.0:
            ti = 'yr'
            if cm == 'point':
                ti+='Pt'
        else:
            raise NotImplementedError('Time period ',cellsize)
    
    return ti


def pp2nc_from_config(cc, config_file, task_number, 
                        target=None, bucket=None, 
                        logging=False, dummy_run=False):
    """ 
    Convert pp files to netcdf using a specifc task_number 
    from an instance of the json configuration 
    created following the eg_1280 template 
    and found in config_file.
    """
    with open(config_file,'r') as f:
        configuration = json.load(f)

    mytask = configuration['tasks'][task_number]
    simulation = mytask[0]
    myfiles = mytask[1]

    global_attributes = configuration['experiment_detail']
    global_attributes['runid'] = simulation
    global_attributes['tracking_id'] =  str(uuid4())
    global_attributes['variant_label'] = configuration['simulations'][simulation]
    
    urldetails = [configuration['experiment_detail'][x] for x in 
                    ['project','experiment','further_info_url_base']]
    global_attributes['further_info_url'] = f'{urldetails[2]}/{urldetails[0]}/{urldetails[1]}'
    today = datetime.date.today().strftime('%Y-%m-%d')
    global_attributes['processing'] = f'pp_to_nice_netcdf:{today}.'
    del global_attributes['further_info_url_base']

    if logging:
        print('Reading')
        print(myfiles)
        print('---')
    e1 = time()
    fields = cf.read(myfiles)
    e2 = time()
    if logging:
        print(f'\nReading completed in {e2-e1:.1f}s\n')

    for f in fields:
        fkey = get_frequency_attribute(f)
        tc = f.coordinate('T').data
        common_concept_name = cc.identify(f)
        if common_concept_name.startswith('UM'):
            pass
        else:
            f.set_property('common_name',f'cmip6:{common_concept_name}')
            chunk_shape = get_optimal_chunkshape(f, configuration['storage_options']['chunksize'])
            #chunk_shape = get_optimal_chunkshape(np.array(f.data.shape), configuration['storage_options']['chunksize'])
            # yes, the method has the wrong name
            f.data.nc_set_hdf5_chunksizes(chunk_shape)
        user_metadata = configuration['user_metadata']
        for k in ['standard_name','long_name']:
            try: 
                user_metadata[k] = getattr(f,k)
            except AttributeError:
                pass
        user_metadata['chunk_shape'] = f'{str(chunk_shape)}/{str(f.shape)}'
        user_metadata['domain'] = f.domain.__repr__()[13:-2]
        for k,v in global_attributes.items():
            user_metadata[k]=v
        ss = make_filename(common_concept_name, global_attributes, fkey, tc[0], len(tc))
        print('\nWriting: ', ss)
        if dummy_run:
            print(user_metadata)
        else:
            compress = configuration['storage_options']['compress']
            shuffle = configuration['storage_options']['shuffle']
            e3a = time()
            current_chunking = f.data.nc_hdf5_chunksizes()
            print(f'Writing array [{f.data.shape}] with chunk shape {current_chunking}.' )
            ss1 = ""
            if current_chunking[0]!=1:
                ss1 = ss[0:-3]+'-tmp.nc'
                pp2chunkednc(f, ss1, ss, current_chunking, compress=compress, shuffle=shuffle, file_descriptors=global_attributes)
            else:
                cf.write(f, ss,
                    compress=compress, shuffle=shuffle,
                    file_descriptors=global_attributes
                    )
            e3b = time()
            print(f"... file {ss} written {e3b-e3a:.1f}")
            if ss1 != '': 
                os.remove(ss1)
            if bucket is not None and target is not None: 
                try:
                    move_to_s3(ss, target, bucket, user_metadata=user_metadata)
                except Exception as e:
                    print(f'Failed transfer to S3: {e}')
                    with open('user_metadata_at_crash.json','w') as ff:
                        json.dump(user_metadata,ff)
                    raise
                e3c = time()
                print(f'...file moved to s3 in {e3c-e3b:.1f}s')
    e3 = time()
    if logging:
        print(f'\nWriting {len(fields)} files took {e3-e2:.1f}s\n')


if __name__ == "__main__":
    cc = CommonConcepts()
    task_number = int(os.environ['SLURM_ARRAY_TASK_ID'])
    config_file = 'n1280_processing_v1.json'
    print(f"Using task {task_number} from {config_file}")
    pp2nc_from_config(cc, config_file, task_number, 
                    target ='hrs3', bucket='hrcm',
                    logging=True, dummy_run=False)
    
