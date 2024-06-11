import cf
from time import time
from uuid import uuid4
import json
import os
import numpy as np
from upload import move_to_s3

from common_concept import CommonConcepts
from get_chunkshape import get_chunkshape

def make_filename(identity, attributes,frequency,starting,length):
    """ 
    Create a suitable filename for the field.
    Note assumption that all fields have the same time duration
    """

    header = "_".join([attributes[k] for k in ['source_id','variant_label','runid']])
  
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
            if method != 'mean':
                raise NotImplementedError(f'Need to handle daily data with {cm} method')
        elif ti == 720:
            ti = 'mon'
            if method != 'mean':
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




def pp2nc_from_config(cc, config_file, task_number, logging=False, dummy_run=False):
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
            chunk_shape = get_chunkshape(np.array(f.data.shape), configuration['storage_options']['chunksize'])
            # yes, the method has the wrong name
            f.data.nc_set_hdf5_chunksizes(chunk_shape)
        print(global_attributes)
        ss = make_filename(common_concept_name, global_attributes, fkey, tc[0], len(tc))
        print('\nWriting: ', ss)
        if dummy_run:
            print(global_attributes)
        else:
            compress = configuration['storage_options']['compress']
            shuffle = configuration['storage_options']['shuffle']
            e3a = time()
            current_chunking = f.data.nc_hdf5_chunksizes()
            print(f'Writing array [{f.data.shape}] with chunk shape {current_chunking}.' )
            ss1 = ""
            if current_chunking[0]!=1:
                # We have to deal with an horrific issue with reading pp data. Effectively we have
                # read the entire data many times and slice in memory. It's much faster to simply
                # write the data out and rechunk from the netcdf version - though it's still
                # slow!
                print('Need to use temp file')
                new_chunk = list(f.data.shape)
                new_chunk[0] = 1
                ss1 = ss[0:-3]+'-tmp.nc'
                e3a1 = time()
                print(new_chunk)
                f.data.nc_set_hdf5_chunksizes(new_chunk)
                cf.write(f,ss1,compress=compress, shuffle=shuffle,
                    file_descriptors=global_attributes)
                e3a2 = time()
                print(f'first temp file written {e3a2-e3a1:.1f}')
                f = cf.read(ss1)[0]
                f.data.nc_set_hdf5_chunksizes=current_chunking
                e3a3 = time()
                print(f'lazy reading temp file took {e3a3-e3a2:.1f}s')
            cf.write(f, ss,
                    compress=compress, shuffle=shuffle,
                    file_descriptors=global_attributes
                    )
            e3b = time()
            print(f"... written {e3b-e3a:.1f}")
            if ss1 != '': 
                os.remove(ss1)
    e3 = time()
    if logging:
        print(f'\nWriting {len(fields)} files took {e3-e2:.1f}s\n')
    return ss

def convert_pp_to_s3nc(cc, config_file, task_number, target, bucket, 
                       logging=True, dummy_run=False):
    """
    Convert a pp file on POSIX disk to netcdf and upload to S3
    """
    filename = pp2nc_from_config(cc, config_file, task_number, logging=True, dummy_run=False)
    move_to_s3(filename, target, bucket)


if __name__ == "__main__":
    cc = CommonConcepts()
    task_number = 2
    config_file = 'n1280_processing_v1.json'
    #filename = pp2nc_from_config(cc, config_file, task_number, logging=True, dummy_run=False)
    convert_pp_to_s3nc(cc, config_file, task_number, 'hpos', 'bnl')