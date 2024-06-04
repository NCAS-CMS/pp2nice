import cf
from time import time
from uuid import uuid4
import json
import math

from common_concept import CommonConcepts

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

def chunk_volume_to_shape(data, volume, word_size=4):
    """
    Given a data object which has a .shape method, calculate a suitable chunk shape
    for a given volume (in bytes). (We use word instead of dtype in case the user
    changes the data type within the writing operation.)
    """
    v = volume/word_size 
    shape = data.shape
    size = data.size
    
    n_chunks = int(size/v)
    dlen = np.sum(shape)
    factors = [list(factors(n)) for n in shape]
    first_guess=[math.ceil(v*x/dlen) for x in shape]

    # correct to be factors
    result = [x/math.ceil(x/y) for x,y in zip(shape,first_guess)]
    return result

def factors(n):  
    j= 2
    while n > 1:
        for i in range(j, int(sqrt(n+0.05)) + 1):
            if n % i == 0:
                n //= i ; j = i
                yield i
                break
        else:
            if n > 1:
                yield n; break


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
        print(f'\nReading completed in {e2-e1:.1}s\n')

    for f in fields:
        fkey = get_frequency_attribute(f)
        tc = f.coordinate('T').data
        common_concept_name = cc.identify(f)
        if common_concept_name.startswith('UM'):
            pass
        else:
            f.set_property('common_name',f'cmip6:{common_concept_name}')
            f.data.nc_set_hdf5_chunksizes(configuration['storage_options']['chunksize'])
        print(global_attributes)
        ss = make_filename(common_concept_name, global_attributes, fkey, tc[0], len(tc))
        print('\nWriting: ', ss)
        if dummy_run:
            print(global_attributes)
        else:
            compress = configuration['storage_options']['compress']
            shuffle = configuration['storage_options']['shuffle']
            e3a = time()
            cf.write(f, ss, 
                    compress=compress, shuffle=shuffle, 
                    file_descriptors=global_attributes
                    )
            e3b = time()
            print(f"... written {e3b-e3a:.1}")
    e3 = time()
    if logging:
        print(f'\nWriting {len(fields)} files took {e3-e2:.1}s\n')


if __name__ == "__main__":
    cc = CommonConcepts()
    task_number = 2
    config_file = 'n1280_processing_v1.json'
    pp2nc_from_config(cc, config_file, task_number, logging=True, dummy_run=False)
