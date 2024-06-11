## This is the file that was used to generate HRCM N1280 NetCDF files
## by Bryan Lawrence in June 2024. 
#
# It generates a json file that can be used by the accompanying
# processing infrastructure as documented in the README.
# 
# For a new application you should copy this file, edit the output json name,
# the experiment detail, and the locaiton of your existing data, 
# and where you want the output data.  You should then run it to generate
# your processing configuraiton file.
# 

### Output Configuation File Name
# This is the name of the configuration file you are going to write for your 
# processing
config_name = "n1280_processing_v1.json"


### NetCDF Storage Configuration
# Only change this if you really know what you are doing.
# Chunksize is size _before_ compression, and applied to all variables.
# Your actual chunk size will depend on the shape of the variable and the compressed volume

storage_options = {'compress':4, 'shuffle':True, 'chunksize':1e6}

### Experiment Configuration
# Add your own metadata, but note there is a small mandatory list shown below

experiment_detail = {

    'institution':          'NCAS',
    'source_id':            'N1280-GA7EA',
    'source_type':          'AGCM',
    'um_resolution':        'N1280',
    'configuration':        'GA7.2.1-Easy Aerosol L85 / GL8 ',
    'version':              'UM11.6',
    'nominal_resolution':   '10 km',
    'owner':                'Professor P.-L. Vidale',
    'project':              'HRCM',
    'experiment':           'HighresSST-present',
    'further_info_url_base':'https://github.com/ncas-cms/further_info',
    'MIP':                  'HighResMIP',
    'MIP_ERA':              'CMIP6',
    'PARENTAGE':            'Follows one year of u-cd936 then 1 year of u-cf432',
    'ENSEMBLE_TYPE':        'Perturbed stochastic physics',
    'realm':                'atmos',
    'grid_label':           'gn',
    'NOTES':    [
                    "Not formally part of HighResMIP but conforming to the protocol"
                ]
}
# check mandatory keys exist
for x in ['project','experiment','further_info_url_base','source_id','nominal_resolution']:
    assert x in experiment_detail

### Existing data location and app ggregation instructions
## This is where your input pp data is, and how you want the pp variables to be aggregated in time

origin = '/gws/nopw/j04/hrcm/n1280run'

#
# names and number of files to aggregate directly
# what will hapepns is that, for each variable, the values will be aggregated across N files.
# eg. if you have an output stream with 6 variables output hourly with 48 hours per file, and you
# enter ('1hrly',15) you will end up with 6 files each with 30 days per file.
# this obviously works nicely for 360 day calendars, will need to be smarter soon.
#
subdirs = [('1hrly',15),('6hrly',3),('apy',35)]
simulations = {
        'u-ch330':'r1i1p1f1',
        'u-ck777':'r2i1p1f1', 
        'u-ck778':'r3i1p1f1',
}

### Output Location
# Leave blank if you want it to be in the working directory from where this is run!

output_location = ''

##### Executable Code Follows. 
## This generates the slurm scripts you need to do the data conversion

from pathlib import Path
import json

def flist(origin, simulation, subdir):
    ''' Utility function for getting all pp files in a particular directory'''
    path = f'{origin}/{simulation}/{subdir}/'
    return list(Path(path).glob('*.pp'))

if __name__ == "__main__":

    complete_configuration = {
        'storage_options': storage_options,
        'experiment_detail': experiment_detail,
        'output_location': output_location,
        'simulations': simulations,
        'tasks':[]
    }

    for simulation in simulations.keys():
        for subdir in subdirs:
            files = flist(origin,simulation, subdir[0])
            nfiles = len(files)
            for i in range(int(nfiles/subdir[1])):
                s, f = subdir[1]*i, subdir[1]*(i+1)
                file_group = files[s:f]
                complete_configuration['tasks'].append((simulation,[str(f) for f in file_group]))
        
    with open(config_name,'w') as f:
        json.dump(complete_configuration,f)


