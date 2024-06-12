from common_concept import CommonConcepts
from pp_to_nice_netcdf import pp2nc_from_config

#
# You need to edit this file to use YOUR config file 
# and your S3 alias and your bucket name.
#

CONFIG_FILE = 'n1280_processing_v1.json'
S3_TARGET = 'hpos'
S3_BUCKET = 'bnl'

#
# Change nothing below here
#

cc = CommonConcepts()
task_number = int(os.environ['SLURM_ARRAY_TASK_ID'])
config_file = CONFIG_FILE
print(f"Using task {task_number} from {config_file}")
pp2nc_from_config(cc, config_file, task_number, 
                target = S3_TARGET, bucket=S3_BUCKET,
                logging=True, dummy_run=False)