#!/bin/bash
#SBATCH --partition=short-serial 
#SBATCH -o ppjob-%A_%a.out 
#SBATCH -e ppjob-%A_%a%.err
#SBATCH --time=10:00:00  #(HH:MM:SS)

#
# The total number of tasks needed for the job
# array can be foundin the configuration file
# 'ntasks'
# This should then be run (for 990 tasks) 
# using 20 simulataneous jobs  with 
# sbatch --array 0-989%20 -N1 this_file_name
#



echo "SLURM has $SLURM_ARRAY_TASK_ID"
cd ~
cd hiresgw/hrcm
bash speedo.sh
conda activate mampy24b
#unbuffered output!
python -u pp_to_nice_netcdf.py
echo "Finished"
