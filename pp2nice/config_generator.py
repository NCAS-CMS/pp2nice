from pathlib import Path
import json
import yaml
from pp2nice.logger_utils import get_logger

MANDATORY = ['project', 'experiment', 'source_id', 'nominal_resolution', 'model_configuration', 'author']

def flist(origin, simulation, subdir):
    """Return list of .pp files in a given directory."""
    path = Path(origin) / simulation / subdir
    return list(path.glob('*.pp'))


def read_config(config_file='eg_config.yaml', logger=None):
    """Read a YAML configuration file and return JSON-ready structure."""
    logger = logger or get_logger()
    
    logger.info(f"Reading configuration from {config_file}")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    complete_configuration = {
        'storage_options': config.get("storage_options", {}),
        'experiment_detail': config.get("experiment_detail", {}),
        'output_location': config.get("output_location", ""),
        'simulations': config.get("simulations", {}),
        'user_metadata': config.get("user_metadata", {}),
        'subdirs': config.get('subdirs', []),
        'tasks': []
    }

    # check mandatory keys exist
    experiment_detail = complete_configuration['experiment_detail']
    missing = [x for x in MANDATORY if x not in experiment_detail]
    if 'author' in missing and 'owner' in experiment_detail:
        missing.remove('author')
    if missing:
        raise ValueError(f'Mandatory experiment detail missing: {missing}')

    config_name = config.get('config_name', 'processed_config.json')
    return config_name, complete_configuration


def generate_json(config_name, complete_configuration, origin, logger=None):
    """Generate tasks by scanning directories and write JSON configuration."""
    logger = logger or get_logger()
    simulations = complete_configuration['simulations']
    subdirs = complete_configuration['subdirs']

    for simulation in simulations.keys():
        for subdir, nfiles_per_group in subdirs:
            files = flist(origin, simulation, subdir)
            nfiles = len(files)
            for i in range(int(nfiles / nfiles_per_group)):
                s, f = nfiles_per_group * i, nfiles_per_group * (i + 1)
                file_group = files[s:f]
                complete_configuration['tasks'].append(
                    (simulation, [str(f) for f in file_group])
                )

    with open(config_name, 'w') as f:
        json.dump(complete_configuration, f, indent=2)

    logger.info(f'Completed "{config_name}" with {len(complete_configuration["tasks"])} tasks')