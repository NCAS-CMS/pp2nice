import argparse
from pp2nice.config_generator import read_config, generate_json
from pp2nice.logger_utils import get_logger

def main():
    logger = get_logger()

    parser = argparse.ArgumentParser(
        description="Generate JSON configuration for pp2nice from YAML template."
    )
    parser.add_argument(
        "config_file",
        type=str,
        help="Path to the YAML configuration file (e.g., eg_config.yaml)"
    )
    parser.add_argument(
        "--origin",
        type=str,
        required=True,
        help="Root path where simulation directories are located"
    )

    args = parser.parse_args()

    config_name, complete_configuration = read_config(args.config_file, logger=logger)
    generate_json(config_name, complete_configuration, origin=args.origin, logger=logger)


if __name__ == "__main__":
    main()