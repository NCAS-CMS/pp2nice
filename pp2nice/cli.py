import os
import platform
import argparse
from pp2nice.common_concept import CommonConcepts
from pp2nice.convert import pp2nc_from_config
from pp2nice.logger_utils import get_logger


def main():
    logger = get_logger()

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Convert PP files to chunked NetCDF using a configuration file."
    )
    parser.add_argument(
        "config_file",
        type=str,
        help="Path to the JSON configuration file (e.g., n1280_processing_v1.json)"
    )
    parser.add_argument(
        "-t", "--task_number",
        type=int,
        default=None,
        help="Task number to process (falls back to SLURM_ARRAY_TASK_ID if not provided)"
    )
    parser.add_argument(
        "--dummy_run",
        action="store_true",
        help="If set, do not write files, just print metadata"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="Optional S3 bucket name for uploading files"
    )
    parser.add_argument(
        "--target",
        type=str,
        default=None,
        help="Minio configuration identifier for endpoint S3 server"
    )

    args = parser.parse_args()

    # Determine task number
    if args.task_number is not None:
        task_number = args.task_number
    else:
        task_number = int(os.environ.get("SLURM_ARRAY_TASK_ID", 0))

    config_file = args.config_file

    logger.info(f"Running task {task_number} on {platform.node()} ({platform.machine()})")
    logger.info(f"Using configuration file: {config_file}")

    cc = CommonConcepts()

    # Call the library function
    pp2nc_from_config(
        cc,
        config_file,
        task_number,
        target=args.target,
        bucket=args.bucket,
        dummy_run=args.dummy_run,
        logger=logger,
    )


if __name__ == "__main__":
    main()