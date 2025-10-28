Project Layout
==============

The structure of the `pp2nice` project is as follows:

.. code-block:: text

   pp2nice/                  <- project root
   ├─ pyproject.toml         <- project metadata and build configuration
   ├─ readme.md              <- project readme
   ├─ scripts/               <- top-level scripts installed to env/bin
   │  └─ slurm_pp2nc.sh      <- Slurm submission script
   ├─ tests/                 <- unit and integration tests
   │  └─ ...                 <- test files
   ├─ pp2nice/pp2nice/       <- Python package
   │  ├─ __init__.py
   │  ├─ cli.py                      <- main CLI entry point
   │  ├─ cli_generate_config.py      <- CLI for generating configuration JSON
   │  ├─ convert.py                  <- conversion utilities
   │  ├─ config_generator.py         <- configuration generator library
   │  ├─ common_concept.py           <- common concepts utilities
   │  ├─ get_chunkshape.py           <- chunk shape utilities
   │  ├─ upload.py                   <- S3 upload helpers
   │  ├─ slurm.py                    <- Slurm-related helpers
   │  ├─ logger_utils.py             <- logging helpers
   │  ├─ load_asset.py               <- asset loading utilities
   │  ├─ assets/                     <- packaged YAML and JSON assets
   │  │  ├─ eg_config.yaml           <- example configuration file
   │  │  └─ vocabs/
   │  │      ├─ cmip6_common_concept.json. <- This is the CMIP6 vocabulary
   │  │      └─ ncas_common_concept.yaml.  <- With extensions based on NCAS requirements

Installed Executables
--------------------

After running ``pip install .``, the following commands will be available in the Python environment's ``bin/`` directory:

.. code-block:: text

   # Python CLI executables
   pp2nice                 <- main CLI command
   pp2niceconfig           <- generate configuration JSON to be used by pp2nice executable

   # Bash/Slurm script
   slurm_pp2nc.sh          <- Slurm submission script, retains SBATCH directives

