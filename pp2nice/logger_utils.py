from pp2nice.slurm import SlurmLogger

# This will hold the shared instance
_logger = None

def get_logger():
    """Return a shared SlurmLogger instance."""
    global _logger
    if _logger is None:
        _logger = SlurmLogger()
    return _logger