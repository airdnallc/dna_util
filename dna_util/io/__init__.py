"""
io module deals with abstracting IO operations between local and s3 file systems
"""
from ._io import cp, ls, rm, already_exists, load_object, save_object, get_size
from ._s3 import is_s3path

__all__ = ["cp", "ls", "rm", "already_exists", "load_object", "save_object", "is_s3path", "get_size"]

# # Load mlflow submodule if mlflow is installed
try:
    from . import mlflow
    __all__.append("mlflow")
except ModuleNotFoundError:
    pass
