import os
import sys
import logging
import binascii
from typing import List, Dict, Any, Optional
import argparse

from dna_util.io import already_exists

logger = logging.getLogger(__name__)


def generate_token(n: int = 16) -> str:
    """ Generates random token

    Parameters
    -----------
    n : int (default 16)
        Number of bits to use when generating the token

    Returns
    --------
    str
    """
    return binascii.hexlify(os.urandom(n)).decode()


# ~Stolen~ Borrowed from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix='B'):
    """ Function for getting human readable formatting of size in bytes
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def parse_args(obj: object, obj_funs: Optional[List[str]] = None,
               **kwargs) -> Dict[Any, Any]:
    """ Extract variables from kwargs that are applicable to obj and/or obj_funs

    For Example:
    parse_args(pd, ["DataFrame", "to_csv"], **kwargs) would return arguments
    relevant to the pandas DataFrame.to_csv() function

    Parameters
    -----------
    obj : object
        Some object to inspect for relevant arguments

    obj_funs : List[str]
        The list of nested class attributes from obj

    **kwargs

    Returns
    --------
    Subset of **kwargs that are relevant to the object in question
    """
    if obj_funs is not None:
        for fun in obj_funs:
            obj = getattr(obj, fun)

    args = {key: value for key, value in kwargs.items() if key in obj.__code__.co_varnames}

    return args


def argparse_path_exists(path: str):
    """ Validates a file path exists when parsing input using ArgParse

    Parameters
    -----------
    path : str
        The file path to validate

    Returns
    --------
    path if path exists, raises argparse.ArgumentTypeError otherwise
    """
    path = str(path)
    if not already_exists(path):
        msg = f"{path!r} does not exist"
        raise argparse.ArgumentTypeError(msg)
    return path
