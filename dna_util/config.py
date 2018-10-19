import os
import logging
import logging.config
from typing import Optional, List, Dict, Any
import zipfile
import yaml

from dna_util import io

logger = logging.getLogger(__name__)


def parse_config(path: Optional[str] = None, 
                 required: Optional[List[str]] = None) -> Dict[str, Any]:
    """ Parse YAML configuration file

    Parameters
    -----------
    path : str (default None)
        File path to config file, looks for ~/dna_util_config[.yml,.yaml] if
        None is specified

    required : List (default None)
        List of any required configuration variables. A ValueError will be
        raised if any required keys are missing

    Returns
    --------
    Dict
    """
    if required is None:
        required = []

    # Attempt to default to a config file
    if path is None:
        possible_paths = ["~/.dna_util_config.yaml", "~/.dna_util_config.yml"]
        possible_paths = list(filter(io.already_exists, possible_paths))

        if possible_paths:
            path = possible_paths[0]
            logger.info(f"Possible config paths include: {possible_paths!r}. Using {path!r}")
        else:
            raise ValueError("No path was specified and a default couldn't be inferred")
    else:
        path = os.path.expanduser(os.path.normpath(path))

        # Check to ensure that the path exists and .zip isn't in the path
    if not io.already_exists(path):
        raise ValueError(f"{path!r} does not exist")

    # TODO deal with zip files here as well

    logger.info(f"Loading in configuration file located at {path!r}")
    
    raw_obj = io.load_object(path, file_type="raw")
    cfg = yaml.safe_load(raw_obj)

    missing_vars = list(set(required).difference(cfg.keys()))
    if missing_vars:
        raise ValueError(f"Error, no configuration variable(s) found for: {missing_vars!r}")
    return cfg


def setup_logging(path: str = None, root_log_level: int = None):
    """ Setup logging configuration

    Parameters
    -----------
    path : str (default None)
        The path to the YAML configuration file. Will use logging.yaml by
        default

    root_log_level : int (default None)
        The level at which the root logger should be configured at. Leaving as
        None will use the set level in the specified logging configuration file
        NOTE: Future calls to setup_logging may interfere with this setting.
        Temporarily changing logging.yaml is a more robust way to set this

    Example
    --------
    >>> from dna_util.config import setup_logging
    >>> setup_logging()
    >>> logger = logging.getLogger(__name__)
    """
    if not path:
        path = os.path.join(
            os.path.dirname(__file__), "logging.yaml"
        )

    logging.disable(logging.INFO)
    cfg_dict = parse_config(path)
    logging.disable(logging.NOTSET)

    # Set root log level if one was specified
    if root_log_level:
        cfg_dict["root"]["level"] = root_log_level

    logging.config.dictConfig(cfg_dict)