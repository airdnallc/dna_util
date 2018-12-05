import os
import logging
from typing import Any

import mlflow as mlflow_

from dna_util import io

logger = logging.getLogger(__name__)


def save_artifact(obj, subpath: str, **kwargs) -> None:
    """ Saves obj to the specified subpath

    Parameters
    -----------
    obj : object
        Python object in memory

    subpath : str
        The location to save the object to. The subpath will be placed within
        the currently active mlflow run_uuid within the specified artifact
        store (this can be specified when creating the mlflow run)

    kwargs : Dict
        All other arguments are passed to io.save_object

    Returns
    --------
    None
    """
    path = os.path.join(mlflow_.get_artifact_uri(), subpath)
    logger.info(f"Saving artifact to {path}")
    io.save_object(obj, path, **kwargs)


def load_artifact(run_uuid: str, subpath: str, tracking_uri: str = None,
                  **kwargs) -> Any:
    """ Loads an artifact from a specific run saved at subpath

    Parameters
    -----------
    run_uuid : str
        The run uuid to query the tracking server for

    subpath : str
        The subpath to load in. The run_uuid's artifact uri is joined with the
        subpath to determine where the file is located.

    tracking_uri : str
        This specifies how to connect to the tracking server
        e.g. "http://ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com"

    kwargs : Dict
        All other arguments are passed to io.load_object
    """
    client = mlflow_.tracking.MlflowClient(tracking_uri)
    run = client.get_run(run_uuid)
    path = os.path.join(run.info.artifact_uri, subpath)
    logger.info(f"Loading artifact from {path}")
    return io.load_object(path, **kwargs)
