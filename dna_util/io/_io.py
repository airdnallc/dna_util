import logging
import json
from typing import List, Any, Optional
import pickle

import pandas as pd

from dna_util.io import _s3 as s3
from dna_util.io import _local as local

logger = logging.getLogger(__name__)


def cp(from_path: str, to_path: str, overwrite: bool = True,
       include_folder_name: bool = True, **kwargs) -> None:
    """ Copy a file or directory of files from local/s3 to local/s3

    Parameters
    -----------
    from_path : str
        Directory/file path to copy

    to_path : str
        Path to copy file(s) to.

    overwrite : bool (default True)
        Should the to_path be overwritten if it already exists?

    include_folder_name : bool (default True)
        If copying a directory, add the directory name automatically to the
        to_path.  i.e. if True, the entire folder will be copied to the
        to_path. If False, the *contents* of the directory will be copied to
        the to_path

    kwargs : Dict
        Extra arguments to pass to the appropriate cp (either _local.cp or
        _s3.cp)

    Returns
    --------
    None
    """
    if s3.is_s3path(from_path) or s3.is_s3path(to_path):
        s3.cp(from_path, to_path, overwrite, include_folder_name, **kwargs)
    else:
        local.cp(from_path, to_path, overwrite, include_folder_name)


def ls(path: str, full_path: bool = False, recursive: bool = False,
       **kwargs) -> List[str]:
    """ List the contents of a local/s3 directory

    Parameters
    -----------
    path : str
        Local or S3 Path

    full_path : bool
        Include the full path, or just the path relative to `path`

    recursive : bool
        Recursively list within the given path

    kwargs : Dict
        If path is an s3 path, fs: s3fs.S3FileSystem can be specified

    Returns
    --------
    List[str]
    """
    if s3.is_s3path(path):
        return s3.ls(path, full_path, recursive, **kwargs)
    else:
        return local.ls(path, full_path, recursive)


def rm(path: str, dry_run: bool = False, **kwargs) -> None:
    """ Deletes a file or directory

    Parameters
    -----------
    path : str
        File path to delete

    dry_run : bool
        Print out number of files to be deleted and exit. If False, numbe of
        files to be deleted will be logged and files will be removed

    kwargs : Dict
        If path is an s3 path, fs: s3fs.S3FileSystem can be specified

    Returns
    --------
    None
    """
    if s3.is_s3path(path):
        s3.rm(path, dry_run, **kwargs)
    else:
        local.rm(path, dry_run)


def already_exists(path: str, **kwargs) -> bool:
    """ Check if a file/directory already exists

    Parameters
    -----------
    path : str
        File / Directory path

    kwargs : Dict
        If path is an s3 path, fs: s3fs.S3FileSystem can be optionally specified

    Returns
    --------
    bool
    """
    if s3.is_s3path(path):
        return s3.already_exists(path, **kwargs)
    else:
        return local.already_exists(path)


def get_size(path: str, **kwargs) -> int:
    """ Return size of file/directory in bytes

    Parameters
    -----------
    path : str
        File / Directory path

    kwargs : Dict
        If path is an s3 path, fs: s3fs.S3FileSystem can be optionally specified

    Returns
    --------
    int
    """
    fs = kwargs.pop("fs", None)
    if s3.is_s3path(path):
        return s3.get_size(path, fs)
    else:
        return local.get_size(path)


def load_object(path: str, file_type: Optional[str] = None, **kwargs) -> Any:
    """ Load a file into memory

    Parameters
    -----------
    path : str
        Path to the file. If file_type is not specified, an attempt will be
        made to infer the file_type based on the extension.

    file_type : str
        Type of file to load.  Supported options are currently:
            "pickle"
                kwargs are passed to pickle.loads
            "raw"
            "csv"
                Load a CSV file into a pandas DataFrame. Additional kwargs are
                passed to pd.read_csv
            "json"
                kwargs are passed to json.loads
            "parquet"
                Load a parquet dataset in as a pandas DataFrame. Additional
                kwargs are passed to _parquet.load_parquet(). See that function
                for more information
                NOTE: This functionality is still in beta

    kwarg : Dict
        fs : s3fs.S3FileSystem
            Will be passed to s3.load_object if path is an s3path

    Returns
    --------
    Any : Depends on the file_type specified
    """
    # Pop fs from kwargs
    fs = kwargs.pop("fs", None)

    if file_type is None:
        file_type = _file_type_helper(path)

    if file_type == "parquet":
        from ._parquet import load_parquet
        return load_parquet(path, fs=fs, **kwargs)

    if s3.is_s3path(path):
        logger.info(f"Loading {path!r} from S3")
        data_file = s3.load_object(path, fs)
    else:
        path = local._norm_path(path)
        logger.info(f"Loading {path!r} from local directory")
        data_file = open(path, "rb")

    if file_type == "pickle":
        logger.info(f"Loading file as a 'pickle' object. kwargs passed {kwargs!r}")
        data_read = data_file.read()
        obj = pickle.loads(data_read, **kwargs)
    elif file_type == "raw":
        logger.info("Loading file as a 'raw' object")
        obj = data_file.read()
    elif file_type == "csv":
        logger.info("Loading file as a 'csv' object")
        import pandas as pd
        obj = pd.read_csv(data_file, **kwargs)
    elif file_type == "json":
        logger.info(f"loading file as a 'json' object. kwargs passed {kwargs!r}")
        obj = json.load(data_file, **kwargs)
    else:
        if hasattr(data_file, "close"):
            logger.info(f"Closing data_file {data_file!r}")
            data_file.close()
        raise ValueError(f"File type {file_type!r} is not supported")

    if hasattr(data_file, "close"):
        logger.info(f"Closing data_file {data_file!r}")
        data_file.close()

    return obj


def save_object(obj: object, path: str, file_type: Optional[str] = None,
                overwrite: bool = True, protocol: int = pickle.HIGHEST_PROTOCOL,
                **kwargs) -> None:
    """ Save an object in memory to a file

    Parameters
    -----------
    obj : object
        Python object in memory

    path : str
        Local or S3 path to save file. If file_type is not specified, an
        attempt will be made to infer the file_type based on the extension.

    file_type : str
        Type of file to save.
        Supported options are currently:
            "pickle"
                Additional kwargs are passed to pickle.dumps
            "raw"
            "csv"
                Save a pandas DataFrame as a CSV file.  Additional kwargs are
                passed to obj.to_csv
                NOTE: A TypeError will be thrown in "csv" is specified and obj
                is not a pandas DataFrame
            "json"
                Additional kwargs are passed to json.dumps
            "parquet"
                Save a pandas DataFrame to a parquet dataset. Additional kwargs
                are passed to the _save_parquet helper function and are applied
                to either pa.Table.from_pandas() or pq.write_to_dataset()
                depending on the argument.
                NOTE: This functionality is still in beta and currently only works with a pandas dataframe as input.

    overwrite : bool
        Should the file be overwritten if it already exists?

    protocol : int
        Used when calling pickle

    kwargs : Dict
        The following extra parameters can be passed:
            fs : s3fs.S3FileSystem
                Used when the path is an s3 path
            acl : str
                Used to set the Access Control List settings when writing to S3

    Returns
    --------
    None
    """
    fs = kwargs.pop("fs", None)
    acl = kwargs.pop("acl", "bucket-owner-full-control")

    # Check to see if path already exists
    if not overwrite and already_exists(path, fs=fs):
        raise ValueError(f"overwrite set to False and {path!r} already exists")

    if file_type is None:
        file_type = _file_type_helper(path)

    if file_type == "pickle":
        logger.info(f"Saving obj as a pickle file. kwargs passed {kwargs!r}")
        obj = pickle.dumps(obj, protocol=protocol, **kwargs)
    elif file_type == "raw":
        logger.info(f"Saving obj as a raw file.")
        pass
    elif file_type == "csv":
        logger.info(f"Saving obj as a CSV file. kwargs passed {kwargs!r}")
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"obj must be a pandas DataFrame when file_type='csv'. {type(obj)!r} passed")
        obj = obj.to_csv(path_or_buf=None, **kwargs)
    elif file_type == "json":
        logger.info(f"Saving obj as a json file. kwargs passed {kwargs!r}")
        obj = json.dumps(obj, **kwargs)
    elif file_type == "parquet":
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Saving to parquet currently only supports a pandas DataFrame. {type(obj)!r} passed")
        from ._parquet import save_parquet
        return save_parquet(obj, path, fs=fs, **kwargs)
    else:
        raise ValueError(f"file_type={file_type!r} is not supported")

    # Save file to appropriate system
    if s3.is_s3path(path):
        logger.info("Saving object to S3")
        s3.save_object(obj, path, overwrite, fs, acl)
    else:
        logger.info("Saving object to local")
        path = local._norm_path(path)
        if isinstance(obj, (bytes, bytearray)):
            mode = "wb"
        else:
            mode = "w"

        with open(path, mode) as f:
            f.write(obj)


def _file_type_helper(path):
    """ The purpose of this helper is to try an infer the file type based on
        the extension of the input path. This removes the need to specify the
        file type if the property extension is provided.

    A ValueError is raised if the type cannot be inferred

    """
    type_dict = dict(
        pkl="pickle",
        csv="csv",
        json="json",
        parquet="parquet",
        parq="parquet",
        txt="raw"
    )

    extension = path.split(".")[-1]

    if extension not in type_dict:
        raise ValueError(
            f"The file type could not be inferred. Either the specified path "
            f"did not include a file extension or the specified extension is "
            f"not supported. Include a supported extension or specify the file "
            f"type explicitly with the file_type argument. {path!r} passed."
        )

    return type_dict[extension]
