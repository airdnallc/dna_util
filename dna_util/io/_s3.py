import os
from typing import Optional, Tuple, List, io
import logging
from concurrent.futures import ThreadPoolExecutor

import s3fs

from dna_util.io import _local as local

logger = logging.getLogger(__name__)


def _norm_s3_path(path: str) -> str:
    new_path = os.path.normpath(path.replace("s3://", "").replace("s3n://", ""))
    logger.debug(f"Normalizing {path!r} to {new_path!r}")
    return new_path


def already_exists(path: str, fs: Optional[s3fs.S3FileSystem] = None, **kwargs) -> bool:
    """ Test to see if a file/directory already exists

    Parameters
    -----------
    path : str
        File/directory path

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    **kwargs
        Extra args to be passed to S3FileSystem if one wasn't provided.  e.g.
        passing profile_name="..." will use that profile defined in your
        ~/.aws/credentials file

    Returns
    --------
    bool
    """
    if fs is None:
        fs = s3fs.S3FileSystem(**kwargs)
    return fs.exists(path)


def is_s3path(path: str) -> bool:
    """ Determines if a filepath is an s3 path

    Parameters
    -----------
    path : str
        file path

    Returns
    --------
    bool
    """
    # Ensure path isn't a py.path.local object
    path = str(path)
    return path.startswith("s3://") or path.startswith("s3n://")


def split_s3path(path: str) -> Tuple[str, str]:
    """ Split an s3 filepath into its bucket and key

    Parameters
    -----------
    path : str
        s3 file path

    Returns
    --------
    Tuple[<bucket>, <key>]
    """
    if not is_s3path(path):
        raise ValueError(f"{path!r} is not a valid s3 path.")
    path_lst = path.split("/")
    bucket, key = path_lst[2], "/".join(path_lst[3:])
    return bucket, key


def is_dir(path: str, fs: Optional[s3fs.S3FileSystem] = None, **kwargs) -> bool:
    """ Test if a given s3 path is a directory or not

    Parameters
    -----------
    path : str
        file path

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    **kwargs
        Extra args to be passed to S3FileSystem if one wasn't provided.  e.g.
        passing profile_name="..." will use that profile defined in your
        ~/.aws/credentials file

    Returns
    --------
    bool
    """
    if not is_s3path(path):
        raise ValueError(f"{path!r} is not a valid s3path.")

    if fs is None:
        fs = s3fs.S3FileSystem(**kwargs)

    path = _norm_s3_path(path)
    lst = fs.ls(path)

    if len(lst) == 1 and path == lst[0]:
        return False
    else:
        return True


def cp(from_path: str, to_path: str, overwrite: bool = True,
       include_folder_name: bool = True,
       fs: Optional[s3fs.S3FileSystem] = None, **kwargs) -> None:
    """ Copy a file/directory to/from s3 and your local machine

    Parameters
    -----------
    from_path : str
        File path containing file(s) to copy

    to_path : str
        File path to copy file(s) to

    overwrite : bool (default True)
        Should the to_path be overwritten if it already exists?

    include_folder_name : bool (default True)
        If copying a directory, add the directory name automatically to the
        to_path.  i.e. if True, the entire folder will be copied to the
        to_path. If False, the *contents* of the directory will be copied to
        the to_path

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    **kwargs
        "acl" to specify how file permission are set
        "num_threads" to specify number of threads when copying (default 100)
            NOTE: This is only used when copying a directory of files
        Extra args to be passed to S3FileSystem

    Returns
    --------
    None
    """
    s3FileArgs = {
        "acl": kwargs.pop("acl", "bucket-owner-full-control"),
        "num_threads": kwargs.pop("num_threads", 100)
    }

    if fs is None:
        fs = s3fs.S3FileSystem(**kwargs)

    if is_s3path(from_path):
        ##################################
        # Copy s3 file(s) to local or s3 #
        ##################################
        if not already_exists(from_path, fs):
            raise ValueError(f"from_path: {from_path!r} does not exist")

        # Use existing file/directory name if one was not specified
        if include_folder_name and is_dir(from_path, fs):
            folder_name = os.path.basename(os.path.normpath(from_path))
            to_path = os.path.join(to_path, folder_name)
            logger.debug(f"to_path after adding folder name: {to_path!r}")

        if is_s3path(to_path):
            #################
            # s3 -> s3 copy #
            #################
            logger.debug(f"Copying s3 files: {from_path!r} to s3 location: {to_path!r}")
            _s3_to_s3_cp(from_path, to_path, overwrite, fs, **s3FileArgs)
        else:
            #####################
            # s3 --> local copy #
            #####################
            logger.debug(f"Copying s3 files: {from_path!r} to local location: {to_path!r}")
            _s3_to_local_cp(from_path, to_path, overwrite, fs, **s3FileArgs)

    else:
        ############################
        # Copy local file(s) to s3 #
        ############################
        if not local.already_exists(from_path):
            raise ValueError(f"{from_path!r} does not exist")

        if include_folder_name and os.path.isdir(from_path):
            folder_name = os.path.basename(local._norm_path(from_path))
            to_path = os.path.join(to_path, folder_name)
            logger.debug(f"to_path after adding folder name: {to_path!r}")

        logger.debug(f"Copying local files: {from_path!r} to s3 location: {to_path!r}")
        _local_to_s3_cp(from_path, to_path, overwrite, fs, **s3FileArgs)


def ls(path: str, full_path: bool = False, recursive: bool = False,
       fs: Optional[s3fs.S3FileSystem] = None, **kwargs) -> List[str]:
    """ List the contents under an s3 key/"directory"

    Will throw a ValueError if the given path doesn't exist

    Parameters
    -----------
    path : str
        Full s3 path

    full_path : bool (default False)
        Include full path, or just the path relative to path

    recursive : bool (default False)
        Recursively list within the given path

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    **kwargs
        Extra args to be passed to S3FileSystem if one wasn't provided.  e.g.
        passing profile_name="..." will use that profile defined in your
        ~/.aws/credentials file

    Returns
    --------
    List[str]
    """
    if fs is None:
        fs = s3fs.S3FileSystem(**kwargs)

    if not is_s3path(path):
        raise ValueError(f"{path!r} is not a valid s3 path")

    if is_dir(path, fs):
        if recursive:
            files = fs.walk(path)
        else:
            files = fs.ls(path, detail=True)
            files = [f["Key"]+"/" if f["StorageClass"] == "DIRECTORY" else f["Key"] for f in files]
    else:
        files = [_norm_s3_path(path)]
        path = "/".join(path.split("/")[:-1])

    if full_path:
        files = [os.path.join("s3://", f) for f in files]
    else:
        strip_path = _norm_s3_path(path) + "/"
        files = [f.replace(strip_path, "") for f in files]

    return sorted(files)


def rm(path: str, dry_run: bool = False,
       fs: Optional[s3fs.S3FileSystem] = None, **kwargs) -> None:
    """ Delete a file/directory

    Parameters
    -----------
    path : str
        File path to delete

    dry_run : bool (default False)
        Print out number of files to be deleted and exit.  If False, number of
        files to be deleted will be logged and files will be removed

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    **kwargs
        Extra args to be passed to S3FileSystem if one wasn't provided.  e.g.
        passing profile_name="..." will use that profile defined in your
        ~/.aws/credentials file

    Returns
    --------
    None
    """
    if not is_s3path(path):
        raise ValueError(f"{path!r} is not a valid s3 path.")

    if fs is None:
        fs = s3fs.S3FileSystem(**kwargs)

    num_files = len(ls(path, recursive=True, fs=fs))

    if dry_run:
        print(f"Deleting {path!r} would remove {num_files} file(s)")
        return

    if is_dir(path, fs):
        logger.info(f"Removing {num_files} file(s) located in directory {path!r}")
        fs.rm(path, recursive=True)
    else:
        logger.info(f"Removing 1 file located at {path!r}")
        fs.rm(path)


def save_object(obj: object, path: str, overwrite: bool = True,
                fs: Optional[s3fs.S3FileSystem] = None,
                acl: str = "bucket-owner-full-control", **kwargs) -> None:
    """ Save an object from memory to s3

    Parameters
    -----------
    obj : object
        Python object in memory. Should be the result of some sort of
        serialization process on the object. e.g. some sort of byte/text stream

    path : str
        Where to save the object to in S3

    overwrite : bool (default True)
        Should the file be overwritten if it already exists?

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    acl : str
        Access Control List for writing data to s3; be default give the bucket
        owner full control over the data.

    Returns
    --------
    None
    """
    if not fs:
        fs = s3fs.S3FileSystem(**kwargs)

    if not overwrite and already_exists(path, fs):
        raise ValueError(f"Overwrite set to False and {path!r} already exists")

    if isinstance(obj, (bytes, bytearray)):
        mode = "wb"
    else:
        mode = "w"

    with fs.open(path, mode, acl=acl) as f:
        f.write(obj)


def load_object(path: str, fs: Optional[s3fs.S3FileSystem] = None, **kwargs) -> io:
    """ Load an object from s3 into memory

    Parameters
    -----------
    path : str
        The path of the s3 file

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    Returns
    --------
    typing.io
        An open instance of the file (that can be read via read())
    """
    if not fs:
        fs = s3fs.S3FileSystem(**kwargs)

    if not already_exists(path, fs):
        raise ValueError(f"{path!r} does not exist")

    return fs.open(path)


def get_size(path: str, fs: Optional[s3fs.S3FileSystem] = None, **kwargs) -> int:
    """ Return size of file/directory in bytes

    Parameters
    -----------
    path : str
        Path to s3 file/directory

    fs : s3fs.S3FileSystem
        If None, an instance of S3FileSystem will be created

    **kwargs
        Extra args to be passed to S3FileSystem if one wasn't provided.  e.g.
        passing profile_name="..." will use that profile defined in your
        ~/.aws/credentials file

    Returns
    --------
    int
    """
    if not fs:
        fs = s3fs.S3FileSystem(**kwargs)

    if is_dir(path, fs):
        return sum(map(lambda fpath: get_size(fpath, fs), ls(path, full_path=True, recursive=True, fs=fs)))
    else:
        return fs.info(path)["Size"]


def _s3_to_s3_cp(from_path: str, to_path: str, overwrite: bool,
                 fs: s3fs.S3FileSystem, **kwargs) -> None:
    from_path = _norm_s3_path(from_path)
    to_path = _norm_s3_path(to_path)
    files = fs.walk(from_path)

    if files:
        ################################
        # Copying a directory of files #
        ################################
        to_files = [os.path.join(to_path, f.replace(from_path+"/", "")) for f in files]

        # Ensure we aren't overwriting any files
        if not overwrite:
            for to_file in to_files:
                if already_exists(to_file, fs):
                    raise ValueError(f"Overwrite set to False and {to_file!r} exists")

        num_threads = kwargs.pop("num_threads", 100)
        # Turn off connectionpool warnings
        logging.getLogger("urllib3.connectionpool").setLevel(logging.CRITICAL)
        with ThreadPoolExecutor(num_threads) as executor:
            for from_file, to_file in zip(files, to_files):
                executor.submit(fs.copy, from_file, to_file, **kwargs)
    else:
        #########################
        # Copying a single file #
        #########################

        # Ensure we aren't overwriting the file
        if not overwrite and already_exists(to_path, fs):
            raise ValueError(f"Overwrite set to False and {to_file!r} exists")

        fs.copy(from_path, to_path, **kwargs)


def _s3_to_local_cp(from_path: str, to_path: str, overwrite: bool,
                    fs: s3fs.S3FileSystem, **kwargs) -> None:
    from_path = _norm_s3_path(from_path)
    to_path = local._norm_path(to_path)
    files = fs.walk(from_path)

    if files:
        ################################
        # Copying a directory of files #
        ################################

        # Check to see if to_path already exists
        if not overwrite and local.already_exists(to_path):
            raise ValueError(f"Overwrite set to False and {to_path!r} "
                             f"already exists")
        elif local.already_exists(to_path):
            local.rm(to_path)

        # Make the root directory to fill in
        os.makedirs(to_path)

        # Need to create any additional subfolders
        _local_create_subfolders(from_path, to_path, fs)

        to_files = [os.path.join(to_path, f.replace(from_path+"/", "")) for f in files]

        num_threads = kwargs.pop("num_threads", 100)
        # Turn off connectionpool warnings
        logging.getLogger("urllib3.connectionpool").setLevel(logging.CRITICAL)
        with ThreadPoolExecutor(num_threads) as executor:
            for from_file, to_file in zip(files, to_files):
                executor.submit(fs.get, from_file, to_file, **kwargs)
    else:
        ######################
        # Copy a single file #
        ######################
        if not overwrite and local.already_exists(to_path, fs):
            raise ValueError(f"Overwrite set to False and {to_path!r} already "
                             f"exists")

        fs.get(from_path, to_path, **kwargs)


def _local_to_s3_cp(from_path, to_path, overwrite, fs, **kwargs):
    from_path = local._norm_path(from_path)

    if not overwrite and already_exists(to_path, fs):
        raise ValueError(f"Overwrite set to False and {to_path!r} "
                         f"already exists")

    if os.path.isdir(from_path):
        ###########################
        # Copy directory of files #
        ###########################
        files = local.ls(from_path, full_path=True, recursive=True)
        to_files = [os.path.join(to_path, f) for f in local.ls(from_path, recursive=True)]

        num_threads = kwargs.pop("num_threads", 100)
        # Turn off connectionpool warnings
        logging.getLogger("urllib3.connectionpool").setLevel(logging.CRITICAL)
        with ThreadPoolExecutor(num_threads) as executor:
            for from_file, to_file in zip(files, to_files):
                executor.submit(fs.put, from_file, to_file, **kwargs)
    else:
        ######################
        # Copy a single file #
        ######################
        fs.put(from_path, to_path, **kwargs)


def _local_create_subfolders(from_path: str, to_path: str,
                             fs: s3fs.S3FileSystem) -> None:
    """ Helper for creating subdirectories when calling _s3_to_local_cp
    """
    files = fs.ls(from_path, detail=True)

    subfolders = [f["Key"].replace(from_path+"/", "") for f in files if f["StorageClass"] == "DIRECTORY"]

    for sub in subfolders:
        from_sub_path = os.path.join(from_path, sub)
        path_to_create = os.path.join(to_path, sub)

        logger.debug(f"Creating local subfolder {to_path!r}")

        os.makedirs(path_to_create)
        _local_create_subfolders(from_sub_path, path_to_create, fs)
