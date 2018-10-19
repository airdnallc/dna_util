import os
import shutil
import logging
from typing import List

logger = logging.getLogger(__name__)


def _norm_path(path: str):
    return os.path.expanduser(os.path.normpath(path))


def already_exists(path: str) -> bool:
    """ Test to see if a file/directory already exists

    Parameters
    -----------
    path : str
        File/Directory path

    Returns:
    bool
    """
    path = _norm_path(path)
    return os.path.exists(path)


def ls(path: str, full_path: bool = False, recursive: bool = False) -> List[str]:
    """ List the files on your local filesystem

    Parameters
    -----------
    path : str
        Local file or directory path

    full_path : bool (default False)
        Include the absolute path or just the file name.  If False and
        recursive is True, any files contained in subfolders will have relative
        path information included

    recursive : bool (default False)
        Recursively list within contained directories

    Returns
    --------
    List[str]
    """
    path = _norm_path(path)
    result: List = []

    if recursive:
        for root, dirs, files in os.walk(path):
            if full_path:
                result.extend(map(lambda f: os.path.join(root, f), files))
            else:
                result.extend(map(lambda f: os.path.join(root, f)[len(path)+1:], files))
        return result
    else:
        for obj in os.scandir(path):
            if full_path:
                name = obj.path
            else:
                name = obj.name

            if obj.is_dir():
                name += "/"

            result.append(name)
        return result


def cp(from_path: str, to_path: str, overwrite: bool = True, 
       include_folder_name: bool = True, **kwargs) -> None:
    """ Copy a local file or recursively copy a directory of local files

    Parameters
    -----------
    from_path : str
        File/directory to copy

    to_path : str
        File/directory to copy to

    overwrite : bool (default True)
        Should you overwrite the file/directory?  A ValueError is raised if the
        file/directory already exists and overwrite is False

    include_folder_name : bool (default True)
        If copying a directory, add the directory name automatically to the
        to_path.  i.e. if True, the entire folder will be copied to the
        to_path. If False, the *contents* of the directory will be copied to
        the to_path

    Returns
    --------
    None
    """
    from_path, to_path = _norm_path(from_path), _norm_path(to_path)

    if os.path.isdir(from_path):

        if include_folder_name:
            # Add from_path folder name to to_path
            to_path = os.path.join(to_path, os.path.basename(from_path))
            logger.debug(f"to_path after adding folder name: {to_path!r}")

        if not overwrite and already_exists(to_path):
            raise ValueError(f"Overwrite set to false but {to_path!r} already exists")

        # Clear out directory before copying
        if os.path.isdir(to_path):
            shutil.rmtree(to_path)

        shutil.copytree(from_path, to_path)
    else:
        if not overwrite and already_exists(to_path):
            raise ValueError(f"Overwrite set to false but {to_path!r} already exists")

        shutil.copy(from_path, to_path)


def get_size(path: str) -> int:
    """ Return size of file/directory in bytes

    Parameters
    -----------
    path : str
        Path to file/directory

    Returns
    --------
    int
        Size in bytes
    """
    path = _norm_path(path)

    if os.path.isdir(path):
        total_size = 0
        for root, dirs, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                total_size += os.path.getsize(fp)
    else:
        total_size = os.path.getsize(path)
    return total_size


def rm(path: str, dry_run: bool = False) -> None:
    """ Delete a file

    Parameters
    -----------
    path : str
        File path to delete

    dry_run : bool (default False)
        Print out number of files to be deleted and exit.  If False, number of 
        files to be deleted will be logged and files will be removed 

    Returns
    --------
    None
    """
    path = _norm_path(path)

    num_files = len(ls(path, recursive=True))

    if dry_run:
        print(f"Deleting {path!r} would remove {num_files} file(s)")
        return

    if os.path.isdir(path):
        logger.info(f"Removing {num_files} file(s) located in directory {path!r}")
        shutil.rmtree(path)
    else:
        logger.info(f"Removing 1 file located at {path!r}")
        try:
            os.remove(path)
        except OSError:
            logger.warning(f"OSError when attempting to delete {path!r}")