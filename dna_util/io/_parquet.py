""" Separate module for dealing with parquet with pyarrow """
import logging
import subprocess
import sys

import pandas as pd
import s3fs

from dna_util.io import _s3 as s3
from dna_util.util import parse_args

logger = logging.getLogger(__name__)


def save_parquet(df: pd.DataFrame, path: str, engine: str = "auto",
                 **kwargs) -> None:
    """ Helper function to save a DataFrame to parquet using either fastparquet
        or pyarrow

    Parameters
    -----------
    df : pd.DataFrame
        The DataFrame to export to parquet

    path : str
        The root path the save the DataFrame to, this can either be S3 or local

    engine : ["auto", "pyarrow", "fastparquet"] (default "auto")
        Parquet reader library to use. If only one library is installed, it
        will use that one; if both, it will use ‘fastparquet’

    """
    assert engine in {"auto", "fastparquet", "pyarrow"}
    if engine == "auto":
        # Determine which packages are installed
        reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])
        installed_packages = {r.decode().split('==')[0] for r in reqs.split()}
        if "fastparquet" in installed_packages:
            engine = "fastparquet"
        elif "pyarrow" in installed_packages:
            engine = "pyarrow"
        else:
            raise ImportError("Neither fastparquet nor pyarrow are installed")

    if engine == "fastparquet":
        save_parquet_fp(df, path, **kwargs)
    else:
        save_parquet_pa(df, path, **kwargs)


def load_parquet(path: str, engine: str = "auto",
                 **kwargs) -> pd.DataFrame:
    """ Helper function to load a parquet file into a DataFrame using either
        fastparquet or pyarrow

    Parameters
    -----------
    path : str
        The root path the save the DataFrame to, this can either be S3 or local

    engine : ["auto", "pyarrow", "fastparquet"] (default "auto")
        Parquet reader library to use. If only one library is installed, it
        will use that one; if both, it will use ‘fastparquet’

    Returns
    --------
    pd.DataFrame
    """
    assert engine in {"auto", "fastparquet", "pyarrow"}
    if engine == "auto":
        # Determine which packages are installed
        reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])
        installed_packages = {r.decode().split('==')[0] for r in reqs.split()}
        if "fastparquet" in installed_packages:
            engine = "fastparquet"
        elif "pyarrow" in installed_packages:
            engine = "pyarrow"
        else:
            raise ImportError("Neither fastparquet nor pyarrow are installed")

    if engine == "fastparquet":
        return load_parquet_fp(path, **kwargs)
    else:
        return load_parquet_pa(path, **kwargs)


def save_parquet_pa(df: pd.DataFrame, path: str, **kwargs) -> None:
    """ Helper function to save a DataFrame to a parquet DataSet

    See the [PyArrow Docs](https://arrow.apache.org/docs/python/index.html) for more information

    Parameters
    -----------
    df : pd.DataFrame
        The DataFrame to export to parquet

    path : str
        The root path the save the DataFrame to, this can either be S3 or local

    Additional Parameters
    ----------------------
    The following parameters are optional and can tweak how the DataFrame gets
    converted to parquet.

    fs : s3fs.S3FileSystem
        This will be used to save the data to S3 if applicable

    schema : pyarrow.Schema
        Passed to pyarrow.Table.from_pandas()
        The expected schema of the Arrow Table. This can be used to indicate
        the type of columns if we cannot infer it automatically.

    preserve_index : bool (default False)
        Passed to pyarrow.Table.from_pandas()
        Whether to store the index as an additional column in the resulting Table

    nthreads : int
        Passed to pyarrow.Table.from_pandas()
        If greater than 1, convert columns to Arrow in parallel using indicated
        number of threads

    columns : List[str]
        Passed to pyarrow.Table.from_pandas()
        List of columns to be converted. Uses all columns be default

    partition_cols : List[str]
        Passed to pyarrow.parquet.write_to_dataset()
        Column names by which to partition the dataset
        Columns are partitioned in the order that they are given

    Returns
    --------
    None
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    logger.info(f"Converting dataframe to PyArrow Table. kwargs passed {kwargs!r}")

    fs = kwargs.pop("fs", None)
    schema = kwargs.pop("schema", None)
    preserve_index = kwargs.pop("preserve_index", False)
    nthreads = kwargs.pop("nthreads", None)
    columns = kwargs.pop("columns", None)
    partition_cols = kwargs.pop("partition_cols", None)

    # Convert the dataframe into a pyArrow Table object
    table = pa.Table.from_pandas(
        df,
        schema=schema,
        preserve_index=preserve_index,
        nthreads=nthreads,
        columns=columns
    )

    if not s3.is_s3path(path):
        fs = None
    elif fs is None:
        fs = s3fs.S3FileSystem()

    logger.info("Writing Arrow Table to Parquet Dataset")

    pq.write_to_dataset(
        table,
        path,
        partition_cols=partition_cols,
        filesystem=fs,
        preserve_index=preserve_index
    )

    logger.info("Done.")


def save_parquet_fp(df: pd.DataFrame, path: str, **kwargs) -> None:
    """ Helper function to save a DataFrame to a parquet DataSet

    See the [fastparquet Docs](https://fastparquet.readthedocs.io/en/latest/api.html) for more information

    Parameters
    -----------
    df : pd.DataFrame
        The DataFrame to export to parquet

    path : str
        The root path the save the DataFrame to, this can either be S3 or local

    Additional Parameters
    ----------------------
    The following parameters are optional and can tweak how the DataFrame gets
    converted to parquet.

    fs : s3fs.S3FileSystem
        This will be used to save the data to S3 if applicable

    file_scheme: "simple"|"hive" (default "hive")
        If simple: all goes in a single file
        If hive: each row group is in a separate file, and a separate file
        (called "_metadata") contains the metadata.

    write_index: bool
        Whether or not to write the index to a separate column.  By default we
        write the index *if* it is not 0, 1, ..., n.

    partition_on: List[str]
        Passed to groupby in order to split data within each row-group,
        producing a structured directory tree. Note: as with pandas, null
        values will be dropped. Ignored if file_scheme is simple.

    See [fastparquet.write](https://fastparquet.readthedocs.io/en/latest/api.html#fastparquet.write)
    documentation for full details.

    Returns
    --------
    None
    """
    import fastparquet as fp

    fs = kwargs.pop("fs", None)
    file_scheme = kwargs.pop("file_scheme", "hive")

    if s3.is_s3path(path):
        fs = fs or s3fs.S3FileSystem()
        myopen = fs.open
    else:
        myopen = open

    logger.info("Writing Dataframe to Parquet using fastparquet")

    fp.write(
        path,
        df,
        file_scheme=file_scheme,
        open_with=myopen,
        **kwargs
    )

    logger.info("Done.")


def load_parquet_pa(path: str, **kwargs) -> pd.DataFrame:
    """ Helper function to load a parquet Dataset as a Pandas DataFrame

    Parameters
    -----------
    path : str
        The root directory of the Parquet Dataset stored locally or in S3

    Additional Parameters
    ----------------------
    The following parameters are optional and can tweak how the Dataset gets
    converted back to a DataFrame

    split_row_groups : bool (default False)
        Passed to pyarrow.parquet.ParquetDataset()
        Divide files into pieces for each row group in the file

    filters : List[Tuple]
        Passed to pyarrow.parquet.ParquetDataset()
        List of filters to apply, like `[('x', '=', 0), ...]`. This implements
        partition-level (hive) filtering only, i.e., to prevent the loading of
        some files of the dataset.

    columns : List[str]
        Passed to pyarrow.parquet.ParquetDataset().read()
        Names of columns to read from the dataset

    Any additional kwargs are passed to pyarrow.Table.to_pandas().
    See [documentation](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?highlight=table#pyarrow.Table.to_pandas) for more information

    Returns
    --------
    pd.DataFrame
    """
    import pyarrow.parquet as pq

    logger.info(f"Reading in Parquet dataset to PyArrow Table. kwargs passed {kwargs!r}")

    fs = kwargs.pop("fs", None)
    split_row_groups = kwargs.pop("split_row_groups", False)
    filters = kwargs.pop("filters", None)
    columns = kwargs.pop("columns", None)

    if not s3.is_s3path(path):
        fs = None
    elif fs is None:
        fs = s3fs.S3FileSystem()

    dataset = pq.ParquetDataset(
        path,
        filesystem=fs,
        split_row_groups=split_row_groups,
        filters=filters
    )

    table = dataset.read(columns=columns)

    logger.info(f"Converting PyArrow Table to Pandas DataFrame. kwargs passed {kwargs!r}")

    return table.to_pandas(**kwargs)


def load_parquet_fp(path: str, **kwargs) -> pd.DataFrame:
    """ Helper function to load a parquet Dataset as a Pandas DataFrame using
        fastparquet

    First creates a [ParquetFile](https://fastparquet.readthedocs.io/en/latest/api.html#fastparquet.ParquetFile)
    and then converts the ParquetFile to a DataFrame using .to_pandas.
    Refer to the fastparquet documentation for accepted arguments

    Parameters
    -----------
    path : str
        The root directory of the Parquet Dataset stored locally or in S3

    Returns
    --------
    pd.DataFrame
    """
    import fastparquet as fp

    logger.info(f"Reading in Parquet dataset to ParquetFile. kwargs passed {kwargs!r}")

    fs = kwargs.pop("fs", None)

    # Pull out arguments that should be directed to to_pandas
    to_pandas_args = parse_args(fp, ["ParquetFile", "to_pandas"], **kwargs)
    # Remove these args from kwargs
    kwargs = {k: v for k, v in kwargs.items() if k in set(kwargs) - set(to_pandas_args)}

    if s3.is_s3path(path):
        fs = fs or s3fs.S3FileSystem()
        myopen = fs.open
    else:
        myopen = open

    pf = fp.ParquetFile(
        path,
        open_with=myopen,
        **kwargs
    )

    df = pf.to_pandas(**to_pandas_args)
    return df
