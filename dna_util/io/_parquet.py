""" Separate module for dealing with parquet with pyarrow """
import logging

import pandas as pd
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq

from dna_util.io import _s3 as s3

logger = logging.getLogger(__name__)


def save_parquet(df: pd.DataFrame, path: str, **kwargs) -> None:
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


def load_parquet(path: str, **kwargs) -> pd.DataFrame:
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

    nthreads : int
        Passed to pyarrow.parquet.ParquetDataset().read()
        Number of columns to read in parallel. Requires that the underlying
        file source is threadsafe

    Any additional kwargs are passed to pyarrow.Table.to_pandas().
    See [documentation](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?highlight=table#pyarrow.Table.to_pandas) for more information

    Returns
    --------
    pd.DataFrame
    """
    logger.info(f"Reading in Parquet dataset to PyArrow Table. kwargs passed {kwargs!r}")

    fs = kwargs.pop("fs", None)
    split_row_groups = kwargs.pop("split_row_groups", False)
    filters = kwargs.pop("filters", None)
    columns = kwargs.pop("columns", None)
    nthreads = kwargs.pop("nthreads", 1)

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

    table = dataset.read(columns=columns, nthreads=nthreads)

    logger.info(f"Converting PyArrow Table to Pandas DataFrame. kwargs passed {kwargs!r}")

    return table.to_pandas(**kwargs)
