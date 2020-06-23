![Python Utilities](https://avatars3.githubusercontent.com/u/37234308?s=200&v=4)

# dna_util

Data Science Utility Functions

## Modules

### io

The `dna_util.io` module deals with abstracting IO operations between local and S3 filesystems.

Notable functions include:
* `cp` - Copy a file/directory from local/S3 to local/S3
* `ls` - List files located in a directory either local/S3
* `rm` - Remove file/directory from local/S3
* `already_exists` - Test whether a file/directory already exists locally or on S3
* `load_object` - Load a file into memory from local/S3 storage. A variety of file types are supported including "pickle", "raw", "csv", "json", and "parquet"
* `save_object` - Save an object from memory to a local/S3 file. All file types that load_objects supports are supported.
* `is_s3path` - Determine if a path refers to an S3 path or not
* `get_size` - Return the size of the file/directory in bytes

The `io` module also includes a `mlflow` submodule for easily saving and loading artifacts to a dynamic location given the currently active mlflow run.

* `mlflow.load_artifact` - Load a file into memory from local/S3 storage based on the specified run_uuid and subpath. If you are not working with a local `mlruns` directory, a tracking uri must be supplied. Any additional arguments are passed to `load_object`
* `mlflow.save_artifact` - Save an object from memory to local/S3 storage based on the mlflow run's specified artifact store combined with the specified subpath. Any additional argument are passed to `save_object`

NOTE: `mlflow` is not a requirement for this package. If you do not have `mlflow` installed, the submodule will not appear.

## config

Notable functions include:
* `parse_config` - Reads in a local/S3 YAML file and returns it as a dictionary
* `setup_logging` - Setup logging configuration. Also always a custom YAML configuration to be specified to override the `logging.yaml` included with this repo.


## dates

Various date functionality.

Notable functions include:
* `validate_date` - Takes a string, datetime, or timedelta and returns the date in "YYYY-MM-DD" format. Raises an error if the input cannot be validated
* `get_window` - Returns a window object given an input date and a given lookback/lookforward range
* `get_daterange` - Given a window or a start and end date, return an iterator of date strings


## util

Notable function include:
* `generate_token` - Return a random token with a specified number of bits


## Running Tests

From the top level of the repo, run the following command to run all the tests:

```bash
$ python -m pytest
```

If you add functionality, write some tests for it!

## Installing

`pip install git+https://github.com/airdnallc/dna_util.git@v0.0.9#egg=dna_util`
