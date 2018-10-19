""" Test the overall io module """
import os
import pytest
import json
import pickle

from s3fs.core import S3FileSystem
import boto3
import moto

from dna_util import io
import dna_util.io._s3 as s3
import dna_util.io._local as local

test_bucket_name = "test-bucket"
files = {
    "foo/bar.txt": "This is test file bar",
    "foo/fizz/buzz.txt": "This is test file buzz"
}


@pytest.fixture(scope="session")
def sample_local_dir(tmpdir_factory):
    # Create nested directories
    io_dir = tmpdir_factory.mktemp("io_tests", numbered=False)
    dict_dir = tmpdir_factory.mktemp("io_tests/dict", numbered=False)

    # This is where all tests will write information to
    copy_dir = tmpdir_factory.mktemp("io_tests/copy", numbered=False)

    # Return base path
    return tmpdir_factory.getbasetemp()

@pytest.yield_fixture
def s3_fs():
    # writable local S3 system
    try:
        m = moto.mock_s3()
        m.start()
        client = boto3.client("s3")
        client.create_bucket(Bucket=test_bucket_name, ACL="public-read")

        for f, data in files.items():
            client.put_object(Bucket=test_bucket_name, Key=f, Body=data)
        
        yield S3FileSystem(anon=False)

        for f, data in files.items():
            try:
                client.delete_object(Bucket=test_bucket_name, Key=f, Body=data)
            except:
                pass
    finally:
        m.stop()


@pytest.fixture
def sample_dict(sample_local_dir, s3_fs):
    """
    Creates a testing dictonary, saves it as a json object and a pickle object
    to both local and s3 for use in testing the load_object and save_object
    """
    d = {"foo": "bar", "fizz": 42}

    pkl_obj = pickle.dumps(d)
    json_obj = json.dumps(d)

    # Save to local fpath
    pkl_path = os.path.join(sample_local_dir, "io_tests/dict/dict.pkl")
    json_path = os.path.join(sample_local_dir, "io_tests/dict/dict.json")

    with open(pkl_path, "wb") as f:
        f.write(pkl_obj)

    with open(json_path, "w") as f:
        f.write(json_obj)

    # Save to s3 fpath
    pkl_path = f"s3://{test_bucket_name}/tests/dict/dict.pkl"
    json_path = f"s3://{test_bucket_name}/tests/dict/dict.json"

    with s3_fs.open(pkl_path, "wb") as f:
        f.write(pkl_obj)

    with s3_fs.open(json_path, "w") as f:
        f.write(json_obj)

    return d



class TestSaveObject(object):
    def test_save_dict_as_json_local(self, sample_local_dir, sample_dict):
        test_path = os.path.join(sample_local_dir, "io_tests/dict/dict.json")
        save_path = os.path.join(sample_local_dir, "io_tests/copy/dict.json")

        io.save_object(sample_dict, save_path, file_type="json")

        with open(test_path, "r") as f:
            test_obj = f.read()

        with open(save_path, "r") as f:
            save_obj = f.read()

        assert test_obj == save_obj


    def test_save_dict_as_json_s3(self, s3_fs, sample_dict):
        test_path = f"s3://{test_bucket_name}/tests/dict/dict.json"
        save_path = f"s3://{test_bucket_name}/copy/dict.json"

        io.save_object(sample_dict, save_path, file_type="json", fs=s3_fs)

        test_obj = s3_fs.cat(test_path)
        save_obj = s3_fs.cat(save_path)

        assert test_obj == save_obj


    def test_save_dict_as_pkl_local(self, sample_local_dir, sample_dict):
        test_path = os.path.join(sample_local_dir, "io_tests/dict/dict.pkl")
        save_path = os.path.join(sample_local_dir, "io_tests/copy/dict.pkl")

        io.save_object(sample_dict, save_path, file_type="pickle")

        with open(test_path, "rb") as f:
            test_obj = pickle.load(f)

        with open(save_path, "rb") as f:
            save_obj = pickle.load(f)

        assert test_obj == save_obj


    def test_save_dict_as_pkl_s3(self, s3_fs, sample_dict):
        test_path = f"s3://{test_bucket_name}/tests/dict/dict.pkl"
        save_path = f"s3://{test_bucket_name}/copy/dict.pkl"

        io.save_object(sample_dict, save_path, file_type="pickle", fs=s3_fs)

        test_obj = pickle.loads(s3_fs.cat(test_path))
        save_obj = pickle.loads(s3_fs.cat(save_path))

        assert test_obj == save_obj


    def test_save_invalid_file_type(self):
        obj = [1, 2, 3]
        path = f"s3://{test_bucket_name}/copy/foo"

        with pytest.raises(ValueError):
            io.save_object(obj, path, file_type="foobar", fs=s3_fs)



class TestLoadObject(object):
    def test_load_dict_as_json_local(self, sample_local_dir, sample_dict):
        path = os.path.join(sample_local_dir, "io_tests/dict/dict.json")

        load_obj = io.load_object(path, file_type="json")

        assert sample_dict == load_obj

    
    def test_load_dict_as_json_s3(self, s3_fs, sample_dict):
        path = f"s3://{test_bucket_name}/tests/dict/dict.json"

        load_obj = io.load_object(path, file_type="json", fs=s3_fs)

        assert sample_dict == load_obj


    def test_load_dict_as_pkl_local(self, sample_local_dir, sample_dict):
        path = os.path.join(sample_local_dir, "io_tests/dict/dict.pkl")

        load_obj = io.load_object(path, file_type="pickle")

        assert sample_dict == load_obj


    def test_load_dict_as_pkl_s3(self, s3_fs, sample_dict):
        path = f"s3://{test_bucket_name}/tests/dict/dict.pkl"

        load_obj = io.load_object(path, file_type="pickle", fs=s3_fs)

        assert sample_dict == load_obj


    def test_load_invalid_file_type(self, sample_local_dir):
        path = os.path.join(sample_local_dir, "io_tests/dict/dict.json")

        with pytest.raises(ValueError):
            io.load_object(path, file_type="foobar")