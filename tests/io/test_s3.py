import os
import pytest
import json
import pickle

from s3fs.core import S3FileSystem
import boto3
import moto

import dna_util.io._s3 as s3

test_bucket_name = "test-bucket"
files = {
    "foo/bar.txt": "This is test file bar",
    "foo/fizz/buzz.txt": "This is test file buzz"
}

@pytest.fixture(scope="session")
def sample_local_dir(tmpdir_factory):
    # Create nested directories
    foo_dir = tmpdir_factory.mktemp("foo")
    fizz_dir = tmpdir_factory.mktemp("foo/fizz")
    
    # Create sample files
    bar_txt = foo_dir.join("bar.txt")
    bar_txt.write("This is test file bar")
    buzz_txt = fizz_dir.join("buzz.txt")
    buzz_txt.write("This is test file buzz")

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


def test_split_s3path():
    path = "s3://airdna-data/scratch/ewellinger"
    bucket, key = s3.split_s3path(path)
    assert bucket == "airdna-data"
    assert key == "scratch/ewellinger"


def test_is_s3path(tmpdir):
    path = "s3://airdna-data/scratch/ewellinger"
    assert s3.is_s3path(path)
    assert not s3.is_s3path(tmpdir)


def test_simple(s3_fs):
    data = b"a" * 100

    fpath = os.path.join(test_bucket_name, "foobar")
    
    with s3_fs.open(fpath, "wb") as f:
        f.write(data)

    with s3_fs.open(fpath, "rb") as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


class TestS3IsDir(object):
    def test_is_dir(self, s3_fs):
        
        test_dir_path = f"s3://{test_bucket_name}/foo"
        assert s3.is_dir(test_dir_path, fs=s3_fs)

        # Test with trailing slash
        test_dir_path += "/"
        assert s3.is_dir(test_dir_path, fs=s3_fs)

    def test_is_not_dir(self, s3_fs):
        test_path = f"s3://{test_bucket_name}/foo/bar.txt"
        assert not s3.is_dir(test_path, fs=s3_fs)

        test_path += "/"
        assert not s3.is_dir(test_path, fs=s3_fs)

    def test_non_s3_path(self, s3_fs, sample_local_dir):

        with pytest.raises(ValueError):
            s3.is_dir(sample_local_dir, fs=s3_fs)


class TestS3Cp(object):
    def test_local_s3_cp_file(self, sample_local_dir, s3_fs):
        from_path = os.path.join(sample_local_dir, "foo", "bar.txt")
        to_path = f"s3://{test_bucket_name}/tmp/test/bar.txt"

        with pytest.raises(FileNotFoundError):
            s3_fs.cat(to_path)

        s3.cp(from_path, to_path, fs=s3_fs)

        assert s3_fs.cat(to_path) == b"This is test file bar"

    def test_local_s3_cp_dir(self, sample_local_dir, s3_fs):
        from_path = os.path.join(sample_local_dir, "foo")
        to_path = f"s3://{test_bucket_name}/tmp/test2"

        bar_path = os.path.join(to_path, "foo", "bar.txt")
        buzz_path = os.path.join(to_path, "foo", "fizz", "buzz.txt")

        with pytest.raises(FileNotFoundError):
            s3_fs.cat(buzz_path)

        s3.cp(from_path, to_path, fs=s3_fs)

        assert s3_fs.cat(bar_path) == b"This is test file bar"
        assert s3_fs.cat(buzz_path) == b"This is test file buzz"

        # Copy contents of directory
        from_path = os.path.join(sample_local_dir, "foo")
        to_path = f"s3://{test_bucket_name}/tmp/test2"

        bar_path = os.path.join(to_path, "bar.txt")
        buzz_path = os.path.join(to_path, "fizz", "buzz.txt")

        with pytest.raises(FileNotFoundError):
            s3_fs.cat(buzz_path)

        s3.cp(from_path, to_path, include_folder_name=False, fs=s3_fs)

        assert s3_fs.cat(bar_path) == b"This is test file bar"
        assert s3_fs.cat(buzz_path) == b"This is test file buzz"


    def test_s3_s3_cp_file(self, s3_fs):
        from_path = f"s3://{test_bucket_name}/foo/bar.txt"
        to_path = f"s3://{test_bucket_name}/tmp3/bar.txt"

        with pytest.raises(FileNotFoundError):
            s3_fs.cat(to_path)

        s3.cp(from_path, to_path, fs=s3_fs)

        assert s3_fs.cat(from_path) == b"This is test file bar"
        assert s3_fs.cat(to_path) == b"This is test file bar"

    def test_s3_s3_cp_dir(self, s3_fs):
        from_path = f"s3://{test_bucket_name}/foo"
        to_path = f"s3://{test_bucket_name}/tmp4"
        bar_path = f"s3://{test_bucket_name}/tmp4/foo/bar.txt"

        with pytest.raises(FileNotFoundError):
            s3_fs.cat(bar_path)

        s3.cp(from_path, to_path, fs=s3_fs)

        assert s3_fs.cat(bar_path) == b"This is test file bar"

        # Copy the contents of folder
        from_path = f"s3://{test_bucket_name}/foo"
        to_path = f"s3://{test_bucket_name}/tmp5"
        bar_path = f"s3://{test_bucket_name}/tmp5/bar.txt"

        with pytest.raises(FileNotFoundError):
            s3_fs.cat(bar_path)

        s3.cp(from_path, to_path, include_folder_name=False, fs=s3_fs)

        assert s3_fs.cat(bar_path) == b"This is test file bar"

    def test_s3_local_cp_file(self, s3_fs, tmpdir):
        from_path = f"s3://{test_bucket_name}/foo/bar.txt"
        to_path = tmpdir.join("foobar.txt")

        s3.cp(from_path, to_path, fs=s3_fs)

        assert to_path.read() == "This is test file bar"
        
    def test_s3_local_cp_dir(self, s3_fs, tmpdir):
        from_path = f"s3://{test_bucket_name}/foo"
        
        bar_path = tmpdir.join("foo/bar.txt")
        buzz_path = tmpdir.join("foo/fizz/buzz.txt")

        s3.cp(from_path, tmpdir, fs=s3_fs)

        assert bar_path.read() == "This is test file bar"
        assert buzz_path.read() == "This is test file buzz"

        # Copy contents of folder
        bar_path = tmpdir.join("bar.txt")
        buzz_path = tmpdir.join("fizz/buzz.txt")

        s3.cp(from_path, tmpdir, include_folder_name=False, fs=s3_fs)

        assert bar_path.read() == "This is test file bar"
        assert buzz_path.read() == "This is test file buzz"


class TestS3Ls(object):

    def test_ls_non_existent_path(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo/foobar/"
        with pytest.raises(ValueError):
            s3.ls(path, fs=s3_fs)


    def test_ls(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo"

        expected_lst = ["bar.txt", "fizz/"]

        # no full_path and not recursive
        assert s3.ls(path, fs=s3_fs) == expected_lst
        assert s3.ls(path+"/", fs=s3_fs) == expected_lst

    
    def test_ls_full_path(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo"

        expected_lst = [
            f"s3://{test_bucket_name}/foo/bar.txt", 
            f"s3://{test_bucket_name}/foo/fizz/"
        ]

        # full_path but not recursive
        assert s3.ls(path, full_path=True, fs=s3_fs) == expected_lst
        assert s3.ls(path+"/", full_path=True, fs=s3_fs) == expected_lst

    
    def test_ls_recursive(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo"

        expected_lst = ["bar.txt", "fizz/buzz.txt"]

        # no full_path with recursive
        assert s3.ls(path, recursive=True, fs=s3_fs) == expected_lst
        assert s3.ls(path+"/", recursive=True, fs=s3_fs) == expected_lst


    def test_ls_full_path_recursive(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo"

        expected_lst = [
            f"s3://{test_bucket_name}/foo/bar.txt", 
            f"s3://{test_bucket_name}/foo/fizz/buzz.txt"
        ]

        # full_path but not recursive
        assert s3.ls(path, full_path=True, recursive=True, fs=s3_fs) == expected_lst
        assert s3.ls(path+"/", full_path=True, recursive=True, fs=s3_fs) == expected_lst

    
    def test_ls_single_file(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo/bar.txt"

        expected_lst = ["bar.txt"]

        assert s3.ls(path, fs=s3_fs) == expected_lst

    
    def test_ls_single_file_full_path(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo/bar.txt"

        expected_lst = [path]

        assert s3.ls(path, full_path=True, fs=s3_fs) == expected_lst


class TestS3Rm(object):
    def test_rm_file_dry_run(self, s3_fs, capfd):
        path = f"s3://{test_bucket_name}/foo/bar.txt"

        s3.rm(path, dry_run=True, fs=s3_fs)

        out, err = capfd.readouterr()

        assert out == f"Deleting 's3://{test_bucket_name}/foo/bar.txt' would remove 1 file(s)\n"

    def test_rm_dir_dry_run(self, s3_fs, capfd):
        path = f"s3://{test_bucket_name}/foo"

        s3.rm(path, dry_run=True, fs=s3_fs)

        out, err = capfd.readouterr()

        assert out == f"Deleting 's3://{test_bucket_name}/foo' would remove 2 file(s)\n"


class TestSaveObject(object):
    def test_save_dict(self, s3_fs):
        d = {"foo": "bar", "fizz": 42}

        path = f"s3://{test_bucket_name}/save_object/dict.json"
        obj = json.dumps(d)

        s3.save_object(obj, path, fs=s3_fs)

        new_d = json.loads(s3_fs.cat(path))

        assert d == new_d

    def test_save_dict_as_pickle(self, s3_fs):
        d = {"foo": "bar", "fizz": 42}

        path = f"s3://{test_bucket_name}/save_object/dict.pkl"
        obj = pickle.dumps(d)

        s3.save_object(obj, path, fs=s3_fs)

        new_d = pickle.loads(s3_fs.cat(path))

        assert d == new_d




class TestLoadObject(object):
    def test_load_dict(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo/bar.txt"
        
        obj = s3.load_object(path, s3_fs)

        assert obj.read() == b"This is test file bar"

    def test_load_dict_pickle(self, s3_fs):
        """ This utilizes io.save_object and load_object to test the round trip 
        """
        d = {"foo": "bar", "fizz": 42}
        path = f"s3://{test_bucket_name}/load_object/dict.pkl"

        obj = pickle.dumps(d)

        with s3_fs.open(path, "wb") as f:
            f.write(obj)

        new_d = pickle.loads(s3.load_object(path, fs=s3_fs).read())

        assert d == new_d


class TestS3GetSize(object):
    def test_get_size_file(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo/bar.txt"
        assert s3.get_size(path, fs=s3_fs) == 21

    def test_get_size_dir(self, s3_fs):
        path = f"s3://{test_bucket_name}/foo"
        assert s3.get_size(path, fs=s3_fs) == 43