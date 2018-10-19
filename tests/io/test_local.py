import os
import pytest

import dna_util.io._local as local


@pytest.fixture(scope="session")
def sample_dir(tmpdir_factory):
    # Create nested directories
    foo_dir = tmpdir_factory.mktemp("foo", numbered=False)
    fizz_dir = tmpdir_factory.mktemp("foo/fizz", numbered=False)
    
    # Create sample files
    bar_txt = foo_dir.join("bar.txt")
    bar_txt.write("This is test file bar")
    buzz_txt = fizz_dir.join("buzz.txt")
    buzz_txt.write("This is test file buzz")

    # Return base path
    return tmpdir_factory.getbasetemp()


class TestAlreadyExists(object):

    def test_already_exists_file(self, sample_dir):
        path = os.path.join(sample_dir, "foo/bar.txt")
        assert local.already_exists(path)

    def test_already_exists_dir(self, sample_dir):
        path = os.path.join(sample_dir, "foo")
        assert local.already_exists(path)

    def test_already_exists_non_file(self, sample_dir):
        path = os.path.join(sample_dir, "foo/foobar.txt")
        assert not local.already_exists(path)

    def test_already_exists_non_dir(self, sample_dir):
        path = os.path.join(sample_dir, "foo/fizz/bar/")
        assert not local.already_exists(path)


class TestLocalLs(object):

    def test_ls(self, sample_dir):
        assert "foo/" in local.ls(sample_dir)
        foo_dir = os.path.join(sample_dir, "foo")
        assert local.ls(foo_dir) == ["bar.txt", "fizz/"]
    
    def test_ls_recursive(self, sample_dir):
        foo_dir = os.path.join(sample_dir, "foo")
        assert local.ls(foo_dir, recursive=True) == ["bar.txt", "fizz/buzz.txt"]
    
    def test_ls_full_path(self, sample_dir):
        foo_dir = os.path.join(sample_dir, "foo/")
        assert foo_dir in local.ls(sample_dir, full_path=True)

    def test_ls_full_path_recursive(self, sample_dir):
        path = os.path.join(sample_dir, "foo", "fizz", "buzz.txt")
        assert path in local.ls(sample_dir, full_path=True, recursive=True)


class TestLocalCp(object):

    def test_cp_file(self, sample_dir, tmpdir):
        from_path = os.path.join(sample_dir, "foo", "bar.txt")
        to_path = tmpdir.join("bar.txt")

        local.cp(from_path, to_path)
        assert to_path.read() == "This is test file bar"

    def test_cp_dir(self, sample_dir, tmpdir):
        from_path = os.path.join(sample_dir, "foo")
        local.cp(from_path, tmpdir)
        
        bar = tmpdir.join("foo/bar.txt")
        buzz = tmpdir.join("foo/fizz/buzz.txt")

        assert bar.read() == "This is test file bar"
        assert buzz.read() == "This is test file buzz"

    def test_cp_file_err(self, sample_dir, tmpdir):
        from_path = os.path.join(sample_dir, "foo", "bar.txt")
        bar = tmpdir.mkdir("foo").join("bar.txt")
        bar.write("Hello there")

        with pytest.raises(ValueError):
            local.cp(from_path, bar, overwrite=False)

    def test_cp_dir_error(self, sample_dir, tmpdir):
        from_path = os.path.join(sample_dir, "foo")

        foo = tmpdir.mkdir("foo")
        bar = foo.join("bar.txt")
        bar.write("Hello there")

        with pytest.raises(ValueError):
            local.cp(from_path, tmpdir, overwrite=False)


class TestGetSize(object):

    def test_get_size_file(self, sample_dir):
        fpath = os.path.join(sample_dir, "foo", "bar.txt")
        assert local.get_size(fpath) == 21

    def test_get_size_dir(self, sample_dir):
        fpath = os.path.join(sample_dir, "foo")
        assert local.get_size(fpath) == 43
