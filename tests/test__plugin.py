def test__pl_local_snapshot_fixture__failed_snapshot(pytester):
    """Make sure that pytest accepts our fixture."""
    # create a temporary pytest conftest.py
    pytester.makeconftest(
        """
        import pytest
        from pathlib import Path

        @pytest.fixture
        def temp_snapshot_dir(tmp_path) -> Path:
            d = tmp_path / ".snapshots"
            d.mkdir()
            yield d
            # cleanup
            # empty the directory
            for child in d.iterdir():
                if child.is_file():
                    child.unlink()
            d.rmdir()

        @pytest.fixture
        def snapshot(monkeypatch, temp_snapshot_dir, local_snapshot):
            local_snapshot.storage.directory = temp_snapshot_dir
            return local_snapshot
        """
    )

    # create a temporary pytest test module
    pytester.makepyfile("""
        import polars as pl

        def test_sth(snapshot):
            df = pl.DataFrame({"a": [1, 2, 3]})
            snapshot(df)
    """)

    # run pytest with the following cmd args
    result = pytester.runpytest("-v")
    print(result.stdout)

    # make sure that we get a '0' exit code for the testsuite
    assert result.ret == 1


def test__pl_local_snapshot_fixture__new_snapshot(pytester):
    """Make sure that pytest accepts our fixture."""
    # create a temporary pytest conftest.py
    pytester.makeconftest(
        """
        import pytest
        from pathlib import Path

        @pytest.fixture
        def temp_snapshot_dir(tmp_path) -> Path:
            d = tmp_path / ".snapshots"
            d.mkdir()
            yield d
            # cleanup
            # empty the directory
            for child in d.iterdir():
                if child.is_file():
                    child.unlink()
            d.rmdir()

        @pytest.fixture
        def snapshot(monkeypatch, temp_snapshot_dir, local_snapshot):
            local_snapshot.storage.directory = temp_snapshot_dir
            return local_snapshot
        """
    )

    # create a temporary pytest test module
    pytester.makepyfile("""
        import polars as pl

        def test_sth(snapshot):
            df = pl.DataFrame({"a": [1, 2, 3]})
            snapshot(df)
    """)

    # run pytest with the following cmd args
    result = pytester.runpytest("-v", "--new-snapshot")
    print(result.stdout)

    # make sure that we get a '0' exit code for the testsuite
    assert result.ret == 0

def test_gcs_path_ini_setting(pytester):
    pytester.makeini("""
        [pytest]
        photoshoot-gcs-uri = gs://my-bucket/test-files
    """)

    pytester.makepyfile("""
        import pytest

        @pytest.fixture
        def hello(request):
            return request.config.getini('photoshoot-gcs-uri')

        def test_hello_world(hello):
            assert hello == 'gs://my-bucket/test-files'
    """)

    result = pytester.runpytest("-v")

    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(
        [
            "*::test_hello_world PASSED*",
        ]
    )

    # make sure that we get a '0' exit code for the testsuite
    assert result.ret == 0
