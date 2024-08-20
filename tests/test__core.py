from pathlib import Path

import polars as pl
import pytest

from photoshoot.core import PhotoshootTest, SnapshotTestFailedError


class FakeComp:
    def __init__(self,):
        pass
    def compare(self, a, b):
        pass


class FakeCompFailed:
    def __init__(self,):
        pass
    def compare(self, a, b):
        raise AssertionError


class FakeStorage:
    name = None
    def __init__(self,):
        pass

    def read(self, file_name: str) -> str:
        return "a"

    def write(self, data: str, file_name: str):
        self.name = file_name


class FakeStorageFailed:
    def __init__(self,):
        pass

    def read(self, file_name: str) -> str:
        raise FileNotFoundError

    def write(self, data: str, file_name: str):
        pass


def test__PhotoshootTest__snapshot_update():
    storage = FakeStorage()
    comp = FakeComp()
    test = PhotoshootTest("test", storage, comp, update_snapshot=True)
    test("data", "a")
    assert test.storage.name == "test/a"


def test__PhotoshootTest__snapshot():
    storage = FakeStorageFailed()
    comp = FakeComp()
    test = PhotoshootTest("test", storage, comp, update_snapshot=False)
    with pytest.raises(SnapshotTestFailedError):
        test("data", "a")


def test__PhotoshootTest__compare_failed():
    storage = FakeStorage()
    comp = FakeCompFailed()
    test = PhotoshootTest("test", storage, comp, update_snapshot=False)
    with pytest.raises(SnapshotTestFailedError):
        test("data", "a")
