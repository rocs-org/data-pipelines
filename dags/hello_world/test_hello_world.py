from .hello_world import extract, transform


def test_extract():
    assert extract() == "hello_world"


def test_transform():
    assert transform("lower") == "LOWER"
