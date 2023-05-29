from python_module.mock_file import func


def test_python_module():
    assert func() == ["a", "b", "c"]
