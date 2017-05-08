import pytest


def pytest_addoption(parser):
    parser.addoption("--port", default=None, help="my option: type1 or type2")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")
