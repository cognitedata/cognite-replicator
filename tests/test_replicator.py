import toml

from cognite.replicator import __version__


def test_version_consistency():
    project_settings = toml.load("pyproject.toml")
    assert project_settings["tool"]["poetry"]["version"] == __version__
