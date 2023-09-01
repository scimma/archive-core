import argparse
import logging
import pytest

from archive import utility_api

from conftest import temp_environ

class Config:
	pass

config_toml = """
foo = "bar"
baz = 7
hyphenated-name = "stuff"
"""

def test_load_toml_config(tmpdir):
    config_path = tmpdir.join("config.toml")
    with open(config_path, 'w') as f:
        f.write(config_toml)
    config = Config()
    setattr(config, "previously_set", "a value")
    setattr(config, "baz", 6)
    utility_api.load_toml_config(config_path, config)
    assert config.foo == "bar", "Values should be added to the config object from the TOML file"
    assert config.baz == 7, "Old values in the config object should be over-written"
    assert config.hyphenated_name == "stuff", "Hyphens in setting names should be converted to underscores"
    assert config.previously_set == "a value", "Previously set values not mentioned in the TOML file should remain untouched"

def test_load_toml_config_bad(tmpdir):
    config_path = tmpdir.join("config.toml")
    with open(config_path, 'w') as f:
        f.write("***thisisnotvalidTOML***")
    with pytest.raises(RuntimeError) as err:
        config = Config()
        utility_api.load_toml_config(config_path, config)
    assert "Unable to read TOML config" in str(err)
    assert f"from {str(config_path)}" in str(err)

def test_LoadTomlConfig(tmpdir):
    config_path = tmpdir.join("config.toml")
    with open(config_path, 'w') as f:
        f.write(config_toml)
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--config-file", help="read configuration from a TOML file",
                        action=utility_api.LoadTomlConfig, default="bad")
    config = parser.parse_args(["-f", str(config_path)])
    assert config.foo == "bar", "Values should be added to the config object from the TOML file"
    assert config.baz == 7, "Old values in the config object should be over-written"
    assert config.hyphenated_name == "stuff", "Hyphens in setting names should be converted to underscores"

def test_EnvDefault():
    EnvDefault = utility_api.EnvDefault
    
    parser = argparse.ArgumentParser()
    with temp_environ(a="A", c="bar"):
        parser.add_argument("-a", default="a", action=EnvDefault, envvar="a")
        parser.add_argument("-b", default="b", action=EnvDefault, envvar="b")
        parser.add_argument("-c", choices=["foo","bar","baz"], action=EnvDefault, envvar="c")
        parser.add_argument("-d", default="d", action=EnvDefault, envvar="d")
        config = parser.parse_args(["-d","D"])
        assert config.a == "A"
        assert config.b == "b"
        assert config.c == "bar"
        assert config.d == "D"
    
    parser = argparse.ArgumentParser()
    with temp_environ(c="invalid"):
        with pytest.raises(RuntimeError) as err:
            parser.add_argument("-a", default="a", action=EnvDefault, envvar="a")
            parser.add_argument("-b", default="b", action=EnvDefault, envvar="b")
            parser.add_argument("-c", choices=["foo","bar","baz"], action=EnvDefault, envvar="c")
            config = parser.parse_args([])
        assert "Invalid choice 'invalid' from $c" in str(err)
        assert "must be one of ['foo', 'bar', 'baz']" in str(err)

def test_add_parser_options(tmpdir):
    config_path = tmpdir.join("config.toml")
    with open(config_path, 'w') as f:
        f.write(config_toml)
    parser = argparse.ArgumentParser()
    utility_api.add_parser_options(parser)
    config = parser.parse_args(["-f", str(config_path),
                                "--log-level", "WARN",
                                "--log-format", "%(message)s"])
    assert config.foo == "bar", "Values should be added to the config object from the TOML file"
    assert config.baz == 7, "Old values in the config object should be over-written"
    assert config.hyphenated_name == "stuff", "Hyphens in setting names should be converted to underscores"
    assert config.log_level == "WARN"
    assert config.log_format == "%(message)s"

    with temp_environ(LOG_LEVEL="DEBUG", LOG_FORMAT="%(message)s"):
        parser = argparse.ArgumentParser()
        utility_api.add_parser_options(parser)
        config = parser.parse_args([])
        assert config.log_level == "DEBUG"
        assert config.log_format == "%(message)s"

def test_make_logging(caplog):
    config = {
        "log_level": "WARN",
        "log_format": "%(filename)s:%(levelname)s: %(message)s",
    }
    with caplog.at_level(logging.DEBUG):
        utility_api.make_logging(config)
        print(caplog.text)
        assert "Basic logging is configured at WARN" in caplog.text
#         assert "utility_api.py:INFO: " in caplog.text

def test_terse():
    terse = utility_api.terse
    s1 = "A rather long and rambling string"
    t1 = terse(s1)
    assert len(t1) == 30
    assert t1 == "'A rather long and rambling..."
    
    s2 = "shorter string"
    assert terse(s2) == repr(s2)
