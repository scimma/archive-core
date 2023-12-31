"""
    Utility  functions 
"""
import argparse
import logging
import os
import toml
##################################                                                                                                          
#   environment                                                                                                                             
##################################                                                                                                          

def load_toml_config(file_path, config):
    """Load settings from file_path and merge into config"""
    print(f"Loading configuration from {file_path}")
    try:
        toml_data = toml.load(file_path)
        for key, value in toml_data.items():
            if '-' in key:
                key = key.replace('-','_')
            setattr(config, key, value)
    except:
        raise RuntimeError(f"Unable to read TOML config from {file_path}")

class LoadTomlConfig(argparse.Action):
    def __init__(self, **kwargs):
        if "default" in kwargs:
            del kwargs["default"]
        kwargs["required"] = False
        super().__init__(**kwargs)
    
    def __call__(self, parser, namespace, file_path, option_string=None):
        delattr(namespace, self.dest)
        load_toml_config(file_path, namespace)


class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if envvar in os.environ:
            envval = os.environ[envvar]
            if "choices" in kwargs and envval not in kwargs["choices"]:
                raise RuntimeError(f"Invalid choice '{envval}' from ${envvar}; "
                                   f"must be one of {kwargs['choices']}")
            else:
                if "nargs" in kwargs and (kwargs["nargs"] == '*' or kwargs["nargs"] == '+'):
                    default = envval.split()
                else:
                    default = envval
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required, 
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)

def add_parser_options(parser):
    parser.add_argument("-f", "--config-file", help="read configuration from a TOML file",
                        action=LoadTomlConfig)
    parser.add_argument("--log-level", help="Log display level", type=str, action=EnvDefault,
                        envvar="LOG_LEVEL", required=False, default="INFO")
    parser.add_argument("--log-format", help="Log message format", type=str, action=EnvDefault,
                        envvar="LOG_FORMAT", required=False,
                        default="%(asctime)s:%(filename)s:%(levelname)s:%(message)s")

def make_logging(config):
    """
    establish python logging
    """
    logging.basicConfig(level=config["log_level"], format=config["log_format"])
    logging.info(f"Basic logging is configured at {config['log_level']}")


def terse(object):
    "shorten the text used to represent an object"
    text = object.__repr__()
    max_length = 30
    if len(text) > max_length:
        return text[:max_length-3] + '...'
    return text
