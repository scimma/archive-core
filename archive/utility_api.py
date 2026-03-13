"""
    Utility  functions 
"""
import argparse
import boto3
import hop
import json
import logging
import os
import re
import toml
from urllib.parse import urlparse
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
    add_hopauth_parser_options(parser)

def add_hopauth_parser_options(parser):
    parser.add_argument("--hopauth-api-url", help="Base URL for the hopauth API",
                        type=str, action=EnvDefault, envvar="HOPAUTH_API_URL", required=False)
    parser.add_argument("--hopauth-username",
                        help="Username of the credential to use for the hopauth API", type=str,
                        action=EnvDefault, envvar="HOPAUTH_USERNAME", required=False)
    parser.add_argument("--hopauth-local-auth",
                        help="Path to a local hop auth TOML file from which to read credentials "
                        "for the hopauth API", type=str, action=EnvDefault,
                        envvar="HOPAUTH_LOCAL_AUTH", required=False)
    parser.add_argument("--hopauth-aws-secret-name",
                        help="Name of an AWS secret from which to read hopauth API credentials",
                        type=str, action=EnvDefault, envvar="HOPAUTH_AWS_SECRET_NAME",
                        required=False)
    parser.add_argument("--hopauth-aws-secret-region",
                        help="Name of the AWS region in which to look for the AWS secret from which"
                        " to read hopauth API credentials", type=str, action=EnvDefault, 
                        envvar="HOPAUTH_AWS_SECRET_REGION", default="us-west-2", required=False)

def get_hopskotch_credential(config):
	if "hopauth_username" in config and config["hopauth_username"] \
	  and "HOPAUTH_PASSWORD" in os.environ:
		return hop.auth.Auth(config["hopauth_username"], os.environ["HOPAUTH_PASSWORD"])
	elif "hopauth_local_auth" in config and config["hopauth_local_auth"]:
		auth = hop.auth.load_auth(config["hopauth_local_auth"])
		return hop.auth.select_matching_auth(auth, urlparse(config["hopauth_api_url"]).netloc,
		                                     config.get("hopauth_username", None))
	elif "hopauth_aws_secret_name" in config and config["hopauth_aws_secret_name"] \
		 and "hopauth_aws_secret_region" in config and config["hopauth_aws_secret_region"]:
		return auth_from_secret(config["hopauth_aws_secret_region"],
		                        config["hopauth_aws_secret_name"],
		                        config.get("hopauth_username", None))
	else:
		raise RuntimeError("Hopauth API credentials not configured")

def auth_from_secret(secret_region: str, secret_name: str, username: str=""):
	session = boto3.session.Session()
	client = session.client(
		service_name='secretsmanager',
		region_name=secret_region
	)
	resp = client.get_secret_value(
		SecretId=secret_name
	)['SecretString']
	try:
		data = json.loads(resp)
		return hop.auth.Auth(data["username"], data["password"])
	except json.JSONDecodeError:
		pass
	
	# if the data is not JSON try parsing it as a text config file
	chunks = resp.split(' ')
	creds = {}
	last_user=None
	for chunk in chunks:
		if match:=re.fullmatch('username="([^"]+)"', chunk):
			last_user=match[1]
		elif match:=re.fullmatch('password="([^"]+)"', chunk):
			if last_user is not None:
				creds[last_user]=match[1]
			last_user=None
		elif match:=re.fullmatch('user_([^=]+)="([^"]+)"', chunk):
			creds[match[1]]=match[2]
			last_user=None
		else:
			raise RuntimeError("Text credential list parse error")
	if username and username in creds:
		return hop.auth.Auth(username, creds[username], method=hop.auth.SASLMethod.PLAIN)
	elif len(creds) == 1:
		username = next(iter(creds))
		return hop.auth.Auth(username, creds[username], method=hop.auth.SASLMethod.PLAIN)
	else:
		raise RuntimeError(f"Ambiguous text-format credential from secret {secret_name}")

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
