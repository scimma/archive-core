import toml
tomldata=toml.load('pyproject.toml')
with open('/tmp/requirements.txt', 'w') as requirements_file:
    requirements_file.write('\n'.join(tomldata['project']['dependencies']))
