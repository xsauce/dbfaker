import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

def parse(yaml_file):
    with open(yaml_file) as f:
        return yaml.load(f.read(), Loader=Loader)