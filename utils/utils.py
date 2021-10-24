import yaml
import logging

logging.getLogger(__name__)


def load_yaml(path):
    with open(path) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        return config
