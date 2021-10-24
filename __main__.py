from utils.utils import load_yaml
from load_data.load import load_data
import logging


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - [%(levelname)s] -> %(message)s',
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    config = load_yaml('config/config.yaml')
    data = load_data(config['data_source'])