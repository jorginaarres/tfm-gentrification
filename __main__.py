from utils.utils import load_yaml
from etl.extract import load_data
from etl.transform import transform, save_to_csv_l1
import logging


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - [%(levelname)s] -> %(message)s',
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    config = load_yaml('config/config.yaml')

    # 1. Load data from sources
    if 'raw' in config['steps']:
        data_raw = load_data(config['data_source'], 'raw')

        # 2. get basic info for each dataframe (nulls, size, etc.),
        # clean (remove blank spaces, NA's, weird values...) and transform
        # (filters, aggregates, etc.)
        data_l1 = transform(data_raw, config)
        save_to_csv_l1(data_l1, config['l1_save_path'])



