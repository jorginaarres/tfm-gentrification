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
        # 1.1 Load data
        data_raw = load_data(config['data_source'], 'raw')
        # 1.2 Transform input datasets filtering necessary rows and renaming
        # columns. Discretize some column values.
        data_l1 = transform(data_raw, config)
        # 1.3 Clean data: Trim strings and detect format issues..
        pass
        # 1.4 Print dataset info: size, columns, summary, NAs
        save_to_csv_l1(data_l1, config['l1_save_path'])



