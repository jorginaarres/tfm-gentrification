from utils.utils import load_yaml
from etl.extract import load_data
from utils.utils import save_dfs_to_csv
from etl.transform import (transform_raw, transform_l1, clean_data,
                           transform_dataset)
import logging


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - [%(levelname)s] -> %(message)s',
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    config = load_yaml('config/config.yaml')

    # 1. Load data from sources
    if 'l1' in config['steps']:
        # 1.1 Load data
        data_raw = load_data(config['data_source'], 'raw')
        # 1.2 Transform input datasets filtering necessary rows and renaming
        # columns. Discretize some column values.
        data_l1 = transform_raw(data_raw, config)
        # 1.3 Clean data: Trim strings and detect format issues..
        data_l1_clean = clean_data(data_l1)
        # 1.4 Print dataset info: size, columns, summary, NAs
        save_dfs_to_csv(data_l1_clean, config['l1_save_path'])

    # 2. Dataset of all places + others with KPIs
    if 'l2' in config['steps']:
        data_l1 = load_data(layer='l1')
        data_l2 = transform_l1(data_l1, config['min_year'], config['max_year'])
        save_dfs_to_csv(data_l2, config['l2_save_path'])

    # 3. Generate a dataset with a row for each neighborhood - year:
    if 'l3' in config['steps']:
        data_l2 = load_data(layer='l2')
        dataset = transform_dataset(
            data_l2,  config['min_year'], config['max_year'])
        save_dfs_to_csv({'dataset': dataset}, config['dataset'])
    # aggregate number of places of each type and add KPIs



