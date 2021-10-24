from utils.utils import load_yaml
import pandas as pd
import logging


logger = logging.getLogger(__name__)


def load_data(origin):
    config_paths = load_yaml('config/config_paths.yaml')
    data = {}
    for src, src_params in config_paths['data_sources'].items():
        logger.info(f'Loading {src} with params {src_params}')
        if src_params['format'] == 'csv':
            schema = None
            if src_params['schema_mode'] == 'explicit':
                schema = src_params['schema']

            data[src] = pd.read_csv(
                src_params[origin],
                sep=src_params['sep'],
                header=src_params['header'],
                names=src_params['column_alias'],
                dtype=schema
            )
            logger.info(f'{src}\n{data[src]}')

    return data






