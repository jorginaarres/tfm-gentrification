from utils.utils import load_yaml
import pandas as pd
import logging


logger = logging.getLogger(__name__)
pd.set_option('display.max_columns', 10)


def load_data(origin, layer='raw'):
    config_paths = load_yaml('config/config_paths.yaml')
    data = {}
    for src, src_params in config_paths['data_sources'][layer].items():
        logger.info(f'Loading {src} with params {src_params}')
        if src_params['format'] == 'csv':
            schema = None
            if src_params['schema_mode'] == 'explicit':
                schema = src_params['schema']

            if type(src_params[origin] == list):
                logger.info('Unioning parts...')
                parts = []

                for part_path in src_params[origin]:
                    df = pd.read_csv(
                        part_path,
                        sep=src_params['sep'],
                        header=src_params['header'],
                        dtype=schema
                    )
                    parts.append(df)
                data[src] = pd.concat(parts)

            else:
                data[src] = pd.read_csv(
                    src_params[origin],
                    sep=src_params['sep'],
                    header=src_params['header'],
                    dtype=schema
                )
            data[src] = data[src][src_params['select']]
            if 'column_alias' in src_params:
                data[src].columns = src_params['column_alias']
            logger.info(f'{src}\n{data[src]}')

    return data






