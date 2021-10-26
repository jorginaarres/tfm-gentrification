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

        if src_params.get('force_local', False):
            origin = 'local'

        encoding = src_params.get('encoding', 'utf-8')

        if src_params['format'] == 'csv':
            if type(src_params[origin]) == list:
                parts = []
                num_parts = len(src_params[origin])
                for i, part_path in enumerate(src_params[origin]):
                    logger.info(f'Loading part {i+1}/{num_parts}: {part_path}')
                    df = pd.read_csv(
                        part_path,
                        sep=src_params['sep'],
                        header=src_params['header'],
                        encoding=encoding
                    )
                    try:
                        df = df[src_params['select']]
                    except KeyError as e:
                        logger.warning(f'Could not select columns from '
                                       f'{df.columns}. Trying to fix it...')
                        df = df.iloc[:, src_params['part_col_names_fix']]
                    parts.append(df)

                logger.info('Unioning parts...')
                data[src] = pd.concat(parts)

            else:
                data[src] = pd.read_csv(
                    src_params[origin],
                    sep=src_params['sep'],
                    header=src_params['header'],
                    encoding=encoding
                )
                data[src] = data[src][src_params['select']]

            if 'column_alias' in src_params:
                data[src].columns = src_params['column_alias']
            if src_params['schema_mode'] == 'explicit':
                schema = src_params['schema']
                data[src] = data[src].astype(schema)

            logger.info(f'{src}\n{data[src].head(3)}')

    return data






