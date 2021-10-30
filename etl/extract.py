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
            # ################# multiple parts #################
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
                        df.columns = [c.lower() for c in df.columns]
                        df = df[[c.lower() for c in src_params['select']]]
                    except KeyError as e:
                        logger.warning(f'Could not select columns from '
                                       f'{part_path} Trying to fix it...')
                        col_indexes = src_params['part_col_names_fix']
                        ordered_cols = [df.columns[i] for i in col_indexes]
                        df = df.iloc[:, col_indexes]
                        df = df[ordered_cols]

                    if 'column_alias' in src_params:
                        df.columns = src_params['column_alias']
                    parts.append(df)

                logger.info(f'Unioning {len(parts)} parts...')
                data[src] = pd.concat(parts)
                logger.info('All parts united.')

            # ################# only 1 file #################
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

            # change schema if requested
            if src_params['schema_mode'] == 'explicit':
                schema = src_params['schema']
                data[src] = data[src].astype(schema, errors='ignore')

            filters = src_params.get('filters', {})
            for filt, filter_values in filters.items():
                data[src] = data[src][data[src][filt].isin(filter_values)]
            logger.info(f'{src}\n{data[src].head(3)}')

    return data






