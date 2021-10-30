import pandas as pd
import logging
from pathlib import Path
from utils.utils import count_nulls
import etl.processing as processing

logger = logging.getLogger(__name__)


def transform(sources: dict, config: dict) -> dict:
    src_l1 = {}
    logger.info('********** Getting information from dataframes **********')
    for df_name, df in sources.items():
        # get function name to execute
        if df_name in config['common_datasets_transformations']:
            process_function_name = 'process_generic_dataset'
        else:
            process_function_name = f'process_{df_name}'

        # execute function
        try:
            func = getattr(processing, process_function_name)
        except Exception:
            logger.warning(f'Function {process_function_name} not implemented')
        else:
            logger.info(f'Calling {process_function_name} for {df_name}')
            df_l1 = func(df, config)

            if type(df_l1) == pd.DataFrame:
                src_l1[df_name] = df_l1
            elif type(df_l1) == dict:
                src_l1 = {**src_l1, **df_l1}
    return src_l1


def save_to_csv_l1(dfs: dict, path: str):
    Path(path).mkdir(parents=True, exist_ok=True)
    for df_name, df in dfs.items():
        dataset_path = f'{path}{df_name}.csv'
        df.to_csv(dataset_path, index=False)

