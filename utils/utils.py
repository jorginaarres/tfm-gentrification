import yaml
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def load_yaml(path):
    with open(path, encoding='utf8') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        return config


def count_nulls(df_name: str, df: pd.DataFrame):
    counts = df.isna().sum()
    logger.info(f"NA's in {df_name}:\n{counts}\n\n")


def save_dfs_to_csv(dfs: dict, path: str):
    Path(path).mkdir(parents=True, exist_ok=True)
    for df_name, df in dfs.items():
        dataset_path = f'{path}{df_name}.csv'
        df.to_csv(dataset_path, index=False, encoding='utf-8')

