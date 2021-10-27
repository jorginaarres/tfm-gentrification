import yaml
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def load_yaml(path):
    with open(path) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        return config


def replace_values_with_null(df: pd.DataFrame, values_to_replace: list):
    pass


def clean_strings(df: pd.DataFrame):
    pass


def count_nulls(df_name: str, df: pd.DataFrame):
    counts = df.isna().sum()
    logger.info(f"NA's in {df_name}:\n{counts}")
