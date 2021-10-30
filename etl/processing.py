import pandas as pd
import logging
from datetime import datetime
from utils.utils import count_nulls


logger = logging.getLogger(__name__)


def years_array_from_created_modified_ts(created: str, modified: str,
                                         min_year: int) -> list:
    created_year = datetime.fromisoformat(created).year
    modified_year = datetime.fromisoformat(modified).year
    years = list(range(created_year, modified_year))
    years = [year for year in years if year >= min_year]
    return years


def explode_years(df: pd.DataFrame, min_year: int) -> pd.DataFrame:
    df['anyo'] = df.apply(lambda row: years_array_from_created_modified_ts(
        row['created'], row['modified'], min_year), axis=1)
    df = df.explode('anyo')
    df = df.drop(columns=['created', 'modified'])
    return df


def process_renta(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    # Since for each neighborhood it is divided by "seccion_censal", we group
    # by year and neighborhood
    group = ['anyo', 'cod_barrio', 'nom_barrio']
    df = df.groupby(group).agg(importe_eur_anyo=('importe_eur_anyo', 'mean'))
    df = df.reset_index(drop=True)
    return df


def process_generic_dataset(df: pd.DataFrame,
                            config: dict = None) -> pd.DataFrame:
    # Regarding the year, for each record we only have the timestamp of
    # creation. We need a row for each year from creation to last
    # modification year.
    # Also, the same place can have multiple rows if it has multiple
    # classifications, so we need to remove duplicates (to have as much one
    # local in one year)
    df = explode_years(df, config['min_year'])
    df = df.drop_duplicates(subset=['anyo', 'id'], keep='first')
    df = df.drop(columns=['id', 'tipo_local'])
    df = df.reset_index(drop=True)
    return df
