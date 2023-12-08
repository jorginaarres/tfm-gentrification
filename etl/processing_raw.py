import pandas as pd
import logging
from datetime import datetime
import numpy as np
from utils.utils import count_nulls

logger = logging.getLogger(__name__)


def years_array_from_created_modified_ts(created: str, modified: str,
                                         min_year: int) -> list:
    created_year = datetime.fromisoformat(created).year
    modified_year = datetime.fromisoformat(modified).year
    years = list(range(created_year, modified_year + 1))
    years = [int(year) for year in years if year >= min_year]
    return years


def explode_years(df: pd.DataFrame, min_year: int) -> pd.DataFrame:
    df['anyo'] = df.apply(lambda row: years_array_from_created_modified_ts(
        row['created'], row['modified'], min_year), axis=1)
    df = df.explode('anyo')
    df['anyo'] = df['anyo'].astype(str).astype(int, errors='ignore')
    df = df.drop(columns=['created', 'modified'])
    return df


def process_renta(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    # Since for each neighborhood it is divided by "seccion_censal", we group
    # by year and neighborhood
    group = ['anyo', 'id_barrio']
    df = df.groupby(group).agg(importe_eur_anyo=('importe_eur_anyo', 'mean'))
    df['importe_eur_anyo'] = np.round(df['importe_eur_anyo'], 2)
    df = df.reset_index()
    return df


def process_sexo_y_edad(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    df.loc[df['data_referencia'].notnull(), 'anyo'] = df['data_referencia'].str[:4].astype(int, errors='ignore')
    df.loc[df['num_personas'] == "..", "num_personas"] = "0"
    df['num_personas'] = pd.to_numeric(df['num_personas'])

    df.drop(columns=['data_referencia', 'sexo', 'aeb', 'cod_dist', 'nom_dist'], inplace=True)

    df.groupby(["id_barrio", "anyo", "edad"], as_index=False)["num_personas"].sum()
    df["numpersonas_x_edad"] = df["edad"] * df["num_personas"]
    df.groupby(["anyo", "id_barrio", "edad"], as_index=False)["numpersonas_x_edad"].sum()

    df['total_personas_barrio_anyo'] = df.groupby(["anyo", "id_barrio"], as_index=False)['num_personas'].transform(sum)
    df['total_edad_barrio_anyo'] = df.groupby(["anyo", "id_barrio"], as_index=False)['numpersonas_x_edad'].transform(
        sum)
    df['media_edad_barrio_anyo'] = pd.to_numeric(df['total_edad_barrio_anyo'] / df['total_personas_barrio_anyo']).round(
        2)

    df.drop(
        columns=['num_personas', 'edad', 'numpersonas_x_edad', 'total_edad_barrio_anyo', 'total_personas_barrio_anyo'],
        inplace=True)
    df = df[~(df.duplicated(subset=['anyo', 'id_barrio'], keep='first'))].copy()
    return df


def process_nivel_educativo(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    df.loc[df['data_referencia'].notnull(), 'anyo'] = df['data_referencia'].str[:4].astype(int, errors='ignore')
    df.drop(columns=['data_referencia'], inplace=True)
    return df


def process_precio_alquiler(df: pd.DataFrame, config: dict = None
                            ) -> pd.DataFrame:
    # since we have data divided by terms, we group by year. In addition,
    # "tipo_valor" column tells if the value of "precio_mes" is the monthly
    # absolute or the monthly by m^2. We will create one column for each value
    group = ['anyo', 'id_barrio', 'tipo_valor']
    df = df.groupby(group).agg(precio_mes=('precio_mes', 'mean')).reset_index()
    mean_price = df[df['tipo_valor'] == 'Lloguer mitjà mensual (Euros/mes)']
    mean_price_m2 = df[df['tipo_valor'] ==
                       'Lloguer mitjà per superfície (Euros/m2 mes)']

    mean_price_m2['precio_mes_m2'] = mean_price_m2['precio_mes']
    mean_price = mean_price.drop(columns=['tipo_valor'])
    mean_price_m2 = mean_price_m2.drop(columns=['precio_mes', 'tipo_valor'])
    df = pd.merge(mean_price, mean_price_m2, on=['anyo', 'id_barrio'],
                  how='inner')
    df['precio_mes_m2'] = np.round(df['precio_mes_m2'], 2)
    df['precio_mes'] = np.round(df['precio_mes'], 2)
    return df


def process_precio_compra_venta(df: pd.DataFrame, config: dict = None
                                ) -> pd.DataFrame:
    # filter only by "tipo_valor" == Total. Milers d'euros and group by year
    # and neighorhood to avoid dataset parts that are divided by term
    df = df[df['tipo_valor'] == "Total. Euros/m2 construït"]
    # remove "--" as euro value and convert it to numeric
    df.loc[df['euros'] == "--", "euros"] = ""
    df['euros'] = pd.to_numeric(df['euros'])
    group = ['anyo', 'id_barrio']
    df = df.groupby(group).agg(precio_compra_venta_m2=('euros',
                                                       'mean')).reset_index()
    return df


def process_antiguedad_vehiculos(df: pd.DataFrame, config: dict = None
                                 ) -> pd.DataFrame:
    # since the age of the cars is a string but representing an integer,
    # we cast it to intervals and add a column indicating the percentage of
    # cars in each interval of the total in the neighborhood
    int_0_5 = [f'{anyos} anys' for anyos in range(2, 6)]
    int_0_5 = int_0_5 + ["Menys d'un any d'antiguitat", "1 any"]
    df.loc[df['antiguedad'].isin(int_0_5), 'int_antiguedad'] = '0005_anyos'

    int_6_10 = [f'{anyos} anys' for anyos in range(6, 10)]
    df.loc[df['antiguedad'].isin(int_6_10), 'int_antiguedad'] = '0611_anyos'

    df.loc[df['antiguedad'] == "D'11 a 20 anys", 'int_antiguedad'] = '1120_anyos'
    df.loc[df['antiguedad'] == "Més de 20 anys", 'int_antiguedad'] = '20_anyos'

    group = ['anyo', 'id_barrio', 'int_antiguedad']
    df = df.groupby(group).agg(num_turismos=('num_turismos',
                                             'mean')).reset_index()

    group = ['anyo', 'id_barrio']
    df['porc_total_barrio'] = np.round(df['num_turismos'] / df.groupby(group)[
        'num_turismos'].transform('sum'), 2)
    return df


def process_incidentes(df: pd.DataFrame, config: dict = None
                       ) -> pd.DataFrame:
    # filter only by "tipo_valor" == Total. Milers d'euros and group by year
    # and neighorhood to avoid dataset parts that are divided by term
    group = ['anyo', 'id_barrio', 'cod_incidente', 'nom_incidente']
    df = df.groupby(group).agg(num_incidentes=('num_incidentes',
                                               'sum')).reset_index()
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
    df = df.drop(columns=['id'])
    df = df.reset_index(drop=True)
    return df


def process_censo_negocios_2019(df: pd.DataFrame,
                                config: dict = None) -> pd.DataFrame:
    df = df.drop_duplicates()
    return df


def skip_processing(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    return df
