import numpy as np
import pandas as pd
import logging
from utils.utils import count_nulls
import etl.processing_raw as processing_raw
import etl.processing_l1 as processing_l1
from io import StringIO

logger = logging.getLogger(__name__)


def transform_raw(sources: dict, config: dict) -> dict:
    src_l1 = {}
    logger.info('********** Getting information from dataframes **********')
    for df_name, df in sources.items():
        # get function name to execute
        if df_name in config['common_datasets_transformations']:
            process_function_name = 'process_generic_dataset'
        elif df_name in config['skip_processing_raw']:
            process_function_name = 'skip_processing'
        else:
            process_function_name = f'process_{df_name}'

        # execute function
        try:
            func = getattr(processing_raw, process_function_name)
        except Exception:
            logger.warning(f'Function {process_function_name} not implemented')
        else:
            logger.info(f'Calling {process_function_name} for {df_name}')
            config['current_df'] = df_name
            df_l1 = func(df, config)

            if type(df_l1) == pd.DataFrame:
                src_l1[df_name] = df_l1
            elif type(df_l1) == dict:
                src_l1 = {**src_l1, **df_l1}
    return src_l1


def transform_l1(sources: dict) -> dict:
    src_l2 = {}
    for df_name, dfs in sources.items():
        process_function_name = f'process_{df_name}'
        # execute function
        try:
            func = getattr(processing_l1, process_function_name)
        except Exception:
            logger.warning(f'Function {process_function_name} not '
                           f'implemented. Returning df without transforming')
            src_l2[df_name] = dfs[df_name]

        else:
            logger.info(f'Calling {process_function_name} for {df_name}')
            src_l2[df_name] = func(dfs)
    return src_l2


def transform_dataset(sources: dict) -> pd.DataFrame:
    dfs = sources['dataset']
    merge_on = ['anyo', 'id_barrio', 'nom_barrio']
    kpis = pd.merge(dfs['antiguedad_vehiculos'], dfs['incidentes'],
                    on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['inmigracion'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['natalidad'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['ocupacion_media_piso'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['precio_alquiler'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['precio_compra_venta'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['renta'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['superficie'], on=['id_barrio', 'nom_barrio'],
                    how='left')
    kpis = kpis[kpis['nom_barrio'] != 'No consta']

    lugares = dfs['lugares']
    group = ['id_barrio', 'nom_barrio', 'anyo', 'categoria_lugar']
    lugares = lugares.rename(columns={'num_locales': 'num_ubicaciones_'})
    lugares = lugares.groupby(group).size().reset_index(name='num_ubic_')
    lugares['num_ubic_'] = lugares['num_ubic_'].astype(int)
    lugares = lugares.set_index(group).unstack().reset_index()
    col_names = [c[0] + c[1] for c in lugares.columns]
    lugares.columns = col_names

    dataset = pd.merge(kpis, lugares, on=['anyo', 'id_barrio'], how='left')
    return dataset


def clean_data(sources: dict):
    for df_name, df in sources.items():
        # trim spaces
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        # replace some values to NA
        nan_values = ['Desconegut', -1, '']
        df = df.replace(nan_values, np.nan)

        # print info + null count
        buf = StringIO()
        df.info(buf=buf)
        logger.info(f'Dataframe info for {df_name}: {buf.getvalue()}')
        count_nulls(df_name, df)
        sources[df_name] = df

    return sources


