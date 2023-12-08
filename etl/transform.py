import numpy as np
import pandas as pd
import logging
from utils.utils import count_nulls
import etl.processing_raw as processing_raw
import etl.processing_l1 as processing_l1
from io import StringIO
import geopandas as gpd

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


def transform_l1(sources: dict, min_year: int, max_year: int) -> dict:
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
            if process_function_name == 'process_lugares':
                src_l2[df_name] = func(dfs, min_year, max_year)
            else:
                src_l2[df_name] = func(dfs)
    return src_l2


def transform_dataset(sources: dict, min_anyo: int,
                      max_anyo: int) -> pd.DataFrame:
    dfs = sources['dataset']

    # nom_barrio will be taken from superficie
    dfs['ocupacion_media_piso'].drop(columns=['nom_barrio'], inplace=True)
    dfs['natalidad'].drop(columns=['nom_barrio'], inplace=True)
    dfs['lugares'].drop(columns=['nom_barrio'], inplace=True)
    dfs['inmigracion'].drop(columns=['nom_barrio'], inplace=True)

    merge_on = ['anyo', 'id_barrio']
    kpis = pd.merge(dfs['superficie'], dfs['incidentes'], on=['id_barrio'],
                    how='left')
    kpis = pd.merge(kpis, dfs['inmigracion'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['natalidad'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['ocupacion_media_piso'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['precio_alquiler'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['precio_compra_venta'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['renta'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['nivel_educativo'], on=merge_on, how='left')
    kpis = pd.merge(kpis, dfs['sexo_y_edad'], on=merge_on, how='left')
    kpis = kpis[kpis['id_barrio'] != 99]

    lugares = dfs['lugares']
    group = ['id_barrio', 'anyo', 'categoria_lugar']
    lugares = lugares.rename(columns={'num_locales': 'num_ubicaciones_'})
    lugares = lugares.groupby(group).size().reset_index(name='num_ubic_')
    lugares = lugares.set_index(group).unstack().reset_index()
    col_names = [c[0] + c[1] for c in lugares.columns]
    lugares.columns = col_names

    num_ubics_cols = [col for col in col_names if 'num_ubic' in col]
    lugares[num_ubics_cols] = lugares[num_ubics_cols].fillna(0)
    dataset = pd.merge(kpis, lugares, on=['anyo', 'id_barrio'], how='left')

    for col in num_ubics_cols:
        dataset[col] = dataset[col].astype('Int64', errors='ignore')

    dataset['inmigracion_mil_hab'] = np.round(
        dataset['inmigracion_mil_hab'], 2)
    dataset['tasa_natalidad_mil_habitantes'] = np.round(
        dataset['tasa_natalidad_mil_habitantes'], 2)

    dataset = dataset[(dataset['anyo'] >= min_anyo)
                      & (dataset['anyo'] <= max_anyo)]
    date_first_jan = '{}-01-01'
    dataset['anyo_date'] = dataset['anyo'].apply(lambda x:
                                                 date_first_jan.format(x))

    return dataset


def transform_geodata(dataset: pd.DataFrame):
    shapefile_path = './data/shapefiles/barris.geojson'
    gdf = gpd.read_file(shapefile_path, encoding='utf-8')
    gdf['BARRI'] = gdf['BARRI'].astype(np.int32)
    gdf_dataset = gdf.merge(dataset, left_on='BARRI', right_on='id_barrio',
                            how='left')
    return gdf_dataset


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


def transform_places_normalized(lugares: pd.DataFrame):
    group = ['id_barrio', 'nom_barrio', 'categoria_lugar']
    lugares = lugares.groupby(group).size().reset_index(name='num_ubic_')
    lugares = lugares.set_index(group).unstack().reset_index()
    col_names = [c[0] + c[1] for c in lugares.columns]
    lugares.columns = col_names

    num_ubics_cols = [col for col in col_names if 'num_ubic' in col]
    lugares[num_ubics_cols] = lugares[num_ubics_cols].fillna(0).astype(int)

    cols_ubic = [col for col in lugares.columns if col.startswith('num_ubic')]
    df = lugares

    df['total_barrio'] = df[cols_ubic].sum(axis=1)
    df['total_bcn'] = df['total_barrio'].sum()

    df['alojamiento'] = df['num_ubic_alojamiento'] / df['total_barrio']
    df['culto'] = df['num_ubic_culto'] / df['total_barrio']
    df['deporte'] = df['num_ubic_deporte'] / df['total_barrio']
    df['restaurantes'] = df['num_ubic_restaurantes'] / df['total_barrio']
    df['ocio_cultura'] = df['num_ubic_ocio_cultura'] / df['total_barrio']
    df['ropa_cc'] = df['num_ubic_ropa_cc'] / df['total_barrio']
    df['sanidad'] = df['num_ubic_sanidad'] / df['total_barrio']
    df['educacion'] = df['num_ubic_educacion'] / df['total_barrio']
    df['estet_peluq'] = (df['num_ubic_estetica_peluquerias']
                                  / df['total_barrio'])
    df['neg_trad'] = (df['num_ubic_negocios_tradicionales']
                                    / df['total_barrio'])
    df['alim_super'] = (
        df['num_ubic_minoristas_alimentacion_supermercado']
        / df['total_barrio'])

    df.loc['Total'] = df.sum()
    df.loc['Total', 'id_barrio'] = 99
    df.loc['Total', 'nom_barrio'] = 'Media'
    df.loc['Total', 'total_bcn'] = df.loc[0, 'total_bcn']

    df.loc['Total', 'alojamiento'] = (
            df.loc['Total', 'num_ubic_alojamiento']
            / df.loc['Total', 'total_bcn']
    )
    df.loc['Total', 'culto'] = (df.loc['Total', 'num_ubic_culto']
                                / df.loc['Total', 'total_bcn'])
    df.loc['Total', 'deporte'] = (df.loc['Total', 'num_ubic_deporte']
                                  / df.loc['Total', 'total_bcn'])
    df.loc['Total', 'restaurantes'] = (df.loc['Total', 'num_ubic_restaurantes']
                                       / df.loc['Total', 'total_bcn'])
    df.loc['Total', 'ocio_cultura'] = (df.loc['Total', 'num_ubic_ocio_cultura']
                                       / df.loc['Total', 'total_bcn'])
    df.loc['Total', 'ropa_cc'] = (df.loc['Total', 'num_ubic_ropa_cc']
                                  / df.loc['Total', 'total_bcn'])
    df.loc['Total', 'sanidad'] = (df.loc['Total', 'num_ubic_sanidad']
                                  / df.loc['Total', 'total_bcn'])
    df.loc['Total', 'educacion'] = (df.loc['Total', 'num_ubic_educacion']
                                    / df.loc['Total', 'total_bcn'])
    df.loc['Total', 'estet_peluq'] = (
            df.loc['Total', 'num_ubic_estetica_peluquerias']
            / df.loc['Total', 'total_bcn']
    )
    df.loc['Total', 'neg_trad'] = (
            df.loc['Total', 'num_ubic_negocios_tradicionales']
            / df.loc['Total', 'total_bcn']
    )
    df.loc['Total', 'alim_super'] = (
            df.loc['Total', 'num_ubic_minoristas_alimentacion_supermercado']
            / df.loc['Total', 'total_bcn']
    )

    new_cols = ['alojamiento', 'culto', 'deporte', 'restaurantes',
                'ocio_cultura', 'ropa_cc', 'sanidad', 'educacion',
                'estet_peluq',  'neg_trad', 'alim_super']

    df = df[['nom_barrio'] + new_cols]

    total = df[df.index == 'Total']
    df = df[df.index != 'Total']
    df['media'] = df['nom_barrio']
    total = pd.concat([total]*(len(df)-1), ignore_index=True)
    total['media'] = df['nom_barrio']
    df = pd.concat([df, total])
    return df
