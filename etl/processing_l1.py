import pandas as pd
import logging
from datetime import datetime
import numpy as np


def process_lugares(dfs: dict) -> pd.DataFrame:
    df = pd.concat(dfs.values())
    df['anyo'] = df['anyo'].astype(int, errors='ignore')
    return df


def process_antiguedad_vehiculos(dfs: dict) -> pd.DataFrame:
    df = dfs['antiguedad_vehiculos']
    df = df.rename(columns={'porc_total_barrio': 'vehic_antig_'})
    df = df.drop(columns='num_turismos')
    df = df.set_index(['anyo', 'id_barrio', 'nom_barrio', 'int_antiguedad'])
    df = df.unstack()
    df = df.reset_index()
    col_names = [c[0] + c[1] for c in df.columns]
    df.columns = col_names
    return df


def process_incidentes(dfs: dict) -> pd.DataFrame:
    df = dfs['incidentes']
    incidentes = [
        'CONVIVÈNCIA VEINAL', "ATEMPTATS", "BARALLES", "VANDALISME",
        "ACTIVITATS MOLESTES EN ESPAIS PÚBLICS", "AGRESSIONS"
        "ALARMES D'INCENDI / ROBATORI", "OCUPACIONS IL·LÍCITES",
        "DELICTES CONTRA L'ORDRE PÚBLIC", "CONFLICTES EN LOCALS",
        "ACTES CONTRA LA PROPIETAT PRIVADA",  "VIOLÈNCIA DOMÈSTICA",
        "ESTUPEFAENTS / PSICOTROPICS", "CONTRA LA LLIBERTAT SEXUAL"
    ]
    df = df[df['nom_incidente'].isin(incidentes)]
    df = df[df['id_barrio'].notnull()]
    df = df.groupby(['anyo', 'id_barrio', 'nom_barrio']) \
        .agg(num_incidentes=('num_incidentes', sum)) \
        .reset_index()
    df['anyo'] = df['anyo'].astype(int)
    df['num_incidentes'] = df['num_incidentes'].astype(int)
    return df


def process_inmigracion(dfs: dict) -> pd.DataFrame:
    df = dfs['inmigracion']
    df = df.rename(columns={'tasa_mil_habitantes': 'inmigracion_mil_hab'})
    return df


def process_precio_alquiler(dfs: dict) -> pd.DataFrame:
    df = dfs['precio_alquiler']
    df = df.drop(columns='precio_mes')
    df = df.rename(columns={'precio_mes_m2': 'precio_alquiler_mes_m2'})
    return df


def process_renta(dfs: dict) -> pd.DataFrame:
    df = dfs['renta']
    df = df.rename(columns={'importe_eur_anyo': 'renta'})
    return df
