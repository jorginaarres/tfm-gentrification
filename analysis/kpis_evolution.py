import pandas as pd
import numpy as np


def kpi_evolution(df: pd.DataFrame):
    results = {}
    df_2015 = df[df['anyo'] == 2015]
    df_2022 = df[df['anyo'] == 2022]
    variables = df.columns[4:13]
    for var in variables:
        var_2015 = var + '_2015'
        var_2022 = var + '_2022'
        df_2015_var = df_2015[['nom_barrio', var]]
        df_2015_var = df_2015_var.rename(columns={var: var_2015})
        df_2022_var = df_2022[['nom_barrio', var]]
        df_2022_var = df_2022_var.rename(columns={var: var_2022})
        df_diff = pd.merge(df_2015_var, df_2022_var, on='nom_barrio',
                           how='inner')

        df_diff['diff'] = np.where(df_diff[var_2015] <= df_diff[var_2022],
                                   df_diff[var_2022] - df_diff[var_2015],
                                   df_diff[var_2022] - df_diff[var_2022])

        df_diff['diff_perc'] = np.round(100*(df_diff['diff'] / df_diff[
            var_2015]), 2)

        df_diff['evolution'] = np.where(
            df_diff[var_2015] <= df_diff[var_2022],
            '+' + df_diff['diff_perc'].astype(str) + '%',
            '-' + df_diff['diff_perc'].astype(str) + '%'
        )
        df_diff = df_diff.sort_values(by='diff_perc', ascending=False)

        if var == 'num_incidentes':
            df_diff = df_diff[df_diff[var_2015] > 1000]

        res = df_diff[['nom_barrio', 'evolution']]
        res = res.rename(columns={'nom_barrio': 'Barrio',
                                  'evolution': 'Evolución'})
        results[var] = res

    return results







