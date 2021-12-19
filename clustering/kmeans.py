import pandas as pd
import numpy as np
from sklearn import preprocessing, cluster
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import seaborn as sns


def plot_elbow(sse, ks):
    fig, axis = plt.subplots(figsize=(9, 6))
    axis.set_title('Elbow method for optimal k')
    axis.set_xlabel('k')
    axis.set_ylabel('SSE')
    plt.plot(ks, sse, marker='o')
    plt.tight_layout()
    plt.show()


def plot_silhouette(sils, ks):
    fig, axis = plt.subplots(figsize=(9, 6))
    axis.set_title('Silhouette method')
    axis.set_xlabel('k')
    axis.set_ylabel('Silhouette')
    plt.plot(ks, sils, marker='o')
    plt.tight_layout()
    plt.show()


def elbow_method(data):
    sse = []
    ks = range(2, 10)
    for k in ks:
        k_means_model = cluster.KMeans(n_clusters=k, random_state=55)
        k_means_model.fit(data)
        sse.append(k_means_model.inertia_)
    plot_elbow(sse, ks)


def silhouette_method(data):
    ks = range(2, 10)
    sils = []
    for k in ks:
        clusterer = KMeans(n_clusters=k, random_state=55)
        cluster_labels = clusterer.fit_predict(data)
        silhouette_avg = silhouette_score(data, cluster_labels)
        sils.append(silhouette_avg)
        print("For n_clusters =", k, "The average silhouette_score is :",
              silhouette_avg)
    plot_silhouette(sils, ks)


def apply_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    # 1 Filter rows from 2015 and 2018 to calculate differences
    dataset = df.copy()
    df = df[df['anyo'].isin([2015, 2018])]

    # set 2015 values as negative and sum to calculate differences
    kpis = ['num_incidentes', 'inmigracion_mil_hab',
            'tasa_natalidad_mil_habitantes', 'num_personas_por_domicilio',
            'precio_alquiler_mes_m2', 'precio_compra_venta_m2', 'renta']
    for kpi in kpis:
        df[kpi] = np.where(df['anyo'] == 2015, -1 * df[kpi], df[kpi])

    df = (
        df
        .groupby(['id_barrio', 'nom_barrio'])
        .agg(num_incidentes=('num_incidentes', sum),
             inmigracion_mil_hab=('inmigracion_mil_hab', sum),
             tasa_natalidad_mil_habitantes=(
                 'tasa_natalidad_mil_habitantes',
                 sum),
             num_personas_por_domicilio=(
                 'num_personas_por_domicilio', sum),
             precio_alquiler_mes_m2=('precio_alquiler_mes_m2', sum),
             precio_compra_venta_m2=('precio_compra_venta_m2', sum),
             renta=('renta', sum))
        .reset_index()
    )
    for kpi in kpis:
        df[kpi] = np.round(df[kpi], 2)

    # 2 Normalize
    df2 = df.copy()
    df2 = df2.drop(columns=['id_barrio', 'nom_barrio'])
    x = df2.values  # returns a numpy array
    scaler = preprocessing.StandardScaler()
    x_scaled = scaler.fit_transform(x)
    df2 = pd.DataFrame(x_scaled)

    # 3 Correlations
    corr_matrix = df2.corr()
    corr_matrix = np.round(corr_matrix, 2)
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr_matrix, annot=True, fmt="g", cmap='viridis', ax=ax,
                xticklabels=kpis, yticklabels=kpis)
    plt.tight_layout()
    fig.show()

    # Remove highly correlated variables
    df2 = df2.drop(columns=[1])

    # Optimal k
    silhouette_method(df2)
    elbow_method(df2)
    # elbow 4 - silhouette 2 -> 3 -> 4

    # kmeans with k=3
    clusterer = KMeans(n_clusters=3, random_state=55)
    cluster_labels = clusterer.fit_predict(df2)
    df['cluster_k3'] = cluster_labels
    k3 = df[['id_barrio', 'nom_barrio', 'cluster_k3']]
    dataset_k3 = pd.merge(dataset, k3, on=['id_barrio', 'nom_barrio'])
    dataset_k3 = (
        dataset_k3
        .groupby(['anyo', 'cluster_k3'])
        .agg(num_incidentes=('num_incidentes', 'mean'),
             inmigracion_mil_hab=('inmigracion_mil_hab', 'mean'),
             tasa_natalidad_mil_habitantes=(
                 'tasa_natalidad_mil_habitantes', 'mean'),
             num_personas_por_domicilio=(
                 'num_personas_por_domicilio', 'mean'),
             precio_alquiler_mes_m2=('precio_alquiler_mes_m2', 'mean'),
             precio_compra_venta_m2=('precio_compra_venta_m2', 'mean'),
             renta=('renta', 'mean'))
        .reset_index()
    )
    dataset_k3.to_csv('data/dataset/dataset_clusters_3.csv', index=False)

    # kmeans with k=4
    clusterer = KMeans(n_clusters=4, random_state=55)
    cluster_labels = clusterer.fit_predict(df2)
    df['cluster_k4'] = cluster_labels
    df['cluster_k4'] = df['cluster_k4'].replace({2: 0, 0: 2, 3: 1, 1: 3})
    df_cluster = df[['id_barrio', 'nom_barrio', 'cluster_k3', 'cluster_k4']]
    df_cluster.to_csv('data/dataset/kmeans_clusters.csv', index=False)

    k4 = df[['id_barrio', 'nom_barrio', 'cluster_k4']]
    dataset_k4 = pd.merge(dataset, k4, on=['id_barrio', 'nom_barrio'])
    dataset_k4 = (
        dataset_k4
        .groupby(['anyo', 'cluster_k4'])
        .agg(num_incidentes=('num_incidentes', 'mean'),
             inmigracion_mil_hab=('inmigracion_mil_hab', 'mean'),
             tasa_natalidad_mil_habitantes=(
                 'tasa_natalidad_mil_habitantes', 'mean'),
             num_personas_por_domicilio=(
                 'num_personas_por_domicilio', 'mean'),
             precio_alquiler_mes_m2=('precio_alquiler_mes_m2', 'mean'),
             precio_compra_venta_m2=('precio_compra_venta_m2', 'mean'),
             renta=('renta', 'mean'))
        .reset_index()
    )
    dataset_k4.to_csv('data/dataset/dataset_clusters_4.csv', index=False)
    return df_cluster

