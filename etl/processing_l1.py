import pandas as pd


def process_lugares(dfs: dict, min_year, max_year) -> pd.DataFrame:
    df = pd.concat(dfs.values())
    categories = {
        'ocio': [
            'Bars i pubs musicals', 'Salons de ball', 'Discoteques',
            'Tablaos flamencs', 'Karaokes', 'Sales de festes', 'Zoo',
            'Tibidabo', 'Bars i pubs musicals'
        ],
        'gastronomia': ['Cocteleries', 'Restaurants'],
        'cultura': [
            'Museus', 'Biblioteques', "Sales d'estudi", 'Teatres', 'Cinemes',
            'Biblioteques municipals', 'Auditoris'
        ],
        'sanidad': [
            'Hospitals i clíniques', 'CAPs', 'Centres urgències (CUAPs)'
        ],
        'hoteles': [
            'Hotels 4 estr.', 'Hotels 3 estr.', 'Hotels 5 estr.',
            'Hotels 2 estr.', 'Hotels 1 estr.'
        ],
        'deporte': [
            'Altres esports', 'Bàsquet', 'Tennis', 'Badminton', 'Futbol sala',
            'Frontennis', 'Futbol'
        ],
        'lugares_culto': [
            "Capelles de l'Església de Jesucrist dels S.D.D.", 'Mesquites',
            'Esglésies catòliques', 'Esglésies evangèliques', 'Sinagogues',
            'Centres budistes', 'Salons del regne', 'Centres taoistes',
            'Esglésies adventistes', "Centres Baha'is", 'Comunitats ortodoxes',
            'Temples sikhs', 'Centres hindús'
        ],
        'parques_jardines': ['Parcs i jardins'],
        'roba_amiga': ['Punts de roba amiga'],
        'centros_comerciales': ['Grans centres comercials']
    }

    df['categoria_lugar'] = df['tipo_lugar']
    for category, mappings in categories.items():
        df['categoria_lugar'].replace(mappings, category, inplace=True)

    df = df[~df['categoria_lugar'].isin(['Arxius municipals', 'Altres',
                                         "Interiors d'illa"])]
    df = df[df['anyo'].notnull()]
    df = df[df['id_barrio'].notnull()]
    df['id_barrio'] = df['id_barrio'].astype(int, errors='ignore')
    df['anyo'] = df['anyo'].astype(int, errors='ignore')
    df = df[(df['anyo'] >= min_year) & (df['anyo'] <= max_year)]
    date_first_jan = '{}-01-01'
    df['anyo_date'] = df['anyo'].apply(lambda x: date_first_jan.format(x))
    df = df.rename(columns={'geo_epgs_4326_x': 'lat',
                            'geo_epgs_4326_y': 'lon'})
    df = df[(41.0 < df['lat']) & (df['lat'] < 42.0)
            & (2.0 < df['lon']) & (df['lon'] < 2.5)]
    return df


def process_censo_negocios_2022(dfs: dict) -> pd.DataFrame:
    df = dfs['censo_negocios_2022']
    categories = {
        'minoristas_alimentacion_supermercado': [
            'Carn i Porc', 'Peix i marisc', 'Ous i aus',
            'Pa, pastisseria i làctics', 'Autoservei / Supermercat',
            'Fruites i verdures', 'Plats preparats (no degustació)',
            'Herbolaris, dietètica i NUTRICIÓ'
        ],
        'sanidad': [
            'Farmàcies PARAFARMÀCIA', 'Sanitat i assistència',
            'Veterinaris / Mascotes'
        ],
        'negocios_tradicionales': [
            'Calçat i pell', 'Merceria', 'Joieria, rellotgeria i bijuteria',
            'Tintoreries', 'Parament ferreteria', 'Vehicles',
            'Llibres, diaris i revistes',
            'Reparacions (Electrodomèstics i automòbils)',
            'Segells, monedes i antiguitats', 'Floristeries',
            'Mobles i articles fusta i metall', 'Fabricació tèxtil',
            'Souvenirs i basars',   'Locutoris',  'Souvenirs',  'Basars',
            'Informàtica', 'Òptiques i fotografia'
        ],
        'estetica_peluquerias': [
            "Centres d'estètica", 'Drogueria i perfumeria', 'Perruqueries'
        ],
        'ropa_cc': ['Grans magatzems i hipermercats', 'Vestir'],
        'ocio_cultura': [
            'Música',
            'Bars especials amb actuació / Bars musicals / Discoteques /PUB',
            'Equipaments culturals i recreatius', 'Arts gràfiques',
            'Fotografia'
        ],
        'restaurantes': [
            'Xocolateries / Geladeries / Degustació', 'Restaurants', 'Begudes',
            'Serveis de menjar take away MENJAR RÀPID', 'Bars   / CIBERCAFÈ',
            'serveis de menjar i begudes'
        ],
        'alojamiento': ["serveis d'allotjament"],
        'educacion': ['Ensenyament'],
        'culto': ['Equipaments religiosos'],
        'deporte': [
            'Esports', 'Joguines i esports', 'Gimnàs /fitnes',
            'Altres equipaments esportius'
        ]
    }

    df['categoria_lugar'] = df['nom_actividad']
    for category, mappings in categories.items():
        df['categoria_lugar'].replace(mappings, category, inplace=True)

    df = df[df['categoria_lugar'].isin(categories.keys())]
    return df


def process_antiguedad_vehiculos(dfs: dict) -> pd.DataFrame:
    df = dfs['antiguedad_vehiculos']
    df = df.rename(columns={'porc_total_barrio': 'vehic_antig_'})
    df = df.drop(columns='num_turismos')
    df = df.set_index(['anyo', 'id_barrio', 'int_antiguedad'])
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
    df = df.groupby(['anyo', 'id_barrio']) \
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
