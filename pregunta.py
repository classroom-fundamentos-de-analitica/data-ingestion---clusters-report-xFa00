"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    df= pd.read_fwf("clusters_report.txt",widths=[9, 16, 16, 80],header=None)
    lista = df[:2].fillna('').apply(lambda x: ' ' + x).sum().tolist()
    lista = [col.strip().lower().replace(' ', '_') for col in lista]

    df = df[3:]
    df.columns = lista
    df = df.fillna(method = 'ffill')
        
    df['principales_palabras_clave'] = df['principales_palabras_clave'].apply(
        lambda palabras: ' ' + palabras
    )
        
    df = df.groupby(['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave'], as_index=False)[['principales_palabras_clave']].sum()
    df['cantidad_de_palabras_clave'] = df['cantidad_de_palabras_clave'].astype(int)
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace(',', '.').apply(lambda x: float(x[:-2]))
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace(".", "", regex=True)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace("   "," ")
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace("  "," ")
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace("  "," ")
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.strip()


    df.cluster = df.cluster.astype(int)
    df = df.sort_values('cluster')
    df = df.reset_index(drop=True)

    return df
