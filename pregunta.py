"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

with open('clusters_report.txt', 'r') as file:
    lines = file.readlines()

def clean_column_3(x):
    x = x.replace('%', '')
    x = x.replace(',', '.')
    x = x.strip()
    return x

types_df = {
    'cluster': int,
    'cantidad_de_palabras_clave': int,
    'porcentaje_de_palabras_clave': float,
    'principales_palabras_clave': str
}

def ingest_data():
    lista = re.findall(r'\b[A-Z][a-z\s]*', lines[0])
    longitudes = [len(elemento) for elemento in lista]

    new_list = []
    for line in lines:
        if line.strip()=='':
            new_list.append(['\n'])
            continue
        if '-------------------' in line:
            continue
        line = line.rstrip('\n')
        new_line = [line[:longitudes[0]].strip(),
                    line[longitudes[0]:longitudes[0] + longitudes[1]].strip(),
                    line[longitudes[0] + longitudes[1]:longitudes[0] + longitudes[1] + longitudes[2]].strip(),
                    line[longitudes[0] + longitudes[1] + longitudes[2]:].strip()]
        new_list.append(new_line)

    listas = new_list
    
    listas_divididas = []
    sublista = []

    for lista in listas:
        if lista != ['\n']:
            sublista.append(lista)
        else:
            listas_divididas.append(sublista)
            sublista = []

    # Agregar la última sublista si no termina en '\n'
    if sublista:
        listas_divididas.append(sublista)
    
    clean_lines = []
    for lista in listas_divididas:
        clean_line = [' '.join(elementos) for elementos in zip(*lista)]
        clean_lines.append(clean_line)
    
    encabezado = clean_lines[0]
    head = [word.lower().strip().replace(' ', '_') for word in encabezado]
    datos = clean_lines[1:]

    df = pd.DataFrame(datos, columns=head)

    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].apply(clean_column_3)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('\s+', ' ', regex=True)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('.', '')
    df = df.astype(types_df)

    return df
