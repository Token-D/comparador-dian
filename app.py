import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import random
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import BytesIO
import re

# Configuración de la página
st.set_page_config(
    page_title="Comparador DIAN",
    page_icon="📊",
    layout="wide"
)

def procesar_token_dian(df):
    try:
        # Crear una copia del DataFrame para no modificar el original
        df_procesado = df.copy()
        
        # Filtrar solo los registros "Recibido" de la columna "Grupo"
        df_procesado = df_procesado[df_procesado['Grupo'] == 'Recibido']
        
        # Quitar los registros "Application response" de "Tipo de documento"
        df_procesado = df_procesado[df_procesado['Tipo de documento'] != 'Application response']
        
        # Procesar la columna "Folio" para quitar "NC" al inicio
        df_procesado['Folio'] = df_procesado['Folio'].apply(
            lambda x: x[2:] if isinstance(x, str) and x.startswith('NC') else x
        )
        
        # Mostrar resumen del procesamiento
        st.write("Resumen del procesamiento Token DIAN:")
        st.write(f"- Registros originales: {len(df)}")
        st.write(f"- Registros después del filtro: {len(df_procesado)}")
        
        return df_procesado
        
    except Exception as e:
        st.error(f"Error en el procesamiento del Token DIAN: {str(e)}")
        return None

def procesar_libro_auxiliar(df):
    try:
        st.write("Procesando Libro Auxiliar...")
        
        # Obtener los nombres de las columnas de la fila 2 (índice 2)
        nombres_columnas = df.iloc[2]
        st.write("Nombres de columnas encontrados en fila 2:")
        st.write(list(nombres_columnas))
        
        # Crear nuevo DataFrame desde la fila 3 en adelante (índice 3)
        df_procesado = df.iloc[3:].copy()
        
        # Asignar los nombres de las columnas
        df_procesado.columns = nombres_columnas
        
        # Resetear el índice
        df_procesado = df_procesado.reset_index(drop=True)
        
        # Convertir columnas numéricas
        df_procesado['Debitos'] = pd.to_numeric(df_procesado['Debitos'], errors='coerce')
        df_procesado['Creditos'] = pd.to_numeric(df_procesado['Creditos'], errors='coerce')
        
        # Extraer NIT de la columna Tercero
        df_procesado['Nit'] = df_procesado['Tercero'].str.extract(r'Nit:\s*(\d+)')
        
        # Agrupar solo por Doc Num
        df_agrupado = df_procesado.groupby('Doc Num', dropna=False).agg({
            'Debitos': 'sum',
            'Creditos': 'sum',
            'Tercero': 'first',  # Mantener el primer tercero
            'Nit': 'first',      # Mantener el primer NIT
            'Nota': 'first'      # Mantener la primera nota
        }).reset_index()
        
        # Eliminar filas donde Doc Num es NaN o vacío
        df_agrupado = df_agrupado[df_agrupado['Doc Num'].notna() & (df_agrupado['Doc Num'] != '')]
        
        # Mostrar resumen del procesamiento
        st.write("\nResumen del procesamiento:")
        st.write(f"- Registros originales: {len(df_procesado)}")
        st.write(f"- Registros después de agrupar: {len(df_agrupado)}")
        
        # Mostrar muestra de los resultados
        st.write("\nMuestra de los resultados agrupados:")
        st.write(df_agrupado.head())
        
        return df_agrupado
        
    except Exception as e:
        st.error(f"Error en procesamiento del Libro Auxiliar: {str(e)}")
        st.write("Estructura del archivo original:")
        st.write(df.head(10))
        st.write("Total de filas en el archivo:", len(df))
        return None

def buscar_coincidencias(df_token, df_libro):
    try:
        resultados = df_token.copy()
        resultados['Doc_Num_Encontrado'] = 'Validar Manualmente'
        
        # Primera búsqueda: coincidencia exacta de 3 campos
        for idx, row in resultados.iterrows():
            coincidencias = df_libro[
                (df_libro['Nit'] == str(row['NIT Emisor'])) &
                (df_libro['Debito'] == row['Total']) &
                (df_libro['Nota'].str.contains(str(row['Folio']), na=False))
            ]
            
            if not coincidencias.empty:
                resultados.at[idx, 'Doc_Num_Encontrado'] = coincidencias.iloc[0]['Doc Num']
                continue
                
            # Segunda búsqueda: solo NIT y Folio
            coincidencias = df_libro[
                (df_libro['Nit'] == str(row['NIT Emisor'])) &
                (df_libro['Nota'].str.contains(str(row['Folio']), na=False))
            ]
            
            if not coincidencias.empty:
                resultados.at[idx, 'Doc_Num_Encontrado'] = coincidencias.iloc[0]['Doc Num']
        
        return resultados
        
    except Exception as e:
        st.error(f"Error en búsqueda de coincidencias: {str(e)}")
        return None

def crear_google_sheet(df, nombre_empresa):
    try:
        # Convertir todas las columnas a string para evitar problemas de formato
        df = df.astype(str)
        
        # Crear nombre del archivo
        fecha_actual = datetime.now().strftime('%Y%m%d')
        numero_aleatorio = random.randint(1000, 9999)
        nombre_archivo = f"{nombre_empresa}_{fecha_actual}_{numero_aleatorio}"

        # Configurar credenciales
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
        )
        
        # Crear servicio de Sheets y Drive
        sheets_service = build('sheets', 'v4', credentials=credentials)
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Generar nombre del archivo
        fecha_actual = datetime.now().strftime("%Y%m%d")
        numero_aleatorio = random.randint(1000, 9999)
        nombre_archivo = f"{nombre_empresa}_{fecha_actual}_{numero_aleatorio}"
        
        # Crear nuevo spreadsheet
        spreadsheet = {
            'properties': {
                'title': nombre_archivo
            }
        }
        
        spreadsheet = sheets_service.spreadsheets().create(
            body=spreadsheet,
            fields='spreadsheetId'
        ).execute()
        
        # Mover archivo a la carpeta específica
        file = drive_service.files().update(
            fileId=spreadsheet.get('spreadsheetId'),
            addParents='1Kup1_bWb2OTiuitmNE_zNurvplaLmerE',
            fields='id, webViewLink'
        ).execute()
        
        # Actualizar datos en el spreadsheet
        rango = 'Sheet1!A1'
        body = {
            'values': [df_resultados.columns.values.tolist()] + df_resultados.values.tolist()
        }
        
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet.get('spreadsheetId'),
            range=rango,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        return file.get('webViewLink')
        
    except Exception as e:
        st.error(f"Error al crear Google Sheet: {str(e)}")
        return None

def main():
    st.title('🔄 Comparador Token DIAN y Libro Auxiliar')
    
    # Sidebar con instrucciones
    with st.sidebar:
        st.header("Instrucciones")
        st.write("""
        1. Ingrese el nombre de la empresa
        2. Cargue el archivo Token DIAN
        3. Cargue el archivo Libro Auxiliar
        4. El sistema procesará los archivos y generará un Google Sheet con los resultados
        """)
    
    # Campo para nombre de empresa
    nombre_empresa = st.text_input('Nombre de la empresa:', 
                                 help='Este nombre se usará para generar el archivo de resultados')
    
    # Carga de archivos
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Token DIAN")
        archivo_token = st.file_uploader("Cargar archivo Token DIAN", type=['xlsx'])
        
    with col2:
        st.subheader("Libro Auxiliar")
        archivo_libro = st.file_uploader("Cargar archivo Libro Auxiliar", type=['xlsx'])
    
    if archivo_token and archivo_libro and nombre_empresa:
        if st.button('Procesar archivos'):
            with st.spinner('Procesando archivos...'):
                try:
                    # Cargar y procesar archivos
                    df_token = pd.read_excel(archivo_token)
                    df_libro = pd.read_excel(archivo_libro)
                    
                    df_token_proc = procesar_token_dian(df_token)
                    df_libro_proc = procesar_libro_auxiliar(df_libro)
                    
                    if df_token_proc is not None and df_libro_proc is not None:
                        resultados = buscar_coincidencias(df_token_proc, df_libro_proc)
                        
                        if resultados is not None:
                            st.success("¡Procesamiento completado!")
                            
                            # Crear Google Sheet y obtener link
                            link_sheet = crear_google_sheet(resultados, nombre_empresa)
                            
                            if link_sheet:
                                st.success("¡Archivo creado exitosamente!")
                                st.write("Link al archivo de resultados:")
                                st.markdown(f"[Abrir Google Sheet]({link_sheet})")
                
                except Exception as e:
                    st.error(f"Error en el procesamiento: {str(e)}")

if __name__ == "__main__":
    main()