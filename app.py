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
        # Crear una copia del DataFrame
        df_procesado = df.copy()
        
        # Filtrar registros
        df_procesado = df_procesado[df_procesado['Grupo'] == 'Recibido']
        df_procesado = df_procesado[df_procesado['Tipo de documento'] != 'Application response']
        
        # Procesar la columna "Folio"
        df_procesado['Folio'] = df_procesado['Folio'].apply(
            lambda x: x[2:] if isinstance(x, str) and x.startswith('NC') else x
        )
        
        # Asegurar que 'Total' sea numérico
        df_procesado['Total'] = pd.to_numeric(df_procesado['Total'], errors='coerce')
        
        # Asegurar que 'NIT Emisor' sea string
        df_procesado['NIT Emisor'] = df_procesado['NIT Emisor'].astype(str)
        
        return df_procesado
        
    except Exception as e:
        st.error(f"Error en el procesamiento del Token DIAN: {str(e)}")
        return None

def procesar_libro_auxiliar(df):
    try:
        st.write("Procesando Libro Auxiliar...")
        
        # Obtener los nombres de las columnas de la fila 2
        nombres_columnas = df.iloc[2]
        
        # Crear nuevo DataFrame desde la fila 3
        df_procesado = df.iloc[3:].copy()
        df_procesado.columns = nombres_columnas
        df_procesado = df_procesado.reset_index(drop=True)
        
        # Convertir columnas numéricas
        df_procesado['Debitos'] = pd.to_numeric(df_procesado['Debitos'], errors='coerce')
        df_procesado['Creditos'] = pd.to_numeric(df_procesado['Creditos'], errors='coerce')
        
        # Extraer NIT
        df_procesado['Nit'] = df_procesado['Tercero'].str.extract(r'Nit:\s*(\d+)')
        
        # Agrupar por Doc Num y Nota
        df_agrupado = df_procesado.groupby(['Doc Num', 'Nota'], dropna=False).agg({
            'Debitos': 'sum',
            'Creditos': 'sum',
            'Tercero': 'first',
            'Nit': 'first'
        }).reset_index()
        
        # Eliminar filas donde Doc Num es NaN o vacío
        df_agrupado = df_agrupado[df_agrupado['Doc Num'].notna() & (df_agrupado['Doc Num'] != '')]
        
        return df_agrupado
        
    except Exception as e:
        st.error(f"Error en procesamiento del Libro Auxiliar: {str(e)}")
        return None

def buscar_coincidencias(df_token, df_libro):
    try:
        # Crear DataFrame para resultados
        resultados = df_token.copy()
        resultados['Doc_Num_Encontrado'] = 'Validar Manualmente'
        resultados['Nota_Libro'] = ''
        resultados['Debito_Libro'] = np.nan
        resultados['Diferencia_Total'] = np.nan
        
        # Convertir fecha a formato DD/MM/AAAA
        resultados['Fecha Emisión'] = pd.to_datetime(resultados['Fecha Emisión']).dt.strftime('%d/%m/%Y')
        
        # Búsqueda de coincidencias
        for idx, row in resultados.iterrows():
            coincidencias = df_libro[
                (df_libro['Nit'] == str(row['NIT Emisor'])) &
                (df_libro['Debitos'] == float(row['Total'])) &
                df_libro['Nota'].str.contains(str(row['Folio']), na=False)
            ]
            
            if not coincidencias.empty:
                resultados.at[idx, 'Doc_Num_Encontrado'] = coincidencias.iloc[0]['Doc Num']
                resultados.at[idx, 'Nota_Libro'] = coincidencias.iloc[0]['Nota']
                resultados.at[idx, 'Debito_Libro'] = coincidencias.iloc[0]['Debitos']
                resultados.at[idx, 'Diferencia_Total'] = float(row['Total']) - coincidencias.iloc[0]['Debitos']
                continue
            
            # Búsqueda secundaria
            coincidencias = df_libro[
                (df_libro['Nit'] == str(row['NIT Emisor'])) &
                df_libro['Nota'].str.contains(str(row['Folio']), na=False)
            ]
            
            if not coincidencias.empty:
                resultados.at[idx, 'Doc_Num_Encontrado'] = coincidencias.iloc[0]['Doc Num']
                resultados.at[idx, 'Nota_Libro'] = coincidencias.iloc[0]['Nota']
                resultados.at[idx, 'Debito_Libro'] = coincidencias.iloc[0]['Debitos']
                resultados.at[idx, 'Diferencia_Total'] = float(row['Total']) - coincidencias.iloc[0]['Debitos']
        
        # Redondear columnas numéricas a 1 decimal
        resultados['Total'] = resultados['Total'].round(1)
        resultados['Debito_Libro'] = resultados['Debito_Libro'].round(1)
        resultados['Diferencia_Total'] = resultados['Diferencia_Total'].round(1)
        
        # Reemplazar NaN con celdas vacías
        resultados.fillna('', inplace=True)
        
        # Ordenar por Diferencia_Total y Doc_Num_Encontrado
        resultados = resultados.sort_values(
            by=['Diferencia_Total', 'Doc_Num_Encontrado'],
            ascending=[False, False],
            na_position='last'
        )
        
        # Seleccionar y ordenar columnas
        columnas_ordenadas = [
            'Tipo de documento', 'Folio', 'Prefijo', 'Fecha Emisión', 'NIT Emisor', 
            'Nombre Emisor', 'NIT Receptor', 'Total', 'Doc_Num_Encontrado',
            'Nota_Libro', 'Debito_Libro', 'Diferencia_Total'
        ]
        
        # Filtrar solo las columnas que existen
        columnas_existentes = [col for col in columnas_ordenadas if col in resultados.columns]
        resultados = resultados[columnas_existentes]
        
        return resultados
        
    except Exception as e:
        st.error(f"Error en búsqueda de coincidencias: {str(e)}")
        return None

def crear_google_sheet(resultados, nombre_empresa):
    try:
        # Convertir DataFrame a string para el sheet
        df_para_sheet = resultados.astype(str)
        
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
        
        # Crear servicio
        service = build('sheets', 'v4', credentials=credentials)
        
        # Crear spreadsheet
        spreadsheet = sheets_service.spreadsheets().create(body={
            'properties': {'title': nombre_archivo}
        }).execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')
        
        # Preparar datos
        valores = [df_para_sheet.columns.tolist()] + df_para_sheet.values.tolist()
        
        # Escribir datos
        body = {
            'values': valores
        }
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Sheet1!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # Aplicar formato
        requests = [
            # Inmovilizar primera fila
            {
                "updateSheetProperties": {
                    "properties": {
                        "gridProperties": {
                            "frozenRowCount": 1
                        }
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            },
            # Color verde claro para encabezados específicos
            {
                "repeatCell": {
                    "range": {
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 8,  # Columna I (Doc_Num_Encontrado)
                        "endColumnIndex": 12    # Hasta la última columna
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.85,
                                "green": 0.92,
                                "blue": 0.85
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            }
        ]
        
        # Aplicar formatos
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
        
        # Mover a carpeta específica
        folder_id = '1Kup1_bWb2OTiuitmNE_zNurvplaLmerE'  # ID de la carpeta destino
        drive_service.files().update(
            fileId=spreadsheet_id,
            addParents=folder_id,
            removeParents='root',
            fields='id, parents'
        ).execute()
                
        # Compartir con el usuario específico
        permiso = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': 'lkaterinmarin@gmail.com'
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permiso,
            fields='id'
        ).execute()
        
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        
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