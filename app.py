import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import random
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import BytesIO
import re

import streamlit_authenticator as stauth
from yaml.loader import SafeLoader
import yaml

# Configuración de la página
st.set_page_config(
    page_title="Comparador DIAN",
    page_icon="📊",
    layout="wide"
)

# Cargar las credenciales del archivo config.yaml
try:
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
except FileNotFoundError:
    st.error("Error: Archivo 'config.yaml' no encontrado. Asegúrate de crearlo.")
    st.stop()
    
# Crear el objeto Authenticate
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
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

def crear_google_sheet(resultados, nombre_empresa, user_email):
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
        
        # Crear servicios de Sheets y Drive
        sheets_service = build('sheets', 'v4', credentials=credentials)
        drive_service = build('drive', 'v3', credentials=credentials)
        
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
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Sheet1!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # Aplicar formato
        requests = [
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
            {
                "repeatCell": {
                    "range": {
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 8,
                        "endColumnIndex": 12
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
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
        
        # Mover a carpeta específica
        folder_id = '1Kup1_bWb2OTiuitmNE_zNurvplaLmerE'
        drive_service.files().update(
            fileId=spreadsheet_id,
            addParents=folder_id,
            removeParents='root',
            fields='id, parents'
        ).execute()
        
        # Configurar permisos para cualquier persona con el enlace
        domain_permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=domain_permission
        ).execute()
        
        # Dar permisos de edición al usuario específico
        if user_email:
            user_permission = {
                'type': 'user',
                'role': 'writer',
                'emailAddress': user_email
            }
            drive_service.permissions().create(
                fileId=spreadsheet_id,
                body=user_permission,
                sendNotificationEmail=True
            ).execute()
        
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        
    except Exception as e:
        st.error(f"Error al crear Google Sheet: {str(e)}")
        return None

def main():
    # 1. Mostrar el formulario de inicio de sesión
    name, authentication_status, username = authenticator.login('Iniciar Sesión', 'main')
    
    # 2. Control de flujo de autenticación
    
    if authentication_status:
        # ----------------------------------------------------------------------
        # A. CÓDIGO DE LA APLICACIÓN (SOLO SE MUESTRA SI ESTÁ LOGUEADO)
        # ----------------------------------------------------------------------
        
        # Muestra el botón de cerrar sesión en el sidebar y el saludo
        authenticator.logout('Cerrar Sesión', 'sidebar')
        st.sidebar.title(f"Bienvenido, {name}")
        
        st.title('🔄 Comparador Token DIAN y Libro Auxiliar')
        
        # Sidebar con instrucciones (ahora debajo del logout)
        with st.sidebar:
            st.header("Instrucciones")
            st.write("""
            1. Ingrese el nombre de la empresa
            2. Cargue el archivo Token DIAN
            3. Cargue el archivo Libro Auxiliar
            4. El sistema procesará los archivos y generará un Google Sheet con los resultados
            """)
        
        # EL RESTO DEL CÓDIGO DE TU APP ORIGINAL VA AQUI DENTRO DEL 'if'
        
        # Campo para nombre de empresa
        nombre_empresa = st.text_input('Nombre de la empresa:',
                                      help='Este nombre se usará para generar el archivo de resultados')

        # Agregar campo para el correo del usuario
        user_email = st.text_input('Correo electrónico del usuario:',
                                  help="Se usará para dar acceso al archivo.")
        
        # Carga de archivos
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Token DIAN")
            archivo_token = st.file_uploader("Cargar archivo Token DIAN", type=['xlsx'])
            
        with col2:
            st.subheader("Libro Auxiliar")
            archivo_libro = st.file_uploader("Cargar archivo Libro Auxiliar", type=['xlsx'])
        
        # Verificar que todos los campos necesarios estén completos
        if archivo_token and archivo_libro and nombre_empresa and user_email:
            if st.button('Procesar archivos'):
                with st.spinner('Procesando archivos...'):
                    try:
                        # Leer archivos
                        df_token = pd.read_excel(archivo_token)
                        df_libro = pd.read_excel(archivo_libro)

                        # Procesar datos
                        df_token_proc = procesar_token_dian(df_token)
                        df_libro_proc = procesar_libro_auxiliar(df_libro)

                        if df_token_proc is not None and df_libro_proc is not None:
                            resultados = buscar_coincidencias(df_token_proc, df_libro_proc)

                            if resultados is not None:
                                st.success("¡Procesamiento completado!")

                                # Crear Google Sheet y compartir con el usuario
                                link_sheet = crear_google_sheet(resultados, nombre_empresa, user_email)

                                if link_sheet:
                                    st.success("¡Archivo creado exitosamente!")
                                    st.write("Link al archivo de resultados:")
                                    st.markdown(f"[Abrir Google Sheet]({link_sheet})")
                                else:
                                    st.error("No se pudo crear el archivo en Google Sheets.")
                            else:
                                st.error("Error al buscar coincidencias en los datos.")
                        else:
                            st.error("Error al procesar los archivos.")

                    except Exception as e:
                        st.error(f"Error en el procesamiento: {str(e)}")
        else:
            st.info("Por favor, complete todos los campos y cargue los archivos necesarios.")
            
    # ----------------------------------------------------------------------
    # B. MENSAJES PARA USUARIOS NO AUTENTICADOS
    # ----------------------------------------------------------------------
    elif authentication_status is False:
        st.error('Usuario o Contraseña incorrectos')
        
    elif authentication_status is None:
        st.info('Por favor, ingresa tu usuario y contraseña para continuar.')


if __name__ == "__main__":
    main()

