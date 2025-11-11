import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import random
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

def to_excel(df, nombre_empresa):
    """
    Convierte el DataFrame de resultados a un archivo Excel en un buffer (BytesIO).
    Aplica formato numérico a las columnas 'Total' y 'Diferencia_Total'.
    """
    
    if not isinstance(df, pd.DataFrame):
        return None, None 
    
    # 1. Crear una copia segura para la conversión a string
    # Identificar columnas numéricas que deben ser EXCLUIDAS de la conversión a string
    numeric_cols = ['Total', 'Debito_Libro', 'Diferencia_Total'] 
    
    # Hacer una copia del DF para la escritura
    df_safe = df.copy()
    
    # Convertir todas las columnas A STRING EXCEPTO las columnas numéricas
    for col in df.columns:
        if col not in numeric_cols:
            df_safe[col] = df_safe[col].astype(str)
        # Para las columnas numéricas, aseguramos que los valores vacíos sean NaN (luego se tratan en el formato)
        # Esto es seguro ya que se hicieron los redondeos y rellenos antes de este paso.

    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        
        # Usar el DataFrame limpio (df_safe) para la escritura en Excel
        df_safe.to_excel(writer, index=False, sheet_name='Resultados')
        
        workbook = writer.book
        worksheet = writer.sheets['Resultados']
        
        # 2. Definir formato numérico (e.g., separador de miles y dos decimales)
        number_format = workbook.add_format({'num_format': '#,##0.00'})
        
        # Formato de color para las columnas de coincidencia 
        header_format = workbook.add_format({
            'bg_color': '#D9F2D9', 
            'bold': True,
            'border': 1
        })
        
        # Formato básico para las demás celdas de encabezado
        default_header_format = workbook.add_format({'bold': True, 'border': 1})
        
        # Congelar la fila superior
        worksheet.freeze_panes(1, 0)
        
        # Escribir encabezados con formato
        for col_num, value in enumerate(df.columns.values):
            if 8 <= col_num <= 11: 
                worksheet.write(0, col_num, value, header_format)
            else:
                worksheet.write(0, col_num, value, default_header_format)
        
        # 3. Aplicar formato numérico a las columnas después de los encabezados
        
        # Columna 'Total' (Índice H es 7)
        col_total_idx = df.columns.get_loc('Total')
        # Columna 'Diferencia_Total' (Índice L es 11)
        col_diferencia_idx = df.columns.get_loc('Diferencia_Total')
        
        # Aplicar el formato numérico desde la segunda fila hasta el final (1 es la segunda fila)
        # El rango es de fila 1 (segunda fila) a la última fila, columna 7 (Total)
        worksheet.set_column(col_total_idx, col_total_idx, None, number_format)
        
        # Aplicar el formato numérico al 'Diferencia_Total' (columna 11)
        worksheet.set_column(col_diferencia_idx, col_diferencia_idx, None, number_format)

        # Ajuste de ancho de columnas (ahora solo se ajusta el ancho, no el formato de celda)
        for i, col in enumerate(df.columns):
            # Usar df.columns para el ancho
            max_len = max(df[col].astype(str).apply(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_len)
            
    processed_data = output.getvalue()
    
    # Crear nombre del archivo
    fecha_actual = datetime.now().strftime('%Y%m%d')
    numero_aleatorio = random.randint(1000, 9999)
    nombre_archivo = f"{nombre_empresa}_Comparacion_{fecha_actual}_{numero_aleatorio}.xlsx"
    
    return processed_data, nombre_archivo

def main():
    st.title('🔄 Comparador Token DIAN y Libro Auxiliar')
    
    # Sidebar con instrucciones
    with st.sidebar:
        st.header("Instrucciones")
        st.write("""
        1. Ingrese el **nombre de la empresa**
        2. Cargue el archivo **Token DIAN**
        3. Cargue el archivo **Libro Auxiliar**
        4. El sistema procesará los archivos y generará un **botón de descarga** del Excel con los resultados.
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
    
    # Verificar que todos los campos necesarios estén completos
    if archivo_token and archivo_libro and nombre_empresa: # Eliminé la dependencia de user_email
        if st.button('Procesar y generar archivo de descarga'):
            with st.spinner('Procesando archivos y generando Excel...'):
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
                            st.session_state['resultados_df'] = resultados
                            st.session_state['nombre_empresa'] = nombre_empresa
                            st.success("¡Procesamiento completado! Desplázate hacia abajo para descargar el archivo.")
                        else:
                            st.error("Error al buscar coincidencias en los datos.")
                    else:
                        st.error("Error al procesar los archivos.")

                except Exception as e:
                    st.error(f"Error en el procesamiento: {str(e)}")

    if 'resultados_df' in st.session_state and st.session_state['resultados_df'] is not None:
        df_resultados = st.session_state['resultados_df']
        nombre_empresa_file = st.session_state['nombre_empresa']
        
        # Generar Excel y nombre del archivo
        excel_data, nombre_archivo_excel = to_excel(df_resultados, nombre_empresa_file)
        
        # *** LOGICA DE MENSAJE DE ERROR Y DESCARGA AQUI ***
        if excel_data is not None and nombre_archivo_excel is not None:
            st.subheader("✅ Descargar Resultados")
            st.write(f"El archivo **{nombre_archivo_excel}** está listo para descargar.")
            
            # Botón de descarga
            st.download_button(
                label="Descargar Excel de Resultados",
                data=excel_data,
                file_name=nombre_archivo_excel,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key='download_excel'
            )
            
            st.info("Nota: La descarga es local. El proceso de Google Sheets y la entrada de correo electrónico han sido deshabilitados.")
        else:
            # ESTE ES EL NUEVO LUGAR PARA MOSTRAR EL MENSAJE DE ERROR:
            # Solo se muestra si to_excel fue llamada (df_resultados no es None) pero falló
            st.error("Error: Los datos de resultados no son un DataFrame válido. No se puede crear el archivo Excel. Verifique que el procesamiento anterior haya sido exitoso.")
            
    else:
        if not (archivo_token and archivo_libro and nombre_empresa):
            st.info("Por favor, complete todos los campos y cargue los archivos necesarios para iniciar el procesamiento.")

if __name__ == "__main__":
    # Inicializar session_state
    if 'resultados_df' not in st.session_state:
        st.session_state['resultados_df'] = None
    if 'nombre_empresa' not in st.session_state:
        st.session_state['nombre_empresa'] = ''

    main()




