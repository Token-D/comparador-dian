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

        nombres_columnas = df.iloc[2]

        df_procesado = df.iloc[3:].copy()
        df_procesado.columns = nombres_columnas
        df_procesado = df_procesado.reset_index(drop=True)

        df_procesado['Debitos'] = pd.to_numeric(df_procesado['Debitos'], errors='coerce')
        df_procesado['Creditos'] = pd.to_numeric(df_procesado['Creditos'], errors='coerce')

        df_procesado['Nit'] = df_procesado['Tercero'].str.extract(r'Nit:\s*(\d+)')

        df_agrupado = df_procesado.groupby(['Doc Num', 'Nota'], dropna=False).agg({
            'Debitos': 'sum',
            'Creditos': 'sum',
            'Tercero': 'first',
            'Nit': 'first',
            'Fecha': 'first'
        }).reset_index()

        df_agrupado = df_agrupado[df_agrupado['Doc Num'].notna() & (df_agrupado['Doc Num'] != '')]

        return df_agrupado

    except Exception as e:
        st.error(f"Error en procesamiento del Libro Auxiliar: {str(e)}")
        return None

TOLERANCIA = 1000   # diferencia máxima (en pesos) para considerar "Exacto"
DIAS_MAX = 30       # días máximos entre factura y egreso para el cruce "Por valor"

def buscar_coincidencias(df_token, df_libro, tolerancia=TOLERANCIA, dias_max=DIAS_MAX):
    """
    Clasifica cada factura del Token DIAN contra el Libro Auxiliar:
      Exacto           -> NIT + folio (número completo) y diferencia <= tolerancia
      Probable         -> NIT + folio, pero la diferencia supera la tolerancia (revisar)
      Por valor        -> sin folio en la nota, pero un único egreso del mismo NIT con
                          valor dentro de la tolerancia y fecha cercana
      Sin coincidencia -> no se encontró nada (validar manualmente)
    Entre varios candidatos se elige el de débito más cercano al total.
    """
    try:
        resultados = df_token.copy()
        resultados['Doc_Num_Encontrado'] = 'Validar Manualmente'
        resultados['Nota_Libro'] = ''
        resultados['Debito_Libro'] = np.nan
        resultados['Diferencia_Total'] = np.nan
        resultados['Clasificacion'] = 'Sin coincidencia'

        fechas_token = pd.to_datetime(resultados['Fecha Emisión'], errors='coerce')
        resultados['Fecha Emisión'] = fechas_token.dt.strftime('%d/%m/%Y')

        # Solo egresos con débito (las líneas de crédito no son el gasto)
        libro = df_libro.copy()
        libro['Fecha'] = pd.to_datetime(libro['Fecha'], errors='coerce')
        libro = libro[libro['Debitos'] > 0]
        libro_por_nit = {nit: g for nit, g in libro.groupby('Nit')}

        for idx in resultados.index:
            nit = str(resultados.at[idx, 'NIT Emisor'])
            folio = str(resultados.at[idx, 'Folio'])
            total = float(resultados.at[idx, 'Total'])
            fecha = fechas_token.at[idx]

            cand_nit = libro_por_nit.get(nit)
            if cand_nit is None or pd.isna(total):
                continue

            elegido, clasif = None, None

            # 1) Folio como número completo (no como parte de otro número)
            patron = r'(?<!\d)' + re.escape(folio) + r'(?!\d)'
            por_folio = cand_nit[cand_nit['Nota'].str.contains(patron, na=False, regex=True)]
            if not por_folio.empty:
                pos = (por_folio['Debitos'] - total).abs().to_numpy().argmin()
                elegido = por_folio.iloc[pos]
                clasif = 'Exacto' if abs(total - elegido['Debitos']) <= tolerancia else 'Probable'

            # 2) Sin folio: un único egreso con el mismo valor y fecha cercana
            else:
                por_valor = cand_nit[(cand_nit['Debitos'] - total).abs() <= tolerancia]
                if pd.notna(fecha):
                    por_valor = por_valor[(por_valor['Fecha'] - fecha).abs() <= pd.Timedelta(days=dias_max)]
                if len(por_valor) == 1:
                    elegido, clasif = por_valor.iloc[0], 'Por valor'

            if elegido is not None:
                resultados.at[idx, 'Doc_Num_Encontrado'] = elegido['Doc Num']
                resultados.at[idx, 'Nota_Libro'] = elegido['Nota']
                resultados.at[idx, 'Debito_Libro'] = elegido['Debitos']
                resultados.at[idx, 'Diferencia_Total'] = total - elegido['Debitos']
                resultados.at[idx, 'Clasificacion'] = clasif

        resultados['Total'] = resultados['Total'].round(1)
        resultados['Debito_Libro'] = resultados['Debito_Libro'].round(1)
        resultados['Diferencia_Total'] = resultados['Diferencia_Total'].round(1)

        # Primero lo que hay que revisar, luego lo resuelto; dentro de cada grupo, mayor diferencia primero
        orden = {'Probable': 0, 'Sin coincidencia': 1, 'Por valor': 2, 'Exacto': 3}
        resultados['_o'] = resultados['Clasificacion'].map(orden)
        resultados['_d'] = resultados['Diferencia_Total'].abs()
        resultados = resultados.sort_values(by=['_o', '_d'], ascending=[True, False], na_position='last')

        columnas_ordenadas = [
            'Tipo de documento', 'Folio', 'Prefijo', 'Fecha Emisión', 'NIT Emisor',
            'Nombre Emisor', 'NIT Receptor', 'Total', 'Doc_Num_Encontrado',
            'Nota_Libro', 'Debito_Libro', 'Diferencia_Total', 'Clasificacion'
        ]
        resultados = resultados[[c for c in columnas_ordenadas if c in resultados.columns]]

        # Solo las columnas de texto se rellenan con vacío; las numéricas quedan con NaN
        cols_texto = [c for c in resultados.columns
                      if c not in ('Total', 'Debito_Libro', 'Diferencia_Total')]
        resultados[cols_texto] = resultados[cols_texto].fillna('')

        return resultados

    except Exception as e:
        st.error(f"Error en búsqueda de coincidencias: {str(e)}")
        return None

def to_excel(df, nombre_empresa):
    """
    Convierte el DataFrame de resultados a un archivo Excel en un buffer (BytesIO).
    """
    if not isinstance(df, pd.DataFrame):
        return None, None

    numeric_cols = ['Total', 'Debito_Libro', 'Diferencia_Total']

    df_safe = df.copy()
    for col in df.columns:
        if col not in numeric_cols:
            df_safe[col] = df_safe[col].fillna('').astype(str)

    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_safe.to_excel(writer, index=False, sheet_name='Resultados')

        workbook = writer.book
        worksheet = writer.sheets['Resultados']

        number_format = workbook.add_format({'num_format': '#,##0.00'})
        header_format = workbook.add_format({'bg_color': '#D9F2D9', 'bold': True, 'border': 1})
        default_header_format = workbook.add_format({'bold': True, 'border': 1})

        worksheet.freeze_panes(1, 0)

        for col_num, value in enumerate(df.columns.values):
            if 8 <= col_num <= 12:
                worksheet.write(0, col_num, value, header_format)
            else:
                worksheet.write(0, col_num, value, default_header_format)

        # Ancho de columnas (+ formato numérico en las columnas de montos)
        for i, col in enumerate(df.columns):
            max_len = max(df[col].fillna('').astype(str).map(len).max(), len(col)) + 2
            if col in numeric_cols:
                worksheet.set_column(i, i, max_len, number_format)
            else:
                worksheet.set_column(i, i, max_len)

    processed_data = output.getvalue()

    fecha_actual = datetime.now().strftime('%Y%m%d')
    numero_aleatorio = random.randint(1000, 9999)
    nombre_archivo = f"{nombre_empresa}_Token_{fecha_actual}_{numero_aleatorio}.xlsx"

    return processed_data, nombre_archivo

def main():
    st.title('🔄 Comparador Token DIAN y Libro Auxiliar')

    with st.sidebar:
        st.header("Instrucciones")
        st.write("""
        1. Ingrese el **nombre de la empresa**
        2. Cargue el archivo **Token DIAN**
        3. Cargue el archivo **Libro Auxiliar**
        4. El sistema procesará los archivos y generará un **botón de descarga** del Excel con los resultados.
        """)

    nombre_empresa = st.text_input('Nombre de la empresa:',
                                 help='Este nombre se usará para generar el archivo de resultados')

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Token DIAN")
        archivo_token = st.file_uploader("Cargar archivo Token DIAN", type=['xlsx'])

    with col2:
        st.subheader("Libro Auxiliar")
        archivo_libro = st.file_uploader("Cargar archivo Libro Auxiliar", type=['xlsx'])

    if archivo_token and archivo_libro and nombre_empresa:
        if st.button('Procesar y Analizar Datos'):
            with st.spinner('Procesando archivos y generando Excel...'):
                try:
                    df_token = pd.read_excel(archivo_token)
                    df_libro = pd.read_excel(archivo_libro)

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

        excel_data, nombre_archivo_excel = to_excel(df_resultados, nombre_empresa_file)

        if excel_data is not None and nombre_archivo_excel is not None:
            st.subheader("✅ Descargar Resultados")
            st.write(f"El archivo **{nombre_archivo_excel}** está listo para descargar.")

            st.download_button(
                label="Descargar Excel",
                data=excel_data,
                file_name=nombre_archivo_excel,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key='download_excel'
            )
        else:
            st.error("Error: No se puede crear el archivo Excel.")

    else:
        if not (archivo_token and archivo_libro and nombre_empresa):
            st.info("Por favor, complete todos los campos y cargue los archivos necesarios para iniciar el procesamiento.")

if __name__ == "__main__":
    if 'resultados_df' not in st.session_state:
        st.session_state['resultados_df'] = None
    if 'nombre_empresa' not in st.session_state:
        st.session_state['nombre_empresa'] = ''

    main()
