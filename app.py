import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import random
from io import BytesIO
import re
from xlsxwriter.utility import xl_col_to_name

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

def leer_lineas_libro(df):
    """Líneas del Libro Auxiliar con encabezado correcto, números, NIT y fecha ya convertidos."""
    nombres_columnas = df.iloc[2]
    df_p = df.iloc[3:].copy()
    df_p.columns = nombres_columnas
    df_p = df_p.reset_index(drop=True)

    df_p['Debitos'] = pd.to_numeric(df_p['Debitos'], errors='coerce')
    df_p['Creditos'] = pd.to_numeric(df_p['Creditos'], errors='coerce')
    df_p['Nit'] = df_p['Tercero'].str.extract(r'Nit:\s*(\d+)')
    df_p['Fecha'] = pd.to_datetime(df_p['Fecha'], errors='coerce')
    return df_p

def _solo_movimientos(df_lineas):
    """Líneas con Doc Num (descarta saldos iniciales y filas de totales)."""
    return df_lineas[df_lineas['Doc Num'].notna() & (df_lineas['Doc Num'] != '')]

def procesar_libro_auxiliar(df):
    try:
        st.write("Procesando Libro Auxiliar...")

        df_procesado = leer_lineas_libro(df)

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

def agrupar_auxiliar(df_lineas):
    """Hoja de apoyo: una línea por Tercero + Doc Num + Nota + Fecha, con débitos, créditos y diferencia."""
    mov = _solo_movimientos(df_lineas)
    agr = mov.groupby(['Tercero', 'Doc Num', 'Nota', 'Fecha'], dropna=False).agg(
        **{'Suma de Debitos': ('Debitos', 'sum'), 'Suma de Creditos': ('Creditos', 'sum')}
    ).reset_index()
    agr['Diferencia (Deb - Cred)'] = (agr['Suma de Debitos'] - agr['Suma de Creditos']).round(2)
    return agr.sort_values(['Tercero', 'Fecha', 'Doc Num'], na_position='last').reset_index(drop=True)

def calcular_descuadres_tercero(df_lineas):
    """
    Por cada egreso (Doc Num) y tercero (NIT): débitos - créditos.
    'Egreso_Cuadra_en_Total' indica si el egreso completo cuadra aunque el tercero no
    (típico de una confusión de terceros: el mismo valor con signo contrario en otro NIT).
    """
    mov = _solo_movimientos(df_lineas)
    glob = mov.groupby('Doc Num').agg(D=('Debitos', 'sum'), C=('Creditos', 'sum'))
    glob['cuadra'] = (glob['D'] - glob['C']).abs() <= 1

    por_nit = mov.dropna(subset=['Nit']).groupby(['Doc Num', 'Nit']).agg(
        Tercero=('Tercero', 'first'),
        **{'Suma de Debitos': ('Debitos', 'sum'), 'Suma de Creditos': ('Creditos', 'sum')}
    ).reset_index()
    por_nit['Diferencia (Deb - Cred)'] = (por_nit['Suma de Debitos'] - por_nit['Suma de Creditos']).round(2)
    por_nit['Egreso_Cuadra_en_Total'] = por_nit['Doc Num'].map(glob['cuadra']).map({True: 'Sí', False: 'No'})
    return por_nit

TOLERANCIA = 1000   # diferencia máxima (en pesos) para considerar "Exacto"
DIAS_MAX = 30       # días máximos entre factura y egreso para el cruce "Por valor"

def buscar_coincidencias(df_token, df_libro, df_lineas=None, tolerancia=TOLERANCIA, dias_max=DIAS_MAX):
    """
    Cruce UNO A UNO entre facturas del Token DIAN y egresos del Libro Auxiliar.
    Un egreso (Doc Num + Nota) solo se asigna a una factura.

    Fase 1 - por folio (tiene prioridad): NIT + folio como número completo.
        Las parejas se asignan de la menor a la mayor diferencia de valor.
        Exacto   -> diferencia <= tolerancia
        Probable -> diferencia > tolerancia (revisar)
    Fase 2 - por valor, solo con facturas y egresos que quedaron libres:
        mismo NIT, valor dentro de la tolerancia y fecha dentro de dias_max.
        Las parejas se asignan de la menor a la mayor distancia en días.
    Lo que no cruza queda como "Sin coincidencia".
    """
    try:
        resultados = df_token.copy()
        resultados['Doc_Num_Encontrado'] = 'Validar Manualmente'
        resultados['Nota_Libro'] = ''
        resultados['Debito_Libro'] = np.nan
        resultados['Diferencia_Total'] = np.nan
        resultados['Clasificacion'] = 'Sin coincidencia'
        resultados['Dif_Deb_Cred_NIT'] = np.nan
        resultados['Alerta_Descuadre'] = ''

        fechas_token = pd.to_datetime(resultados['Fecha Emisión'], errors='coerce')
        resultados['Fecha Emisión'] = fechas_token.dt.strftime('%d/%m/%Y')

        # Solo egresos con débito (las líneas de crédito no son el gasto)
        libro = df_libro.copy()
        libro['Fecha'] = pd.to_datetime(libro['Fecha'], errors='coerce')
        libro = libro[libro['Debitos'] > 0]
        libro_por_nit = {nit: g for nit, g in libro.groupby('Nit')}

        facturas_usadas = set()   # índices del token ya cruzados
        egresos_usados = set()    # índices del libro ya cruzados

        def asignar(idx_t, idx_l, clasif):
            fila = libro.loc[idx_l]
            total = float(resultados.at[idx_t, 'Total'])
            resultados.at[idx_t, 'Doc_Num_Encontrado'] = fila['Doc Num']
            resultados.at[idx_t, 'Nota_Libro'] = fila['Nota']
            resultados.at[idx_t, 'Debito_Libro'] = fila['Debitos']
            resultados.at[idx_t, 'Diferencia_Total'] = total - fila['Debitos']
            resultados.at[idx_t, 'Clasificacion'] = clasif
            facturas_usadas.add(idx_t)
            egresos_usados.add(idx_l)

        # ---------- Fase 1: por folio ----------
        parejas = []
        for idx_t in resultados.index:
            total = resultados.at[idx_t, 'Total']
            cand = libro_por_nit.get(str(resultados.at[idx_t, 'NIT Emisor']))
            if cand is None or pd.isna(total):
                continue
            folio = str(resultados.at[idx_t, 'Folio'])
            patron = r'(?<!\d)' + re.escape(folio) + r'(?!\d)'
            por_folio = cand[cand['Nota'].str.contains(patron, na=False, regex=True)]
            for idx_l, deb in por_folio['Debitos'].items():
                parejas.append((abs(float(total) - deb), idx_t, idx_l))

        # Orden: menor diferencia primero; ante empate, la factura y el egreso más antiguos
        def _clave(p):
            f_t = fechas_token.get(p[1], pd.NaT)
            f_l = libro.at[p[2], 'Fecha']
            return (p[0],
                    f_t if pd.notna(f_t) else pd.Timestamp.max,
                    f_l if pd.notna(f_l) else pd.Timestamp.max)

        for dif, idx_t, idx_l in sorted(parejas, key=_clave):
            if idx_t in facturas_usadas or idx_l in egresos_usados:
                continue
            asignar(idx_t, idx_l, 'Exacto' if dif <= tolerancia else 'Probable')

        # ---------- Fase 2: por valor y fecha ----------
        parejas = []
        for idx_t in resultados.index:
            if idx_t in facturas_usadas:
                continue
            total = resultados.at[idx_t, 'Total']
            fecha = fechas_token.at[idx_t]
            cand = libro_por_nit.get(str(resultados.at[idx_t, 'NIT Emisor']))
            if cand is None or pd.isna(total) or pd.isna(fecha):
                continue
            cand = cand[~cand.index.isin(egresos_usados)]
            cand = cand[(cand['Debitos'] - float(total)).abs() <= tolerancia]
            for idx_l, f_l in cand['Fecha'].items():
                if pd.notna(f_l):
                    dias = abs((f_l - fecha).days)
                    if dias <= dias_max:
                        parejas.append((dias, idx_t, idx_l))

        for dias, idx_t, idx_l in sorted(parejas, key=_clave):
            if idx_t in facturas_usadas or idx_l in egresos_usados:
                continue
            asignar(idx_t, idx_l, 'Por valor')

        # ---------- Alerta informativa: ¿el egreso cuadra para ESE tercero? ----------
        # No cambia la clasificación; solo señala lo que conviene validar a mano.
        if df_lineas is not None:
            desc = calcular_descuadres_tercero(df_lineas)
            mapa = {(d, n): v for d, n, v in zip(desc['Doc Num'], desc['Nit'], desc['Diferencia (Deb - Cred)'])}
            for idx in resultados.index[resultados['Clasificacion'] != 'Sin coincidencia']:
                dif_nit = mapa.get((resultados.at[idx, 'Doc_Num_Encontrado'],
                                    str(resultados.at[idx, 'NIT Emisor'])))
                if dif_nit is not None:
                    resultados.at[idx, 'Dif_Deb_Cred_NIT'] = dif_nit
                    if abs(dif_nit) > tolerancia:
                        resultados.at[idx, 'Alerta_Descuadre'] = 'Revisar descuadre del tercero'

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
            'Nota_Libro', 'Debito_Libro', 'Diferencia_Total', 'Clasificacion',
            'Dif_Deb_Cred_NIT', 'Alerta_Descuadre'
        ]
        resultados = resultados[[c for c in columnas_ordenadas if c in resultados.columns]]

        # Solo las columnas de texto se rellenan con vacío; las numéricas quedan con NaN
        cols_texto = [c for c in resultados.columns
                      if c not in ('Total', 'Debito_Libro', 'Diferencia_Total', 'Dif_Deb_Cred_NIT')]
        resultados[cols_texto] = resultados[cols_texto].fillna('')

        return resultados

    except Exception as e:
        st.error(f"Error en búsqueda de coincidencias: {str(e)}")
        return None

def to_excel(df, nombre_empresa, df_aux=None, df_desc=None):
    """
    Excel descargable:
      - Resultados: cruce Token vs Libro (filas con descuadre del tercero resaltadas)
      - Auxiliar_Agrupado: libro consolidado por Tercero + Doc Num + Nota + Fecha
      - Descuadres_Tercero: egresos donde un tercero no cuadra (débitos vs créditos)
    """
    if not isinstance(df, pd.DataFrame):
        return None, None

    numeric_cols = ['Total', 'Debito_Libro', 'Diferencia_Total', 'Dif_Deb_Cred_NIT']
    verde = ['Doc_Num_Encontrado', 'Nota_Libro', 'Debito_Libro', 'Diferencia_Total',
             'Clasificacion', 'Dif_Deb_Cred_NIT', 'Alerta_Descuadre']

    df_safe = df.copy()
    for col in df.columns:
        if col not in numeric_cols:
            df_safe[col] = df_safe[col].fillna('').astype(str)

    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy') as writer:
        workbook = writer.book
        number_format = workbook.add_format({'num_format': '#,##0.00'})
        header_format = workbook.add_format({'bg_color': '#D9F2D9', 'bold': True, 'border': 1})
        default_header_format = workbook.add_format({'bold': True, 'border': 1})
        alerta_format = workbook.add_format({'bg_color': '#FFE699'})

        def ancho(serie, titulo):
            return min(max(serie.fillna('').astype(str).map(len).max(), len(titulo)) + 2, 60)

        # ---- Hoja 1: Resultados ----
        df_safe.to_excel(writer, index=False, sheet_name='Resultados')
        ws = writer.sheets['Resultados']
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(df), len(df.columns) - 1)
        for i, col in enumerate(df.columns):
            ws.write(0, i, col, header_format if col in verde else default_header_format)
            ws.set_column(i, i, ancho(df[col], col), number_format if col in numeric_cols else None)

        if 'Alerta_Descuadre' in df.columns and len(df) > 0:
            letra = xl_col_to_name(df.columns.get_loc('Alerta_Descuadre'))
            ws.conditional_format(1, 0, len(df), len(df.columns) - 1, {
                'type': 'formula',
                'criteria': f'=${letra}2<>""',
                'format': alerta_format
            })

        # ---- Hoja 2: Auxiliar agrupado ----
        if df_aux is not None and len(df_aux) > 0:
            df_aux.to_excel(writer, index=False, sheet_name='Auxiliar_Agrupado')
            wa = writer.sheets['Auxiliar_Agrupado']
            wa.freeze_panes(1, 0)
            wa.autofilter(0, 0, len(df_aux), len(df_aux.columns) - 1)
            for i, col in enumerate(df_aux.columns):
                wa.write(0, i, col, default_header_format)
                es_num = col in ('Suma de Debitos', 'Suma de Creditos', 'Diferencia (Deb - Cred)')
                wa.set_column(i, i, 14 if col == 'Fecha' else ancho(df_aux[col], col),
                              number_format if es_num else None)

        # ---- Hoja 3: Descuadres por tercero ----
        if df_desc is not None and len(df_desc) > 0:
            df_desc.to_excel(writer, index=False, sheet_name='Descuadres_Tercero')
            wd = writer.sheets['Descuadres_Tercero']
            wd.freeze_panes(1, 0)
            wd.autofilter(0, 0, len(df_desc), len(df_desc.columns) - 1)
            for i, col in enumerate(df_desc.columns):
                wd.write(0, i, col, default_header_format)
                es_num = col in ('Suma de Debitos', 'Suma de Creditos', 'Diferencia (Deb - Cred)')
                wd.set_column(i, i, ancho(df_desc[col], col), number_format if es_num else None)

    processed_data = output.getvalue()

    fecha_actual = datetime.now().strftime('%Y%m%d')
    numero_aleatorio = random.randint(1000, 9999)
    nombre_archivo = f"{nombre_empresa}_Token_{fecha_actual}_{numero_aleatorio}.xlsx"

    return processed_data, nombre_archivo

def _solo_descuadrados(df_desc, tolerancia=TOLERANCIA):
    if df_desc is None:
        return None
    d = df_desc[df_desc['Diferencia (Deb - Cred)'].abs() > tolerancia]
    return d.sort_values(['Doc Num', 'Nit']).reset_index(drop=True)

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
                    df_lineas = leer_lineas_libro(df_libro)

                    if df_token_proc is not None and df_libro_proc is not None:
                        resultados = buscar_coincidencias(df_token_proc, df_libro_proc, df_lineas=df_lineas)

                        if resultados is not None:
                            st.session_state['resultados_df'] = resultados
                            st.session_state['nombre_empresa'] = nombre_empresa
                            st.session_state['hoja_auxiliar'] = agrupar_auxiliar(df_lineas)
                            st.session_state['hoja_descuadres'] = calcular_descuadres_tercero(df_lineas)
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

        excel_data, nombre_archivo_excel = to_excel(
            df_resultados, nombre_empresa_file,
            st.session_state.get('hoja_auxiliar'),
            _solo_descuadrados(st.session_state.get('hoja_descuadres'))
        )

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
