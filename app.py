import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

# Configuración de la página
st.set_page_config(
    page_title="Comparador DIAN",
    page_icon="📊",
    layout="wide"
)

# Función para procesar Token DIAN
def procesar_token_dian(df):
    # Aquí irán los filtros específicos
    return df

# Función para procesar Libro Auxiliar
def procesar_libro_auxiliar(df):
    try:
        # Filtrar registros que empiecen con FC o NC o contengan PILA
        mascara = (df['Documento'].str.startswith('FC') | 
                  df['Documento'].str.startswith('NC') | 
                  df['Documento'].str.contains('PILA', na=False))
        df_filtrado = df[mascara].copy()
        return df_filtrado
    except Exception as e:
        st.error(f"Error al procesar Libro Auxiliar: {str(e)}")
        return None

# Función para realizar el cruce de datos
def cruzar_datos(df_token, df_libro):
    try:
        # Aquí irá la lógica de cruce
        # Por ahora retornamos un DataFrame de ejemplo
        return pd.DataFrame({
            'Registro': ['Ejemplo'],
            'Estado': ['Pendiente validación']
        })
    except Exception as e:
        st.error(f"Error en el cruce de datos: {str(e)}")
        return None

def main():
    st.title('🔄 Comparador Token DIAN y Libro Auxiliar')
    
    # Sidebar con instrucciones
    with st.sidebar:
        st.header("Instrucciones")
        st.write("""
        1. Carga el archivo Token DIAN
        2. Carga el archivo Libro Auxiliar
        3. Haz clic en 'Procesar' para realizar la comparación
        4. Descarga los resultados
        """)
    
    # Sección de carga de archivos
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Token DIAN")
        archivo_token = st.file_uploader("Cargar archivo Token DIAN", type=['xlsx', 'xls'])
        
    with col2:
        st.subheader("Libro Auxiliar")
        archivo_libro = st.file_uploader("Cargar archivo Libro Auxiliar", type=['xlsx', 'xls'])

    if archivo_token and archivo_libro:
        try:
            # Cargar archivos
            df_token = pd.read_excel(archivo_token)
            df_libro = pd.read_excel(archivo_libro)
            
            # Mostrar información inicial
            st.subheader("Resumen de archivos cargados")
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Token DIAN:")
                st.write(f"- Registros: {len(df_token)}")
                
            with col2:
                st.write("Libro Auxiliar:")
                st.write(f"- Registros: {len(df_libro)}")
            
            if st.button('Procesar archivos'):
                with st.spinner('Procesando...'):
                    # Procesar archivos
                    df_token_proc = procesar_token_dian(df_token)
                    df_libro_proc = procesar_libro_auxiliar(df_libro)
                    
                    if df_token_proc is not None and df_libro_proc is not None:
                        # Realizar cruce
                        resultados = cruzar_datos(df_token_proc, df_libro_proc)
                        
                        if resultados is not None:
                            st.success("¡Procesamiento completado!")
                            
                            # Mostrar resultados
                            st.subheader("Resultados")
                            st.dataframe(resultados)
                            
                            # Botón de descarga
                            buffer = BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                resultados.to_excel(writer, index=False)
                            
                            st.download_button(
                                label="📥 Descargar resultados",
                                data=buffer.getvalue(),
                                file_name="resultados_validacion.xlsx",
                                mime="application/vnd.ms-excel"
                            )

        except Exception as e:
            st.error(f'Error al procesar los archivos: {str(e)}')

if __name__ == "__main__":
    main()