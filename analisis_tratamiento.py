import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re
import unicodedata
import tempfile
import os
import base64
import io
from io import BytesIO
import json
import hashlib

# Configuración de la página
st.set_page_config(
    page_title="Sistema de Tratamiento de Datos",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- FUNCIONES DE UTILIDAD ---

def limpiar_texto(texto):
    """Normaliza texto a minúsculas y sin acentos"""
    if pd.isna(texto) or texto is None:
        return texto
    if not isinstance(texto, str):
        return texto
    
    texto_limpio = unicodedata.normalize('NFD', str(texto))\
                              .encode('ascii', 'ignore')\
                              .decode('utf-8')\
                              .lower()\
                              .strip()
    return texto_limpio

def es_columna_geografica(nombre_columna):
    """Identifica si una columna contiene datos geográficos"""
    patrones_geo = ['lat', 'lon', 'long', 'latitude', 'longitude', 'coord', 'x', 'y']
    nombre_limpio = str(nombre_columna).lower()
    return any(patron in nombre_limpio for patron in patrones_geo)

def preparar_dataframe_parquet(df):
    """Prepara el DataFrame para exportación a Parquet manejando tipos de datos problemáticos"""
    df_parquet = df.copy()
    
    # Convertir tipos de datos problemáticos
    for columna in df_parquet.columns:
        # Manejar tipos mixed
        if df_parquet[columna].dtype == 'object':
            try:
                # Intentar convertir a string
                df_parquet[columna] = df_parquet[columna].astype(str)
            except:
                # Si falla, convertir a string manejando errores
                df_parquet[columna] = df_parquet[columna].apply(lambda x: str(x) if pd.notna(x) else None)
        
        # Manejar datetime problems
        elif 'datetime' in str(df_parquet[columna].dtype):
            df_parquet[columna] = pd.to_datetime(df_parquet[columna], errors='coerce')
    
    return df_parquet

def generar_reporte_calidad(df, df_original):
    """Genera un reporte completo de calidad de datos"""
    reporte = {
        'metadata': {
            'filas_originales': len(df_original),
            'filas_finales': len(df),
            'columnas_originales': len(df_original.columns),
            'columnas_finales': len(df.columns),
            'fecha_generacion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        'estadisticas_por_columna': {},
        'problemas_detectados': [],
        'transformaciones_aplicadas': []
    }
    
    # Estadísticas por columna
    for columna in df.columns:
        stats = {
            'tipo_dato': str(df[columna].dtype),
            'valores_no_nulos': df[columna].count(),
            'valores_nulos': df[columna].isnull().sum(),
            'porcentaje_nulos': round((df[columna].isnull().sum() / len(df)) * 100, 2),
            'valores_unicos': df[columna].nunique(),
            'ejemplos_valores': df[columna].dropna().head(3).tolist()
        }
        
        if pd.api.types.is_numeric_dtype(df[columna]):
            stats.update({
                'min': float(df[columna].min()) if not df[columna].isnull().all() else None,
                'max': float(df[columna].max()) if not df[columna].isnull().all() else None,
                'media': float(df[columna].mean()) if not df[columna].isnull().all() else None,
                'mediana': float(df[columna].median()) if not df[columna].isnull().all() else None
            })
        
        reporte['estadisticas_por_columna'][columna] = stats
    
    # Detectar problemas
    for columna in df.columns:
        nulos_pct = reporte['estadisticas_por_columna'][columna]['porcentaje_nulos']
        if nulos_pct > 50:
            reporte['problemas_detectados'].append(f"Columna '{columna}': {nulos_pct}% de valores nulos")
        
        if df[columna].nunique() == 1:
            reporte['problemas_detectados'].append(f"Columna '{columna}': Solo tiene un valor único")
    
    return reporte

def get_download_link(file_path, file_label, file_type):
    """Genera enlace de descarga"""
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        b64 = base64.b64encode(data).decode()
        file_name = os.path.basename(file_path)
        
        if file_type == 'excel':
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            ext = 'xlsx'
        elif file_type == 'csv':
            mime_type = 'text/csv'
            ext = 'csv'
        elif file_type == 'json':
            mime_type = 'application/json'
            ext = 'json'
        elif file_type == 'parquet':
            mime_type = 'application/octet-stream'
            ext = 'parquet'
        else:
            mime_type = 'text/plain'
            ext = 'txt'
        
        href = f'<a href="data:{mime_type};base64,{b64}" download="{file_name}.{ext}" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-align: center; text-decoration: none; display: inline-block; border-radius: 5px; margin: 5px;">📥 {file_label}</a>'
        return href
    except Exception as e:
        return f'<p style="color: red;">Error al generar enlace: {str(e)}</p>'

# --- INTERFAZ PRINCIPAL ---

def main():
    st.title("🔧 Sistema de Tratamiento de Datos")
    st.markdown("---")
    
    # Inicializar session state
    if 'df_original' not in st.session_state:
        st.session_state.df_original = None
    if 'df_procesado' not in st.session_state:
        st.session_state.df_procesado = None
    if 'transformaciones' not in st.session_state:
        st.session_state.transformaciones = []
    
    # Sidebar - Carga de datos
    with st.sidebar:
        st.header("📁 Carga de Datos")
        uploaded_file = st.file_uploader(
            "Sube tu archivo de datos",
            type=['csv', 'xlsx', 'xls', 'parquet'],
            help="Formatos soportados: CSV, Excel, Parquet"
        )
        
        if uploaded_file is not None:
            try:
                # Leer archivo
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                elif uploaded_file.name.endswith('.parquet'):
                    df = pd.read_parquet(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.session_state.df_original = df.copy()
                st.session_state.df_procesado = df.copy()
                st.session_state.transformaciones = []
                
                st.success(f"✅ Archivo cargado: {uploaded_file.name}")
                st.info(f"📊 Dimensiones: {df.shape[0]} filas × {df.shape[1]} columnas")
                
            except Exception as e:
                st.error(f"❌ Error al cargar el archivo: {str(e)}")
    
    # Panel principal
    if st.session_state.df_original is not None:
        df = st.session_state.df_procesado
        
        # Pestañas principales
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📋 Vista de Datos", 
            "🔍 Análisis de Calidad", 
            "🛠️ Tratamiento", 
            "📊 Visualización",
            "📄 Reportes",
            "💾 Exportar"
        ])
        
        with tab1:
            st.header("Vista de Datos")
            
            col1, col2 = st.columns([1, 3])
            with col1:
                filas_a_mostrar = st.slider("Filas a mostrar:", 5, 100, 10)
                mostrar_original = st.checkbox("Mostrar datos originales")
            
            with col2:
                if mostrar_original:
                    st.dataframe(st.session_state.df_original.head(filas_a_mostrar), use_container_width=True)
                    st.caption("Datos Originales")
                else:
                    st.dataframe(df.head(filas_a_mostrar), use_container_width=True)
                    st.caption("Datos Procesados")
            
            # Información básica
            col3, col4, col5 = st.columns(3)
            with col3:
                st.metric("Total Filas", df.shape[0])
            with col4:
                st.metric("Total Columnas", df.shape[1])
            with col5:
                st.metric("Valores Nulos", df.isnull().sum().sum())
        
        with tab2:
            st.header("Análisis de Calidad de Datos")
            
            # Resumen de nulos
            st.subheader("📊 Distribución de Valores Nulos")
            nulos_por_columna = df.isnull().sum()
            if nulos_por_columna.sum() > 0:
                fig_nulos = px.bar(
                    x=nulos_por_columna.index,
                    y=nulos_por_columna.values,
                    title="Valores Nulos por Columna",
                    labels={'x': 'Columnas', 'y': 'Cantidad de Nulos'}
                )
                fig_nulos.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_nulos, use_container_width=True)
            else:
                st.success("🎉 No se encontraron valores nulos en el dataset")
            
            # Tipos de datos
            st.subheader("🔧 Tipos de Datos")
            tipos_datos = df.dtypes.reset_index()
            tipos_datos.columns = ['Columna', 'Tipo de Dato']
            st.dataframe(tipos_datos, use_container_width=True)
            
            # Estadísticas descriptivas
            st.subheader("📈 Estadísticas Descriptivas")
            if st.checkbox("Mostrar estadísticas completas"):
                st.dataframe(df.describe(include='all'), use_container_width=True)
        
        with tab3:
            st.header("🛠️ Tratamiento de Datos")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Limpieza de Texto")
                columnas_texto = [col for col in df.columns if not es_columna_geografica(col)]
                
                if columnas_texto:
                    columnas_a_limpiar = st.multiselect(
                        "Selecciona columnas para limpiar texto:",
                        options=columnas_texto,
                        help="Se aplicará minúsculas y eliminarán acentos"
                    )
                    
                    if st.button("Aplicar Limpieza de Texto", key="limpiar_texto_btn"):
                        with st.spinner("Limpiando texto..."):
                            df_limpio = df.copy()
                            for columna in columnas_a_limpiar:
                                df_limpio[columna] = df_limpio[columna].apply(limpiar_texto)
                                st.session_state.transformaciones.append(f"Limpieza de texto en columna: {columna}")
                            
                            st.session_state.df_procesado = df_limpio
                            st.success("✅ Limpieza de texto aplicada correctamente")
                            st.rerun()
                else:
                    st.info("No se encontraron columnas de texto para limpiar")
            
            with col2:
                st.subheader("Manejo de Valores Nulos")
                columnas_con_nulos = df.columns[df.isnull().any()].tolist()
                
                if columnas_con_nulos:
                    st.write("Columnas con valores nulos:")
                    for col in columnas_con_nulos:
                        nulos_count = df[col].isnull().sum()
                        st.write(f"- **{col}**: {nulos_count} nulos ({round(nulos_count/len(df)*100, 2)}%)")
                    
                    if st.button("Marcar Nulos como 'null'", key="marcar_nulos_btn"):
                        with st.spinner("Procesando valores nulos..."):
                            df_sin_nulos = df.copy()
                            for columna in columnas_con_nulos:
                                if not es_columna_geografica(columna):
                                    df_sin_nulos[columna].fillna('null', inplace=True)
                                    st.session_state.transformaciones.append(f"Valores nulos marcados como 'null' en: {columna}")
                            
                            st.session_state.df_procesado = df_sin_nulos
                            st.success("✅ Valores nulos procesados correctamente")
                            st.rerun()
                else:
                    st.success("🎉 No hay valores nulos en el dataset")
            
            # Columnas geográficas protegidas
            st.subheader("🛡️ Columnas Geográficas Protegidas")
            columnas_geo = [col for col in df.columns if es_columna_geografica(col)]
            if columnas_geo:
                st.info(f"**Columnas identificadas como geográficas:** {', '.join(columnas_geo)}")
                st.caption("Estas columnas no serán modificadas en las operaciones de limpieza de texto")
            else:
                st.info("No se identificaron columnas geográficas")
            
            # Transformaciones adicionales
            st.subheader("⚙️ Transformaciones Adicionales")
            col3, col4 = st.columns(2)
            
            with col3:
                if st.button("Eliminar Filas Duplicadas", key="eliminar_duplicados_btn"):
                    filas_antes = len(df)
                    df_sin_duplicados = df.drop_duplicates()
                    filas_despues = len(df_sin_duplicados)
                    
                    st.session_state.df_procesado = df_sin_duplicados
                    st.session_state.transformaciones.append(f"Eliminadas {filas_antes - filas_despues} filas duplicadas")
                    st.success(f"✅ Se eliminaron {filas_antes - filas_despues} filas duplicadas")
                    st.rerun()
            
            with col4:
                columnas_a_eliminar = st.multiselect(
                    "Selecciona columnas para eliminar:",
                    options=df.columns.tolist()
                )
                if st.button("Eliminar Columnas Seleccionadas", key="eliminar_columnas_btn"):
                    df_reducido = df.drop(columns=columnas_a_eliminar)
                    st.session_state.df_procesado = df_reducido
                    st.session_state.transformaciones.append(f"Columnas eliminadas: {', '.join(columnas_a_eliminar)}")
                    st.success(f"✅ Columnas eliminadas: {', '.join(columnas_a_eliminar)}")
                    st.rerun()
        
        with tab4:
            st.header("📊 Visualización de Datos")
            
            if not df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Histograma para columnas numéricas
                    columnas_numericas = df.select_dtypes(include=[np.number]).columns.tolist()
                    if columnas_numericas:
                        columna_hist = st.selectbox("Selecciona columna para histograma:", columnas_numericas)
                        if columna_hist:
                            fig_hist = px.histogram(df, x=columna_hist, title=f"Distribución de {columna_hist}")
                            st.plotly_chart(fig_hist, use_container_width=True)
                
                with col2:
                    # Gráfico de barras para categóricas
                    columnas_categoricas = df.select_dtypes(include=['object']).columns.tolist()
                    if columnas_categoricas:
                        columna_bar = st.selectbox("Selecciona columna para gráfico de barras:", columnas_categoricas)
                        if columna_bar:
                            conteo_valores = df[columna_bar].value_counts().head(10)
                            fig_bar = px.bar(
                                x=conteo_valores.index, 
                                y=conteo_valores.values,
                                title=f"Top 10 Valores en {columna_bar}",
                                labels={'x': columna_bar, 'y': 'Frecuencia'}
                            )
                            st.plotly_chart(fig_bar, use_container_width=True)
                
                # Heatmap de correlación
                if len(columnas_numericas) > 1:
                    st.subheader("🔥 Heatmap de Correlación")
                    fig_corr = px.imshow(
                        df[columnas_numericas].corr(),
                        title="Matriz de Correlación",
                        aspect="auto"
                    )
                    st.plotly_chart(fig_corr, use_container_width=True)
        
        with tab5:
            st.header("📄 Reportes de Calidad")
            
            if st.button("Generar Reporte Completo", key="generar_reporte_btn"):
                with st.spinner("Generando reporte..."):
                    reporte = generar_reporte_calidad(df, st.session_state.df_original)
                    
                    # Mostrar resumen
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Filas Originales", reporte['metadata']['filas_originales'])
                    with col2:
                        st.metric("Filas Finales", reporte['metadata']['filas_finales'])
                    with col3:
                        st.metric("Columnas Originales", reporte['metadata']['columnas_originales'])
                    with col4:
                        st.metric("Columnas Finales", reporte['metadata']['columnas_finales'])
                    
                    # Transformaciones aplicadas
                    st.subheader("🔄 Transformaciones Aplicadas")
                    if st.session_state.transformaciones:
                        for i, transformacion in enumerate(st.session_state.transformaciones, 1):
                            st.write(f"{i}. {transformacion}")
                    else:
                        st.info("No se han aplicado transformaciones")
                    
                    # Problemas detectados
                    st.subheader("⚠️ Problemas Detectados")
                    if reporte['problemas_detectados']:
                        for problema in reporte['problemas_detectados']:
                            st.error(problema)
                    else:
                        st.success("No se detectaron problemas críticos")
                    
                    # Exportar reporte
                    st.subheader("💾 Exportar Reporte")
                    reporte_json = json.dumps(reporte, indent=2, ensure_ascii=False)
                    
                    col5, col6 = st.columns(2)
                    with col5:
                        # JSON
                        reporte_path = os.path.join(tempfile.gettempdir(), 'reporte_calidad.json')
                        with open(reporte_path, 'w', encoding='utf-8') as f:
                            f.write(reporte_json)
                        st.markdown(get_download_link(reporte_path, "Descargar Reporte JSON", "json"), unsafe_allow_html=True)
                    
                    with col6:
                        # TXT
                        reporte_txt_path = os.path.join(tempfile.gettempdir(), 'reporte_calidad.txt')
                        with open(reporte_txt_path, 'w', encoding='utf-8') as f:
                            f.write("REPORTE DE CALIDAD DE DATOS\n")
                            f.write("=" * 50 + "\n\n")
                            f.write(f"Fecha de generación: {reporte['metadata']['fecha_generacion']}\n")
                            f.write(f"Filas originales: {reporte['metadata']['filas_originales']}\n")
                            f.write(f"Filas finales: {reporte['metadata']['filas_finales']}\n\n")
                            
                            f.write("TRANSFORMACIONES APLICADAS:\n")
                            for transformacion in st.session_state.transformaciones:
                                f.write(f"- {transformacion}\n")
                            
                            f.write("\nPROBLEMAS DETECTADOS:\n")
                            for problema in reporte['problemas_detectados']:
                                f.write(f"- {problema}\n")
                        
                        st.markdown(get_download_link(reporte_txt_path, "Descargar Reporte TXT", "txt"), unsafe_allow_html=True)
        
        with tab6:
            st.header("💾 Exportar Datos Procesados")
            
            if not df.empty:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    # CSV
                    csv_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.csv')
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    st.markdown(get_download_link(csv_path, "Descargar CSV", "csv"), unsafe_allow_html=True)
                
                with col2:
                    # Excel
                    excel_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.xlsx')
                    df.to_excel(excel_path, index=False)
                    st.markdown(get_download_link(excel_path, "Descargar Excel", "excel"), unsafe_allow_html=True)
                
                with col3:
                    # Parquet con manejo de errores
                    try:
                        df_parquet = preparar_dataframe_parquet(df)
                        parquet_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.parquet')
                        df_parquet.to_parquet(parquet_path, index=False)
                        st.markdown(get_download_link(parquet_path, "Descargar Parquet", "parquet"), unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"❌ Error al exportar a Parquet: {str(e)}")
                        st.info("💡 Intenta exportar en otro formato como CSV o Excel")
                
                with col4:
                    # JSON
                    try:
                        json_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.json')
                        df.to_json(json_path, orient='records', indent=2, force_ascii=False)
                        st.markdown(get_download_link(json_path, "Descargar JSON", "json"), unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"❌ Error al exportar a JSON: {str(e)}")
                
                st.success("✅ Datos listos para exportar en múltiples formatos")
                
                # Resumen de cambios
                st.subheader("📋 Resumen de Cambios")
                cambios_filas = len(st.session_state.df_original) - len(df)
                cambios_columnas = len(st.session_state.df_original.columns) - len(df.columns)
                
                col5, col6 = st.columns(2)
                with col5:
                    st.metric("Cambio en Filas", cambios_filas)
                with col6:
                    st.metric("Cambio en Columnas", cambios_columnas)
            
            else:
                st.warning("No hay datos para exportar")
    
    else:
        # Pantalla de bienvenida
        st.markdown("""
        ## 🚀 Bienvenido al Sistema de Tratamiento de Datos
        
        ### 📋 Funcionalidades Principales:
        
        **🔍 Análisis de Calidad:**
        - Detección de valores nulos
        - Análisis de tipos de datos
        - Estadísticas descriptivas
        - Identificación de problemas
        
        **🛠️ Tratamiento de Datos:**
        - Limpieza automática de texto (minúsculas, sin acentos)
        - Protección de columnas geográficas (lat, lon, etc.)
        - Manejo de valores nulos
        - Eliminación de duplicados
        - Filtrado de columnas
        
        **📊 Visualización:**
        - Gráficos interactivos
        - Histogramas y gráficos de barras
        - Heatmaps de correlación
        - Análisis de distribuciones
        
        **📄 Reportes:**
        - Reportes completos de calidad
        - Documentación de transformaciones
        - Exportación en múltiples formatos
        
        **💾 Exportación:**
        - CSV, Excel, Parquet, JSON
        - Mantenimiento de formatos originales
        - Reportes separados
        
        ### 🎯 Características Especiales:
        - **Protección de datos sensibles** (columnas geográficas)
        - **Interfaz intuitiva** para usuarios técnicos y no técnicos
        - **Flujo completo** desde carga hasta exportación
        - **Procesamiento seguro** sin modificar archivos originales
        
        ### 👆 Para comenzar:
        1. **Sube tu archivo** en la barra lateral ←
        2. **Explora los datos** en las diferentes pestañas
        3. **Aplica tratamientos** según tus necesidades
        4. **Genera reportes** y exporta los resultados
        """)

if __name__ == "__main__":
    main()
