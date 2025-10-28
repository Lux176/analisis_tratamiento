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

def aplicar_tratamiento_automatico(df):
    """Aplica tratamiento automático a los datos"""
    df_tratado = df.copy()
    transformaciones = []
    
    # Identificar columnas de texto (excluyendo geográficas)
    columnas_texto = [col for col in df_tratado.columns if not es_columna_geografica(col)]
    
    # Aplicar limpieza de texto a columnas no geográficas
    for columna in columnas_texto:
        if df_tratado[columna].dtype == 'object':
            df_tratado[columna] = df_tratado[columna].apply(limpiar_texto)
            transformaciones.append(f"Limpieza de texto aplicada a: {columna}")
    
    # Manejar valores nulos en columnas no geográficas
    columnas_con_nulos = df_tratado.columns[df_tratado.isnull().any()].tolist()
    for columna in columnas_con_nulos:
        if not es_columna_geografica(columna):
            df_tratado[columna].fillna('null', inplace=True)
            transformaciones.append(f"Valores nulos marcados como 'null' en: {columna}")
    
    return df_tratado, transformaciones

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
        elif file_type == 'png':
            mime_type = 'image/png'
            ext = 'png'
        elif file_type == 'html':
            mime_type = 'text/html'
            ext = 'html'
        else:
            mime_type = 'text/plain'
            ext = 'txt'
        
        href = f'<a href="data:{mime_type};base64,{b64}" download="{file_name}.{ext}" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-align: center; text-decoration: none; display: inline-block; border-radius: 5px; margin: 5px;">📥 {file_label}</a>'
        return href
    except Exception as e:
        return f'<p style="color: red;">Error al generar enlace: {str(e)}</p>'

def guardar_visualizacion(fig, nombre, formato='png'):
    """Guarda visualización en formato especificado"""
    try:
        if formato == 'png':
            path = os.path.join(tempfile.gettempdir(), f'{nombre}.png')
            fig.write_image(path)
        elif formato == 'html':
            path = os.path.join(tempfile.gettempdir(), f'{nombre}.html')
            fig.write_html(path)
        return path
    except Exception as e:
        st.error(f"Error al guardar visualización: {str(e)}")
        return None

# --- INTERFAZ PRINCIPAL ---

def main():
    st.title("🔧 Sistema de Tratamiento de Datos")
    st.markdown("---")
    
    # Inicializar session state
    if 'etapa_actual' not in st.session_state:
        st.session_state.etapa_actual = 1  # 1: Carga, 2: Tratamiento, 3: Análisis, 4: Exportación
    if 'df_original' not in st.session_state:
        st.session_state.df_original = None
    if 'df_procesado' not in st.session_state:
        st.session_state.df_procesado = None
    if 'transformaciones' not in st.session_state:
        st.session_state.transformaciones = []
    if 'tratamiento_aplicado' not in st.session_state:
        st.session_state.tratamiento_aplicado = False
    if 'visualizaciones_generadas' not in st.session_state:
        st.session_state.visualizaciones_generadas = []
    
    # Barra de progreso
    col_prog1, col_prog2, col_prog3, col_prog4 = st.columns(4)
    with col_prog1:
        st.metric("Paso 1", "📁 Carga", 
                 delta="Activo" if st.session_state.etapa_actual == 1 else "Completado" if st.session_state.etapa_actual > 1 else "Pendiente",
                 delta_color="normal" if st.session_state.etapa_actual == 1 else "off")
    with col_prog2:
        st.metric("Paso 2", "🛠️ Tratamiento", 
                 delta="Activo" if st.session_state.etapa_actual == 2 else "Completado" if st.session_state.etapa_actual > 2 else "Pendiente",
                 delta_color="normal" if st.session_state.etapa_actual == 2 else "off")
    with col_prog3:
        st.metric("Paso 3", "📊 Análisis", 
                 delta="Activo" if st.session_state.etapa_actual == 3 else "Completado" if st.session_state.etapa_actual > 3 else "Pendiente",
                 delta_color="normal" if st.session_state.etapa_actual == 3 else "off")
    with col_prog4:
        st.metric("Paso 4", "💾 Exportar", 
                 delta="Activo" if st.session_state.etapa_actual == 4 else "Pendiente",
                 delta_color="normal" if st.session_state.etapa_actual == 4 else "off")
    
    # ETAPA 1: CARGA DE DATOS
    if st.session_state.etapa_actual == 1:
        st.header("📁 Paso 1: Carga de Datos")
        
        with st.container():
            col1, col2 = st.columns([2, 1])
            with col1:
                uploaded_file = st.file_uploader(
                    "Sube tu archivo de datos",
                    type=['csv', 'xlsx', 'xls', 'parquet'],
                    help="Formatos soportados: CSV, Excel, Parquet"
                )
            
            with col2:
                st.info("""
                **📋 Formatos aceptados:**
                - CSV (.csv)
                - Excel (.xlsx, .xls)
                - Parquet (.parquet)
                """)
        
        if uploaded_file is not None:
            try:
                # Leer archivo
                with st.spinner("Cargando archivo..."):
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file)
                    elif uploaded_file.name.endswith('.parquet'):
                        df = pd.read_parquet(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)
                    
                    st.session_state.df_original = df.copy()
                    st.session_state.df_procesado = df.copy()
                    st.session_state.transformaciones = []
                    st.session_state.visualizaciones_generadas = []
                    
                    st.success(f"✅ Archivo cargado: {uploaded_file.name}")
                    st.info(f"📊 Dimensiones: {df.shape[0]} filas × {df.shape[1]} columnas")
                    
                    # Mostrar vista previa
                    with st.expander("👀 Vista previa de los datos (primeras 10 filas)"):
                        st.dataframe(df.head(10), use_container_width=True)
                    
                    # Información básica
                    col_info1, col_info2, col_info3 = st.columns(3)
                    with col_info1:
                        st.metric("Total Filas", df.shape[0])
                    with col_info2:
                        st.metric("Total Columnas", df.shape[1])
                    with col_info3:
                        st.metric("Valores Nulos", df.isnull().sum().sum())
                    
                    # Botón para avanzar al tratamiento
                    if st.button("🚀 Aplicar Tratamiento Automático", type="primary", use_container_width=True):
                        st.session_state.etapa_actual = 2
                        st.rerun()
                        
            except Exception as e:
                st.error(f"❌ Error al cargar el archivo: {str(e)}")
    
    # ETAPA 2: TRATAMIENTO DE DATOS
    elif st.session_state.etapa_actual == 2:
        st.header("🛠️ Paso 2: Tratamiento de Datos")
        
        if st.session_state.df_original is not None:
            df_original = st.session_state.df_original
            
            with st.container():
                st.subheader("🔍 Análisis Inicial del Dataset")
                
                # Mostrar problemas detectados
                columnas_con_nulos = df_original.columns[df_original.isnull().any()].tolist()
                columnas_geograficas = [col for col in df_original.columns if es_columna_geografica(col)]
                columnas_texto = [col for col in df_original.columns if df_original[col].dtype == 'object' and not es_columna_geografica(col)]
                
                col_anal1, col_anal2, col_anal3 = st.columns(3)
                with col_anal1:
                    st.metric("Columnas con Nulos", len(columnas_con_nulos))
                with col_anal2:
                    st.metric("Columnas de Texto", len(columnas_texto))
                with col_anal3:
                    st.metric("Columnas Geográficas", len(columnas_geograficas))
                
                if columnas_geograficas:
                    st.info(f"🛡️ **Columnas geográficas protegidas:** {', '.join(columnas_geograficas)}")
            
            # Aplicar tratamiento automático
            if not st.session_state.tratamiento_aplicado:
                with st.spinner("Aplicando tratamiento automático..."):
                    df_tratado, transformaciones = aplicar_tratamiento_automatico(df_original)
                    st.session_state.df_procesado = df_tratado
                    st.session_state.transformaciones = transformaciones
                    st.session_state.tratamiento_aplicado = True
            
            # OPCIONES AVANZADAS DE TRATAMIENTO
            st.subheader("⚙️ Opciones Avanzadas de Tratamiento")
            
            col_adv1, col_adv2 = st.columns(2)
            
            with col_adv1:
                st.write("**Eliminar Columnas**")
                columnas_a_eliminar = st.multiselect(
                    "Selecciona columnas para eliminar:",
                    options=st.session_state.df_procesado.columns.tolist(),
                    help="Las columnas seleccionadas serán eliminadas del dataset"
                )
                
                if st.button("🗑️ Eliminar Columnas Seleccionadas", key="eliminar_columnas_btn"):
                    if columnas_a_eliminar:
                        df_actual = st.session_state.df_procesado.copy()
                        df_reducido = df_actual.drop(columns=columnas_a_eliminar)
                        st.session_state.df_procesado = df_reducido
                        st.session_state.transformaciones.append(f"Columnas eliminadas: {', '.join(columnas_a_eliminar)}")
                        st.success(f"✅ Columnas eliminadas: {', '.join(columnas_a_eliminar)}")
                        st.rerun()
                    else:
                        st.warning("⚠️ Selecciona al menos una columna para eliminar")
            
            with col_adv2:
                st.write("**Eliminar Filas Duplicadas**")
                if st.button("🔍 Eliminar Filas Duplicadas", key="eliminar_duplicados_btn"):
                    df_actual = st.session_state.df_procesado.copy()
                    filas_antes = len(df_actual)
                    df_sin_duplicados = df_actual.drop_duplicates()
                    filas_despues = len(df_sin_duplicados)
                    
                    st.session_state.df_procesado = df_sin_duplicados
                    st.session_state.transformaciones.append(f"Eliminadas {filas_antes - filas_despues} filas duplicadas")
                    st.success(f"✅ Se eliminaron {filas_antes - filas_despues} filas duplicadas")
                    st.rerun()
            
            # Mostrar resultados del tratamiento
            st.subheader("✅ Tratamiento Aplicado")
            
            # Transformaciones aplicadas
            with st.expander("📋 Ver transformaciones aplicadas"):
                for i, transformacion in enumerate(st.session_state.transformaciones, 1):
                    st.write(f"{i}. {transformacion}")
            
            # Comparación antes/después
            col_comp1, col_comp2 = st.columns(2)
            with col_comp1:
                st.write("**Antes del tratamiento:**")
                st.dataframe(st.session_state.df_original.head(5), use_container_width=True)
            with col_comp2:
                st.write("**Después del tratamiento:**")
                st.dataframe(st.session_state.df_procesado.head(5), use_container_width=True)
            
            # Métricas de mejora
            st.subheader("📈 Resumen de Mejoras")
            col_mej1, col_mej2, col_mej3 = st.columns(3)
            with col_mej1:
                nulos_antes = st.session_state.df_original.isnull().sum().sum()
                nulos_despues = st.session_state.df_procesado.isnull().sum().sum()
                st.metric("Reducción de Nulos", f"{nulos_antes - nulos_despues}")
            with col_mej2:
                st.metric("Transformaciones", len(st.session_state.transformaciones))
            with col_mej3:
                st.metric("Columnas Protegidas", len(columnas_geograficas))
            
            # Botones de navegación
            col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 1])
            with col_btn1:
                if st.button("⬅️ Volver a Carga", use_container_width=True):
                    st.session_state.etapa_actual = 1
                    st.rerun()
            with col_btn2:
                if st.button("🔄 Re-aplicar Tratamiento", use_container_width=True):
                    st.session_state.tratamiento_aplicado = False
                    st.rerun()
            with col_btn3:
                if st.button("Continuar a Análisis ➡️", type="primary", use_container_width=True):
                    st.session_state.etapa_actual = 3
                    st.rerun()
    
    # ETAPA 3: ANÁLISIS Y VISUALIZACIÓN
    elif st.session_state.etapa_actual == 3:
        st.header("📊 Paso 3: Análisis y Visualización")
        
        if st.session_state.df_procesado is not None:
            df = st.session_state.df_procesado
            
            # Pestañas de análisis
            tab1, tab2, tab3 = st.tabs(["🔍 Calidad de Datos", "📈 Visualización Avanzada", "📄 Reportes"])
            
            with tab1:
                st.subheader("Análisis de Calidad de Datos")
                
                # Resumen de nulos
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
                st.subheader("Tipos de Datos")
                tipos_datos = df.dtypes.reset_index()
                tipos_datos.columns = ['Columna', 'Tipo de Dato']
                st.dataframe(tipos_datos, use_container_width=True)
            
            with tab2:
                st.subheader("📊 Visualización Avanzada")
                
                if not df.empty:
                    # Selección de tipo de gráfico
                    col_viz1, col_viz2 = st.columns(2)
                    
                    with col_viz1:
                        tipo_grafico = st.selectbox(
                            "Tipo de gráfico:",
                            ["Barras", "Dispersión", "Líneas", "Histograma", "Boxplot", "Heatmap", "Torta"]
                        )
                    
                    with col_viz2:
                        # Opciones de descarga
                        formato_descarga = st.multiselect(
                            "Formatos de descarga:",
                            ["PNG", "HTML"],
                            default=["PNG"]
                        )
                    
                    # Configuración del gráfico según tipo
                    if tipo_grafico == "Barras":
                        col_conf1, col_conf2 = st.columns(2)
                        with col_conf1:
                            eje_x = st.selectbox("Eje X:", df.columns.tolist())
                        with col_conf2:
                            eje_y = st.selectbox("Eje Y:", df.select_dtypes(include=[np.number]).columns.tolist())
                        
                        color_col = st.selectbox("Color (opcional):", [None] + df.columns.tolist())
                        
                        if st.button("Generar Gráfico de Barras"):
                            fig = px.bar(df, x=eje_x, y=eje_y, color=color_col, title=f"{eje_y} por {eje_x}")
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Descarga
                            if formato_descarga:
                                nombre_grafico = f"barras_{eje_x}_{eje_y}"
                                for formato in formato_descarga:
                                    if formato == "PNG":
                                        path = guardar_visualizacion(fig, nombre_grafico, 'png')
                                        if path:
                                            st.markdown(get_download_link(path, f"Descargar {nombre_grafico}.png", "png"), unsafe_allow_html=True)
                                            st.session_state.visualizaciones_generadas.append(f"Gráfico de barras: {nombre_grafico}.png")
                                    elif formato == "HTML":
                                        path = guardar_visualizacion(fig, nombre_grafico, 'html')
                                        if path:
                                            st.markdown(get_download_link(path, f"Descargar {nombre_grafico}.html", "html"), unsafe_allow_html=True)
                                            st.session_state.visualizaciones_generadas.append(f"Gráfico de barras: {nombre_grafico}.html")
                    
                    elif tipo_grafico == "Dispersión":
                        col_conf1, col_conf2 = st.columns(2)
                        with col_conf1:
                            eje_x = st.selectbox("Eje X:", df.select_dtypes(include=[np.number]).columns.tolist())
                        with col_conf2:
                            eje_y = st.selectbox("Eje Y:", df.select_dtypes(include=[np.number]).columns.tolist())
                        
                        color_col = st.selectbox("Color (opcional):", [None] + df.columns.tolist())
                        size_col = st.selectbox("Tamaño (opcional):", [None] + df.select_dtypes(include=[np.number]).columns.tolist())
                        
                        if st.button("Generar Gráfico de Dispersión"):
                            fig = px.scatter(df, x=eje_x, y=eje_y, color=color_col, size=size_col, 
                                           title=f"Dispersión: {eje_y} vs {eje_x}")
                            st.plotly_chart(fig, use_container_width=True)
                            
                            if formato_descarga:
                                nombre_grafico = f"dispersion_{eje_x}_{eje_y}"
                                for formato in formato_descarga:
                                    if formato == "PNG":
                                        path = guardar_visualizacion(fig, nombre_grafico, 'png')
                                        if path:
                                            st.markdown(get_download_link(path, f"Descargar {nombre_grafico}.png", "png"), unsafe_allow_html=True)
                                            st.session_state.visualizaciones_generadas.append(f"Gráfico de dispersión: {nombre_grafico}.png")
                                    elif formato == "HTML":
                                        path = guardar_visualizacion(fig, nombre_grafico, 'html')
                                        if path:
                                            st.markdown(get_download_link(path, f"Descargar {nombre_grafico}.html", "html"), unsafe_allow_html=True)
                                            st.session_state.visualizaciones_generadas.append(f"Gráfico de dispersión: {nombre_grafico}.html")
                    
                    elif tipo_grafico == "Heatmap":
                        columnas_numericas = df.select_dtypes(include=[np.number]).columns.tolist()
                        if len(columnas_numericas) > 1:
                            fig = px.imshow(
                                df[columnas_numericas].corr(),
                                title="Matriz de Correlación",
                                aspect="auto"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            if formato_descarga:
                                nombre_grafico = "heatmap_correlacion"
                                for formato in formato_descarga:
                                    if formato == "PNG":
                                        path = guardar_visualizacion(fig, nombre_grafico, 'png')
                                        if path:
                                            st.markdown(get_download_link(path, f"Descargar {nombre_grafico}.png", "png"), unsafe_allow_html=True)
                                            st.session_state.visualizaciones_generadas.append(f"Heatmap: {nombre_grafico}.png")
                                    elif formato == "HTML":
                                        path = guardar_visualizacion(fig, nombre_grafico, 'html')
                                        if path:
                                            st.markdown(get_download_link(path, f"Descargar {nombre_grafico}.html", "html"), unsafe_allow_html=True)
                                            st.session_state.visualizaciones_generadas.append(f"Heatmap: {nombre_grafico}.html")
                        else:
                            st.warning("Se necesitan al menos 2 columnas numéricas para el heatmap")
                    
                    # Más tipos de gráficos pueden agregarse aquí...
            
            with tab3:
                st.subheader("Reportes de Calidad")
                
                if st.button("Generar Reporte Completo", type="primary"):
                    with st.spinner("Generando reporte..."):
                        reporte = generar_reporte_calidad(df, st.session_state.df_original)
                        
                        # Mostrar resumen
                        col_rep1, col_rep2, col_rep3, col_rep4 = st.columns(4)
                        with col_rep1:
                            st.metric("Filas Originales", reporte['metadata']['filas_originales'])
                        with col_rep2:
                            st.metric("Filas Finales", reporte['metadata']['filas_finales'])
                        with col_rep3:
                            st.metric("Columnas Originales", reporte['metadata']['columnas_originales'])
                        with col_rep4:
                            st.metric("Columnas Finales", reporte['metadata']['columnas_finales'])
            
            # Botones de navegación
            col_nav1, col_nav2, col_nav3 = st.columns([1, 1, 1])
            with col_nav1:
                if st.button("⬅️ Volver a Tratamiento", use_container_width=True):
                    st.session_state.etapa_actual = 2
                    st.rerun()
            with col_nav2:
                if st.button("🔄 Actualizar Análisis", use_container_width=True):
                    st.rerun()
            with col_nav3:
                if st.button("Continuar a Exportación ➡️", type="primary", use_container_width=True):
                    st.session_state.etapa_actual = 4
                    st.rerun()
    
    # ETAPA 4: EXPORTACIÓN
    elif st.session_state.etapa_actual == 4:
        st.header("💾 Paso 4: Exportar Datos Procesados")
        
        if st.session_state.df_procesado is not None:
            df = st.session_state.df_procesado
            
            st.success("✅ Tus datos están listos para exportar")
            
            # Resumen final
            col_sum1, col_sum2, col_sum3 = st.columns(3)
            with col_sum1:
                st.metric("Filas Procesadas", len(df))
            with col_sum2:
                st.metric("Columnas Procesadas", len(df.columns))
            with col_sum3:
                st.metric("Transformaciones", len(st.session_state.transformaciones))
            
            # Opciones de exportación
            st.subheader("📤 Formatos de Exportación")
            col_exp1, col_exp2, col_exp3, col_exp4 = st.columns(4)
            
            with col_exp1:
                # CSV
                csv_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.csv')
                df.to_csv(csv_path, index=False, encoding='utf-8')
                st.markdown(get_download_link(csv_path, "Descargar CSV", "csv"), unsafe_allow_html=True)
            
            with col_exp2:
                # Excel
                excel_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.xlsx')
                df.to_excel(excel_path, index=False)
                st.markdown(get_download_link(excel_path, "Descargar Excel", "excel"), unsafe_allow_html=True)
            
            with col_exp3:
                # Parquet con manejo de errores
                try:
                    df_parquet = preparar_dataframe_parquet(df)
                    parquet_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.parquet')
                    df_parquet.to_parquet(parquet_path, index=False)
                    st.markdown(get_download_link(parquet_path, "Descargar Parquet", "parquet"), unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ Error al exportar a Parquet: {str(e)}")
            
            with col_exp4:
                # JSON
                try:
                    json_path = os.path.join(tempfile.gettempdir(), 'datos_procesados.json')
                    df.to_json(json_path, orient='records', indent=2, force_ascii=False)
                    st.markdown(get_download_link(json_path, "Descargar JSON", "json"), unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ Error al exportar a JSON: {str(e)}")
            
            # Exportar reporte de transformaciones (INCLUYENDO VISUALIZACIONES)
            st.subheader("📄 Reporte Completo de Proceso")
            
            if st.button("📋 Generar Reporte Completo", type="primary"):
                reporte_txt_path = os.path.join(tempfile.gettempdir(), 'reporte_completo_proceso.txt')
                with open(reporte_txt_path, 'w', encoding='utf-8') as f:
                    f.write("REPORTE COMPLETO DEL PROCESO DE TRATAMIENTO DE DATOS\n")
                    f.write("=" * 60 + "\n\n")
                    f.write(f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Archivo original: {len(st.session_state.df_original)} filas × {len(st.session_state.df_original.columns)} columnas\n")
                    f.write(f"Archivo procesado: {len(df)} filas × {len(df.columns)} columnas\n\n")
                    
                    f.write("TRANSFORMACIONES APLICADAS:\n")
                    f.write("-" * 30 + "\n")
                    for i, transformacion in enumerate(st.session_state.transformaciones, 1):
                        f.write(f"{i}. {transformacion}\n")
                    
                    f.write("\nVISUALIZACIONES GENERADAS:\n")
                    f.write("-" * 30 + "\n")
                    if st.session_state.visualizaciones_generadas:
                        for i, visualizacion in enumerate(st.session_state.visualizaciones_generadas, 1):
                            f.write(f"{i}. {visualizacion}\n")
                    else:
                        f.write("No se generaron visualizaciones\n")
                    
                    f.write("\nESTADÍSTICAS FINALES:\n")
                    f.write("-" * 25 + "\n")
                    f.write(f"Total de filas: {len(df)}\n")
                    f.write(f"Total de columnas: {len(df.columns)}\n")
                    f.write(f"Valores nulos restantes: {df.isnull().sum().sum()}\n")
                    f.write(f"Memoria utilizada: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB\n")
                
                st.markdown(get_download_link(reporte_txt_path, "Descargar Reporte Completo", "txt"), unsafe_allow_html=True)
                st.success("✅ Reporte completo generado")
            
            # Botones finales
            col_fin1, col_fin2, col_fin3 = st.columns([1, 1, 1])
            with col_fin1:
                if st.button("⬅️ Volver a Análisis", use_container_width=True):
                    st.session_state.etapa_actual = 3
                    st.rerun()
            with col_fin2:
                if st.button("🔄 Nuevo Análisis", use_container_width=True):
                    # Resetear todo
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]
                    st.rerun()
            with col_fin3:
                st.info("🎉 ¡Proceso completado!")
    
    # PANTALLA INICIAL
    else:
        st.markdown("""
        ## 🚀 Bienvenido al Sistema de Tratamiento de Datos
        
        ### 📋 Flujo de Trabajo:
        
        **1. 📁 Carga de Datos**
        - Sube tu archivo (CSV, Excel, Parquet)
        - Vista previa inmediata
        - Análisis inicial automático
        
        **2. 🛠️ Tratamiento Automático**
        - Limpieza de texto (minúsculas, sin acentos)
        - Manejo de valores nulos
        - Protección de columnas geográficas
        - Eliminación de columnas y duplicados
        
        **3. 📊 Análisis y Visualización**
        - Análisis de calidad de datos
        - Gráficos interactivos avanzados
        - Múltiples formatos de descarga
        - Reportes detallados
        
        **4. 💾 Exportación**
        - Múltiples formatos (CSV, Excel, Parquet, JSON)
        - Reportes completos con visualizaciones
        - Datos listos para usar
        
        ### 👆 Para comenzar:
        **Haz clic en 'Cargar Archivo' en la barra lateral** ←
        """)

if __name__ == "__main__":
    main()
