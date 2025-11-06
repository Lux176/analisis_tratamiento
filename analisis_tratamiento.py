# --- IMPORTACIONES NECESARIAS ---
import streamlit as st
import pandas as pd
import numpy as np
import io
import os
from datetime import datetime
from unidecode import unidecode

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="Entorno de Tratamiento de Datos",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🧠",
)

# --- ESTILOS CSS OSCUROS ---
st.markdown("""
    <style>
        body { background-color: #7b7d81; color: #fafafa; }
        .stApp { background-color: #949599; }
        div[data-testid="stSidebar"] {
            background-color: #cccdcf;
        }
        h1, h2, h3, h4, h5 {
            color: #00b4d8;
        }
        .stButton>button {
            background-color: #0077b6;
            color: white;
            border-radius: 10px;
            padding: 10px 20px;
            border: none;
        }
        .stButton>button:hover {
            background-color: #00b4d8;
            color: black;
        }
    </style>
""", unsafe_allow_html=True)

# --- GIF DE BIENVENIDA (DESAPARECE EN 3s) ---
GIF_URL = "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExM28yOTZ1Zmg0cG4wem14ZmNuM3YzcjFydG5pdTZreHVtZjIwYWRhaoZlcD12MV9naWZzX3NlYXJjaCZjdD1n/tIeCLkB8geYtW/giphy.gif"
st.markdown(
    f"""
    <div id="gif-container" style="text-align: center;">
        <img src="{GIF_URL}" alt="Cargando..." width="300">
    </div>
    <script>
        setTimeout(function(){{
            var el = document.getElementById('gif-container');
            if (el) {{
                el.style.display = 'none';
            }}
        }}, 3000);
    </script>
    """,
    unsafe_allow_html=True
)

# --- VARIABLES GLOBALES ---
if "original_df" not in st.session_state:
    st.session_state.original_df = None
if "processed_df" not in st.session_state:
    st.session_state.processed_df = None
if "log" not in st.session_state:
    st.session_state.log = []

# --- FUNCIÓN DE LOG ---
def add_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.log.append(f"[{timestamp}] {message}")

# --- FUNCIÓN DE CARGA ---
def cargar_archivo(archivo):
    extension = archivo.name.split(".")[-1].lower()
    if extension in ["xlsx", "xls"]:
        df = pd.read_excel(archivo)
    elif extension == "csv":
        df = pd.read_csv(archivo)
    elif extension == "txt":
        df = pd.read_csv(archivo, delimiter="\t")
    elif extension == "ods":
        df = pd.read_excel(archivo, engine="odf")
    else:
        st.error("⚠️ Formato no soportado.")
        return None
    add_log(f"Archivo cargado: {archivo.name}")
    return df

# --- FUNCIÓN DE RESTAURACIÓN ---
def restaurar_archivo():
    if st.session_state.original_df is not None:
        st.session_state.processed_df = st.session_state.original_df.copy()
        add_log("Archivo restaurado al estado original.")
        st.success("✅ Archivo restaurado exitosamente.")
    else:
        st.warning("⚠️ No hay archivo cargado para restaurar.")

# --- FUNCIÓN ADICIONAL: PREVISUALIZACIÓN MEJORADA DE FECHAS ---
def mostrar_info_fechas(df, fecha_columna):
    """
    Muestra información detallada sobre la columna de fechas
    """
    try:
        # Crear una copia para análisis
        df_temp = df.copy()
        df_temp['fecha_convertida'] = pd.to_datetime(df_temp[fecha_columna], errors='coerce')
        
        fechas_validas = df_temp['fecha_convertida'].notna().sum()
        fechas_invalidas = df_temp['fecha_convertida'].isna().sum()
        total_registros = len(df_temp)
        
        info_text = f"""
        **📊 Información de fechas:**
        - ✅ Válidas: {fechas_validas} ({fechas_validas/total_registros*100:.1f}%)
        - ❌ Inválidas: {fechas_invalidas} ({fechas_invalidas/total_registros*100:.1f}%)
        """
        
        if fechas_validas > 0:
            fecha_min = df_temp['fecha_convertida'].min()
            fecha_max = df_temp['fecha_convertida'].max()
            info_text += f"\n- 📅 Rango: {fecha_min.strftime('%Y-%m-%d')} a {fecha_max.strftime('%Y-%m-%d')}"
        
        return info_text, fechas_invalidas
        
    except Exception as e:
        return f"Error al analizar fechas: {e}", 0

# --- FUNCIÓN MEJORADA: FILTRADO POR FECHAS PARA DESCARGA ---
def aplicar_filtro_fechas_descarga(df, fecha_columna, filtro_tipo, año=None, mes=None, fecha_inicio=None, fecha_fin=None):
    """
    Aplica filtro de fechas solo para la descarga sin modificar el DataFrame original
    """
    if df is None or df.empty:
        st.warning("⚠️ No hay datos para filtrar.")
        return df
    
    # Verificar que la columna de fecha existe
    if fecha_columna not in df.columns:
        st.error(f"❌ La columna '{fecha_columna}' no existe en el dataset.")
        return df
    
    # Convertir a datetime manejando errores
    try:
        df_filtrado = df.copy()
        
        # Crear columna temporal para el filtrado
        df_filtrado['fecha_temporal'] = pd.to_datetime(df_filtrado[fecha_columna], errors='coerce')
        
        # Verificar si hay valores nulos después de la conversión
        nulos_count = df_filtrado['fecha_temporal'].isna().sum()
        total_registros = len(df_filtrado)
        
        if nulos_count > 0:
            st.warning(f"⚠️ {nulos_count} de {total_registros} registros no pudieron convertirse a fecha y serán excluidos")
            
        # Filtrar solo los registros con fechas válidas
        df_filtrado = df_filtrado.dropna(subset=['fecha_temporal'])
        
        if len(df_filtrado) == 0:
            st.error("❌ No hay registros con fechas válidas después de la conversión.")
            return df
            
    except Exception as e:
        st.error(f"❌ Error al procesar fechas: {e}")
        return df
    
    # Aplicar filtros según el tipo seleccionado
    registros_validos = len(df_filtrado)
    
    if filtro_tipo == "año" and año:
        df_filtrado = df_filtrado[df_filtrado['fecha_temporal'].dt.year == año]
        st.success(f"✅ Descargando datos del año {año}. Registros: {registros_validos} → {len(df_filtrado)}")
        
    elif filtro_tipo == "mes" and mes:
        df_filtrado = df_filtrado[df_filtrado['fecha_temporal'].dt.month == mes]
        nombre_mes = datetime(2023, mes, 1).strftime("%B")
        st.success(f"✅ Descargando datos del mes {nombre_mes}. Registros: {registros_validos} → {len(df_filtrado)}")
        
    elif filtro_tipo == "rango" and fecha_inicio and fecha_fin:
        fecha_inicio_dt = pd.to_datetime(fecha_inicio)
        fecha_fin_dt = pd.to_datetime(fecha_fin)
        df_filtrado = df_filtrado[
            (df_filtrado['fecha_temporal'] >= fecha_inicio_dt) & 
            (df_filtrado['fecha_temporal'] <= fecha_fin_dt)
        ]
        st.success(f"✅ Descargando datos del rango {fecha_inicio} a {fecha_fin}. Registros: {registros_validos} → {len(df_filtrado)}")
    
    # Eliminar la columna temporal antes de retornar
    df_filtrado = df_filtrado.drop(columns=['fecha_temporal'])
    return df_filtrado

# --- FUNCIONES DE TRATAMIENTO COMPLETAMENTE CORREGIDAS ---
def aplicar_tratamientos(df, opciones, protegidas):
    df_tratado = df.copy()
    add_log("Inicio de tratamiento de datos...")

    # 1. Eliminar duplicados
    if "Eliminar duplicados" in opciones:
        duplicados_antes = len(df_tratado)
        df_tratado.drop_duplicates(inplace=True)
        duplicados_eliminados = duplicados_antes - len(df_tratado)
        add_log(f"Duplicados eliminados: {duplicados_eliminados} registros.")

    # 2. Eliminar espacios extra - ENFOQUE COMPLETAMENTE NUEVO
    if "Eliminar espacios extra" in opciones:
        columnas_procesadas = 0
        for col in df_tratado.select_dtypes(include=["object"]).columns:
            if col not in protegidas:
                # ENFOQUE DIRECTO Y ROBUSTO
                # Primero asegurarnos de que todos los valores sean strings
                df_tratado[col] = df_tratado[col].astype(str)
                # Aplicar strip() directamente a toda la serie
                df_tratado[col] = df_tratado[col].str.strip()
                # También eliminar múltiples espacios internos si es necesario
                df_tratado[col] = df_tratado[col].str.replace(r'\s+', ' ', regex=True)
                columnas_procesadas += 1
                
                # VERIFICACIÓN EN TIEMPO REAL - mostrar ejemplos
                if len(df_tratado) > 0:
                    ejemplo = df_tratado[col].iloc[0]
                    # Mostrar el primer ejemplo para verificar
                    if columnas_procesadas == 1:  # Solo para la primera columna procesada
                        st.sidebar.info(f"🔍 Ejemplo columna '{col}': '{ejemplo}'")
        add_log(f"Espacios extra eliminados en {columnas_procesadas} columnas.")

    # 3. Normalizar encabezados
    if "Normalizar encabezados" in opciones:
        nuevos_nombres = {}
        for col in df_tratado.columns:
            nuevo_nombre = unidecode(str(col).strip().lower().replace(" ", "_").replace("-", "_"))
            nuevos_nombres[col] = nuevo_nombre
        df_tratado.rename(columns=nuevos_nombres, inplace=True)
        add_log("Encabezados normalizados.")

    # 4. Rellenar valores nulos - MEJORADO
    if "Rellenar nulos" in opciones:
        nulos_rellenados = 0
        for col in df_tratado.columns:
            if col not in protegidas:
                nulos_antes = df_tratado[col].isna().sum()
                if nulos_antes > 0:
                    if df_tratado[col].dtype == "object":
                        df_tratado[col].fillna("N/A", inplace=True)
                    elif pd.api.types.is_numeric_dtype(df_tratado[col]):
                        # Para coordenadas, verificar por nombre de columna
                        if any(term in col.lower() for term in ['lat', 'lon', 'long', 'latitude', 'longitude']):
                            # Para coordenadas, usar 0 o mantener nulos
                            df_tratado[col].fillna(0, inplace=True)
                        else:
                            df_tratado[col].fillna(df_tratado[col].median(), inplace=True)
                    nulos_rellenados += nulos_antes
        add_log(f"Valores nulos rellenados: {nulos_rellenados} valores.")

    # 5. Eliminar acentos - ENFOQUE MÁS ROBUSTO
    if "Eliminar acentos" in opciones:
        columnas_procesadas = 0
        for col in df_tratado.select_dtypes(include=["object"]).columns:
            if col not in protegidas:
                # Asegurar que todos sean strings
                df_tratado[col] = df_tratado[col].astype(str)
                # Aplicar unidecode directamente a toda la serie
                df_tratado[col] = df_tratado[col].apply(unidecode)
                columnas_procesadas += 1
                
                # VERIFICACIÓN EN TIEMPO REAL
                if len(df_tratado) > 0 and columnas_procesadas == 1:
                    ejemplo = df_tratado[col].iloc[0]
                    st.sidebar.info(f"🔍 Ejemplo sin acentos '{col}': '{ejemplo}'")
        add_log(f"Acentos eliminados en {columnas_procesadas} columnas.")

    # 6. Convertir texto a minúsculas - ENFOQUE DIRECTO
    if "Texto a minúsculas" in opciones:
        columnas_procesadas = 0
        for col in df_tratado.select_dtypes(include=["object"]).columns:
            if col not in protegidas:
                # Aplicar lower() directamente a toda la serie
                df_tratado[col] = df_tratado[col].str.lower()
                columnas_procesadas += 1
        add_log(f"Texto convertido a minúsculas en {columnas_procesadas} columnas.")

    # 7. Eliminar outliers - MEJORADO
    if "Eliminar outliers" in opciones:
        registros_antes = len(df_tratado)
        columnas_numericas = df_tratado.select_dtypes(include=[np.number]).columns
        columnas_procesadas = 0
        
        for col in columnas_numericas:
            if col not in protegidas:
                # Excluir columnas que parecen coordenadas
                if not any(term in col.lower() for term in ['lat', 'lon', 'long', 'latitude', 'longitude']):
                    # Verificar que hay suficientes datos
                    if len(df_tratado[col].dropna()) > 10:
                        q1 = df_tratado[col].quantile(0.25)
                        q3 = df_tratado[col].quantile(0.75)
                        iqr = q3 - q1
                        if iqr > 0:
                            low = q1 - 1.5 * iqr
                            high = q3 + 1.5 * iqr
                            mask = (df_tratado[col] >= low) & (df_tratado[col] <= high)
                            df_tratado = df_tratado[mask | df_tratado[col].isna()]
                            columnas_procesadas += 1
        
        registros_eliminados = registros_antes - len(df_tratado)
        add_log(f"Outliers eliminados: {registros_eliminados} registros en {columnas_procesadas} columnas.")

    add_log("Tratamiento de datos completado.")
    
    # Mostrar resumen de cambios
    cambios_info = f"""
    **📊 Resumen del tratamiento:**
    - Registros finales: {len(df_tratado)}
    - Columnas finales: {len(df_tratado.columns)}
    - Tratamientos aplicados: {len(opciones)}
    """
    st.info(cambios_info)
    
    st.success("✅ Tratamiento completado con éxito.")
    return df_tratado

# --- INTERFAZ PRINCIPAL ---
st.title("🧠 Entorno de Tratamiento de Datos Profesional")

archivo = st.sidebar.file_uploader("📂 Cargar archivo", type=["xlsx", "xls", "csv", "txt", "ods"])

if archivo:
    df = cargar_archivo(archivo)
    if df is not None:
        st.session_state.original_df = df.copy()
        st.session_state.processed_df = df.copy()

        st.subheader("👁️ Vista preliminar del archivo")
        st.dataframe(df.head(10), use_container_width=True)

        # --- OPCIONES DE PROCESAMIENTO ---
        columnas = list(df.columns)
        st.sidebar.subheader("🛡️ Protección y eliminación")
        protegidas = st.sidebar.multiselect("Seleccionar columnas protegidas", columnas)
        eliminar = st.sidebar.multiselect("Eliminar columnas", [c for c in columnas if c not in protegidas])

        # --- ELIMINACIÓN DE COLUMNAS ---
        if eliminar:
            st.session_state.processed_df.drop(columns=eliminar, inplace=True)
            add_log(f"Columnas eliminadas: {', '.join(eliminar)}")
            st.success(f"🗑️ Columnas eliminadas: {', '.join(eliminar)}")

        # --- OPCIONES DE TRATAMIENTO ---
        st.sidebar.subheader("⚙️ Tratamientos disponibles")
        opciones = st.sidebar.multiselect(
            "Selecciona tratamientos a aplicar:",
            ["Eliminar duplicados", "Eliminar espacios extra", "Normalizar encabezados",
             "Rellenar nulos", "Eliminar acentos", "Texto a minúsculas", "Eliminar outliers"]
        )

        # --- BOTONES DE ACCIÓN ---
        if st.sidebar.button("🚀 Iniciar tratamiento"):
            st.session_state.processed_df = aplicar_tratamientos(
                st.session_state.processed_df, opciones, protegidas
            )

        if st.sidebar.button("🔄 Restaurar archivo original"):
            restaurar_archivo()

        # --- SECCIÓN DE DESCARGA CON FILTRO DE FECHAS INTEGRADO ---
        st.sidebar.subheader("📤 Exportar resultados")
        
        if st.session_state.processed_df is not None:
            # Mostrar información general
            total_registros = len(st.session_state.processed_df)
            st.sidebar.info(f"📊 Total de registros procesados: {total_registros}")
            
            # Opciones de formato
            formato = st.sidebar.selectbox("Formato de exportación", ["xlsx", "csv", "json", "parquet"])
            
            # Opción de filtro para descarga
            st.sidebar.subheader("📅 Filtro para descarga")
            aplicar_filtro = st.sidebar.radio(
                "¿Deseas aplicar filtro de fechas?",
                ["Descargar sin filtro", "Aplicar filtro de fechas"]
            )
            
            df_para_descargar = st.session_state.processed_df.copy()
            mensaje_descarga = f"Descargando {total_registros} registros"
            
            if aplicar_filtro == "Aplicar filtro de fechas":
                # Selección de columna de fecha
                fecha_columna = st.sidebar.selectbox(
                    "Seleccionar columna de fecha:",
                    st.session_state.processed_df.columns,
                    help="Selecciona la columna que contiene las fechas"
                )
                
                if fecha_columna:
                    # Mostrar información de fechas
                    info_fechas, invalidas = mostrar_info_fechas(st.session_state.processed_df, fecha_columna)
                    st.sidebar.info(info_fechas)
                    
                    # Tipo de filtro
                    filtro_tipo = st.sidebar.selectbox(
                        "Tipo de filtro:",
                        ["año", "mes", "rango"],
                        help="Selecciona el tipo de filtro a aplicar"
                    )
                    
                    año = None
                    mes = None
                    fecha_inicio = None
                    fecha_fin = None
                    
                    if filtro_tipo == "año":
                        # Obtener años disponibles
                        try:
                            df_temp = st.session_state.processed_df.copy()
                            df_temp['fecha_temp'] = pd.to_datetime(df_temp[fecha_columna], errors='coerce')
                            df_temp = df_temp.dropna(subset=['fecha_temp'])
                            años_disponibles = sorted(df_temp['fecha_temp'].dt.year.dropna().unique())
                            if años_disponibles:
                                año = st.sidebar.selectbox("Seleccionar año:", años_disponibles)
                            else:
                                st.sidebar.warning("No se pudieron obtener años válidos")
                        except Exception as e:
                            st.sidebar.warning(f"No se pueden obtener años: {e}")
                    
                    elif filtro_tipo == "mes":
                        mes = st.sidebar.selectbox(
                            "Seleccionar mes:",
                            range(1, 13),
                            format_func=lambda x: datetime(2023, x, 1).strftime("%B")
                        )
                    
                    elif filtro_tipo == "rango":
                        col1, col2 = st.sidebar.columns(2)
                        with col1:
                            fecha_inicio = st.date_input("Fecha inicio:")
                        with col2:
                            fecha_fin = st.date_input("Fecha fin:")
                    
                    # Aplicar filtro para la descarga
                    if año or mes or (fecha_inicio and fecha_fin):
                        df_para_descargar = aplicar_filtro_fechas_descarga(
                            st.session_state.processed_df,
                            fecha_columna,
                            filtro_tipo,
                            año,
                            mes,
                            fecha_inicio,
                            fecha_fin
                        )
                        mensaje_descarga = f"Descargando {len(df_para_descargar)} registros (con filtro aplicado)"
            
            # Botón de descarga
            st.sidebar.markdown("---")
            st.sidebar.info(mensaje_descarga)
            
            if st.sidebar.button("💾 Generar archivo para descarga"):
                if len(df_para_descargar) > 0:
                    buffer = io.BytesIO()
                    
                    if formato == "xlsx":
                        df_para_descargar.to_excel(buffer, index=False)
                        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        filename = "datos_procesados.xlsx"
                    elif formato == "csv":
                        df_para_descargar.to_csv(buffer, index=False)
                        mime = "text/csv"
                        filename = "datos_procesados.csv"
                    elif formato == "json":
                        df_para_descargar.to_json(buffer, orient="records")
                        mime = "application/json"
                        filename = "datos_procesados.json"
                    else:
                        df_para_descargar.to_parquet(buffer, index=False)
                        mime = "application/octet-stream"
                        filename = "datos_procesados.parquet"

                    # Crear el botón de descarga
                    st.sidebar.download_button(
                        label="⬇️ Descargar archivo",
                        data=buffer.getvalue(),
                        file_name=filename,
                        mime=mime,
                        key="descarga_principal"
                    )
                    
                    # Registrar en el log
                    if aplicar_filtro == "Aplicar filtro de fechas":
                        add_log(f"Archivo exportado como {formato} con {len(df_para_descargar)} registros (filtro aplicado)")
                    else:
                        add_log(f"Archivo exportado como {formato} con {len(df_para_descargar)} registros")
                else:
                    st.sidebar.error("❌ No hay datos para descargar")

        # --- DESCARGA DEL LOG ---
        st.sidebar.subheader("📝 Registro de operaciones")
        if st.sidebar.button("🧾 Descargar log de operaciones"):
            log_txt = "\n".join(st.session_state.log)
            st.sidebar.download_button(
                "⬇️ Descargar log",
                data=log_txt,
                file_name="registro_operaciones.txt",
                mime="text/plain"
            )

        # --- BOTÓN MENÚ PRINCIPAL ---
        if st.sidebar.button("🏠 Volver al menú principal"):
            st.session_state.original_df = None
            st.session_state.processed_df = None
            st.session_state.log = []
            st.experimental_rerun()

else:
    st.info("👈 Carga un archivo para comenzar el tratamiento de datos.")
