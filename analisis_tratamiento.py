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
        body { background-color: #0e1117; color: #fafafa; }
        .stApp { background-color: #0e1117; }
        div[data-testid="stSidebar"] {
            background-color: #1c1f26;
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
GIF_URL = "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExM28yOTZ1Zmg0cG4wem14ZmNuM3YzcjFydG5pdTZreHVtZjIwYWRhbyZlcD12MV9naWZzX3NlYXJjaCZjdD1n/tIeCLkB8geYtW/giphy.gif"
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
        
        st.sidebar.info(info_text)
        
        # Mostrar ejemplos de valores problemáticos
        if fechas_invalidas > 0:
            ejemplos_invalidos = df_temp[df_temp['fecha_convertida'].isna()][fecha_columna].head(3).tolist()
            st.sidebar.warning(f"**Valores problemáticos:** {ejemplos_invalidos}")
            
    except Exception as e:
        st.sidebar.error(f"Error al analizar fechas: {e}")

# --- NUEVA FUNCIÓN: FILTRADO POR FECHAS MEJORADA ---
def filtrar_por_fechas(df, fecha_columna=None, filtro_tipo=None, año=None, mes=None, fecha_inicio=None, fecha_fin=None):
    """
    Filtra el DataFrame por criterios de fecha
    
    Parámetros:
    - df: DataFrame a filtrar
    - fecha_columna: nombre de la columna de fecha
    - filtro_tipo: tipo de filtro ('año', 'mes', 'rango')
    - año: año específico a filtrar
    - mes: mes específico a filtrar
    - fecha_inicio: fecha de inicio para rango
    - fecha_fin: fecha de fin para rango
    
    Retorna:
    - DataFrame filtrado
    """
    
    if df is None or df.empty:
        st.warning("⚠️ No hay datos para filtrar.")
        return df
    
    # Verificar que la columna de fecha existe
    if fecha_columna not in df.columns:
        st.error(f"❌ La columna '{fecha_columna}' no existe en el dataset.")
        return df
    
    # Convertir a datetime manejando errores y fechas antiguas
    try:
        df_filtrado = df.copy()
        
        # Primero intentar conversión directa
        df_filtrado['fecha_temporal'] = pd.to_datetime(df_filtrado[fecha_columna], errors='coerce')
        
        # Verificar si hay valores nulos después de la conversión
        nulos_count = df_filtrado['fecha_temporal'].isna().sum()
        total_registros = len(df_filtrado)
        
        if nulos_count > 0:
            st.warning(f"⚠️ {nulos_count} de {total_registros} registros no pudieron convertirse a fecha y serán excluidos del filtrado")
            
        # Filtrar solo los registros con fechas válidas
        df_filtrado = df_filtrado.dropna(subset=['fecha_temporal'])
        
        # Verificar que quedan registros después del filtrado
        if len(df_filtrado) == 0:
            st.error("❌ No hay registros con fechas válidas después de la conversión.")
            return df
            
    except Exception as e:
        st.error(f"❌ Error al procesar la columna '{fecha_columna}': {e}")
        # Intentar método alternativo para fechas problemáticas
        try:
            st.info("🔄 Intentando método alternativo de conversión...")
            df_filtrado = df.copy()
            # Usar dayfirst=True para formato día/mes/año
            df_filtrado['fecha_temporal'] = pd.to_datetime(
                df_filtrado[fecha_columna], 
                errors='coerce', 
                dayfirst=True
            )
            df_filtrado = df_filtrado.dropna(subset=['fecha_temporal'])
            
            if len(df_filtrado) == 0:
                st.error("❌ No se pudieron convertir las fechas con ningún método.")
                return df
                
        except Exception as e2:
            st.error(f"❌ Error crítico en conversión de fechas: {e2}")
            return df
    
    # Aplicar filtros según el tipo seleccionado
    registros_originales = len(df)
    registros_validos = len(df_filtrado)
    
    if registros_validos < registros_originales:
        st.info(f"📊 Usando {registros_validos} de {registros_originales} registros (fechas válidas)")
    
    if filtro_tipo == "año" and año:
        df_filtrado = df_filtrado[df_filtrado['fecha_temporal'].dt.year == año]
        add_log(f"Filtrado por año: {año}")
        st.success(f"✅ Filtrado por año {año}. Registros: {registros_validos} → {len(df_filtrado)}")
        
    elif filtro_tipo == "mes" and mes:
        df_filtrado = df_filtrado[df_filtrado['fecha_temporal'].dt.month == mes]
        nombre_mes = datetime(2023, mes, 1).strftime("%B")
        add_log(f"Filtrado por mes: {nombre_mes}")
        st.success(f"✅ Filtrado por mes {nombre_mes}. Registros: {registros_validos} → {len(df_filtrado)}")
        
    elif filtro_tipo == "rango" and fecha_inicio and fecha_fin:
        fecha_inicio_dt = pd.to_datetime(fecha_inicio)
        fecha_fin_dt = pd.to_datetime(fecha_fin)
        df_filtrado = df_filtrado[
            (df_filtrado['fecha_temporal'] >= fecha_inicio_dt) & 
            (df_filtrado['fecha_temporal'] <= fecha_fin_dt)
        ]
        add_log(f"Filtrado por rango: {fecha_inicio} a {fecha_fin}")
        st.success(f"✅ Filtrado por rango {fecha_inicio} a {fecha_fin}. Registros: {registros_validos} → {len(df_filtrado)}")
    
    else:
        st.info("ℹ️ No se aplicó ningún filtro de fecha.")
        # Mantener la columna temporal para futuros filtros
        df_filtrado = df_filtrado.drop(columns=['fecha_temporal'])
        return df_filtrado
    
    # Eliminar la columna temporal antes de retornar
    df_filtrado = df_filtrado.drop(columns=['fecha_temporal'])
    return df_filtrado

# --- FUNCIONES DE TRATAMIENTO ---
def aplicar_tratamientos(df, opciones, protegidas):
    df_tratado = df.copy()
    add_log("Inicio de tratamiento de datos...")

    # 1. Eliminar duplicados
    if "Eliminar duplicados" in opciones:
        df_tratado.drop_duplicates(inplace=True)
        add_log("Duplicados eliminados.")

    # 2. Eliminar espacios
    if "Eliminar espacios extra" in opciones:
        for col in df_tratado.select_dtypes(include="object"):
            if col not in protegidas:
                df_tratado[col] = df_tratado[col].astype(str).str.strip()
        add_log("Espacios extra eliminados.")

    # 3. Normalizar encabezados
    if "Normalizar encabezados" in opciones:
        df_tratado.columns = [unidecode(c.strip().lower().replace(" ", "_")) for c in df_tratado.columns]
        add_log("Encabezados normalizados.")

    # 4. Rellenar valores nulos
    if "Rellenar nulos" in opciones:
        for col in df_tratado.columns:
            if col not in protegidas:
                if df_tratado[col].dtype == "O":
                    df_tratado[col].fillna("N/A", inplace=True)
                else:
                    df_tratado[col].fillna(df_tratado[col].median(), inplace=True)
        add_log("Valores nulos rellenados.")

    # 5. Eliminar acentos
    if "Eliminar acentos" in opciones:
        for col in df_tratado.select_dtypes(include="object"):
            if col not in protegidas:
                df_tratado[col] = df_tratado[col].apply(lambda x: unidecode(str(x)))
        add_log("Acentos eliminados.")

    # 6. Convertir texto a minúsculas
    if "Texto a minúsculas" in opciones:
        for col in df_tratado.select_dtypes(include="object"):
            if col not in protegidas:
                df_tratado[col] = df_tratado[col].str.lower()
        add_log("Texto convertido a minúsculas.")

    # 7. Eliminar outliers (numéricos)
    if "Eliminar outliers" in opciones:
        for col in df_tratado.select_dtypes(include=[np.number]):
            if col not in protegidas:
                q1, q3 = df_tratado[col].quantile([0.25, 0.75])
                iqr = q3 - q1
                low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                df_tratado = df_tratado[(df_tratado[col] >= low) & (df_tratado[col] <= high)]
        add_log("Outliers eliminados.")

    add_log("Tratamiento de datos completado.")
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

        # --- NUEVA SECCIÓN: FILTRADO POR FECHAS (DESPUÉS DEL TRATAMIENTO) ---
        st.sidebar.subheader("📅 Filtrado por Fechas")
        st.sidebar.info("ℹ️ Aplica este filtro después del tratamiento")
        
        if st.session_state.processed_df is not None:
            # Permitir selección manual de todas las columnas
            fecha_columna = st.sidebar.selectbox(
                "Seleccionar columna de fecha:",
                st.session_state.processed_df.columns,
                help="Selecciona manualmente la columna que contiene las fechas"
            )
            
            if fecha_columna:
                # Mostrar información detallada de las fechas
                mostrar_info_fechas(st.session_state.processed_df, fecha_columna)
                
                # Mostrar primeros valores para referencia
                col_info = st.session_state.processed_df[fecha_columna].head(3).tolist()
                st.sidebar.info(f"**Primeros valores:** {col_info}")
            
            filtro_tipo = st.sidebar.selectbox(
                "Tipo de filtro:",
                ["ninguno", "año", "mes", "rango"],
                help="Selecciona el tipo de filtro a aplicar"
            )
            
            año = None
            mes = None
            fecha_inicio = None
            fecha_fin = None
            
            if filtro_tipo == "año":
                # Intentar obtener años disponibles de la columna seleccionada
                try:
                    df_temp = st.session_state.processed_df.copy()
                    df_temp['fecha_temp'] = pd.to_datetime(df_temp[fecha_columna], errors='coerce')
                    df_temp = df_temp.dropna(subset=['fecha_temp'])
                    años_disponibles = sorted(df_temp['fecha_temp'].dt.year.dropna().unique())
                    if años_disponibles:
                        año = st.sidebar.selectbox("Seleccionar año:", años_disponibles)
                    else:
                        st.sidebar.warning("No se pudieron obtener años válidos de esta columna")
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
            
            if st.sidebar.button("🔍 Aplicar filtro de fechas"):
                if filtro_tipo != "ninguno":
                    st.session_state.processed_df = filtrar_por_fechas(
                        st.session_state.processed_df,
                        fecha_columna=fecha_columna,
                        filtro_tipo=filtro_tipo,
                        año=año,
                        mes=mes,
                        fecha_inicio=fecha_inicio,
                        fecha_fin=fecha_fin
                    )
                else:
                    st.sidebar.warning("Selecciona un tipo de filtro")

        # --- DESCARGA DE RESULTADOS ---
        st.sidebar.subheader("📤 Exportar resultados")
        formato = st.sidebar.selectbox("Formato de exportación", ["xlsx", "csv", "json", "parquet"])
        if st.sidebar.button("💾 Descargar archivo procesado"):
            buffer = io.BytesIO()
            df_export = st.session_state.processed_df

            if formato == "xlsx":
                df_export.to_excel(buffer, index=False)
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                filename = "datos_procesados.xlsx"
            elif formato == "csv":
                df_export.to_csv(buffer, index=False)
                mime = "text/csv"
                filename = "datos_procesados.csv"
            elif formato == "json":
                df_export.to_json(buffer, orient="records")
                mime = "application/json"
                filename = "datos_procesados.json"
            else:
                df_export.to_parquet(buffer, index=False)
                mime = "application/octet-stream"
                filename = "datos_procesados.parquet"

            st.download_button("⬇️ Descargar", buffer.getvalue(), file_name=filename, mime=mime)
            add_log(f"Archivo exportado como {formato}")

        # --- DESCARGA DEL LOG ---
        if st.sidebar.button("🧾 Descargar log de operaciones"):
            log_txt = "\n".join(st.session_state.log)
            st.download_button(
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
