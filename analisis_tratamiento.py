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
