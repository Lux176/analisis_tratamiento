"""
Streamlit App - Entorno profesional de tratamiento de datos (Tema oscuro, moderno)
Características:
- Carga de archivos (.xlsx, .xls, .csv, .txt)
- Vista preliminar
- Selección de columnas protegidas / eliminar
- Múltiples tratamientos activables (normalizar, eliminar acentos, rellenar NA, eliminar duplicados, fechas, outliers, etc.)
- Backup automático, restauración original / última versión
- Exportación a .xlsx, .csv, .json, .parquet
- Log descargable (.txt)
- GIF de bienvenida que desaparece en 3s
- Tema oscuro CSS, mensajes visuales y mini-log en pantalla
"""

import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime
from unidecode import unidecode
import json
import base64

# ---------- Helpers ----------
def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def add_log(msg):
    entry = f"[{now()}] {msg}"
    st.session_state['log_entries'].append(entry)
    # keep a short "recent" view
    st.session_state['recent_log'] = st.session_state['log_entries'][-10:]

def df_preview(df, n=10):
    return df.head(n)

def to_excel_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.save()
    return output.getvalue()

def to_parquet_bytes(df):
    output = io.BytesIO()
    df.to_parquet(output, index=False)
    output.seek(0)
    return output.read()

def to_csv_bytes(df):
    return df.to_csv(index=False).encode('utf-8')

def to_json_bytes(df):
    return df.to_json(orient='records', force_ascii=False).encode('utf-8')

def safe_apply_columnwise(df, cols, fn):
    """Apply fn only to columns in cols and not protected."""
    for c in cols:
        try:
            df[c] = df[c].map(lambda x: fn(x) if pd.notnull(x) else x)
        except Exception:
            # try apply for series
            df[c] = df[c].apply(lambda x: fn(x) if pd.notnull(x) else x)
    return df

# ---------- Treatments ----------
def normalize_headers(df, log=True):
    old = df.columns.tolist()
    new = [unidecode(str(c)).strip().lower().replace(' ', '_') for c in old]
    df.columns = new
    if log: add_log(f"Normalized headers: {old} -> {new}")
    return df

def trim_spaces(df, protected):
    cols = [c for c in df.columns if c not in protected and df[c].dtype == object]
    before = df[cols].applymap(lambda x: len(str(x)) if pd.notnull(x) else 0).sum().sum()
    df[cols] = df[cols].applymap(lambda x: str(x).strip() if pd.notnull(x) else x)
    add_log(f"Trimmed spaces in columns: {cols}")
    return df

def remove_accents(df, protected):
    cols = [c for c in df.columns if c not in protected and df[c].dtype == object]
    df = safe_apply_columnwise(df, cols, lambda x: unidecode(str(x)))
    add_log(f"Removed accents from columns: {cols}")
    return df

def to_lowercase(df, protected):
    cols = [c for c in df.columns if c not in protected and df[c].dtype == object]
    df = safe_apply_columnwise(df, cols, lambda x: str(x).lower())
    add_log(f"Converted to lowercase columns: {cols}")
    return df

def to_uppercase(df, protected):
    cols = [c for c in df.columns if c not in protected and df[c].dtype == object]
    df = safe_apply_columnwise(df, cols, lambda x: str(x).upper())
    add_log(f"Converted to UPPERCASE columns: {cols}")
    return df

def fill_nas(df, protected, strategy='constant', value='N/A'):
    cols = [c for c in df.columns if c not in protected]
    affected = 0
    for c in cols:
        n_before = df[c].isna().sum()
        if n_before > 0:
            if strategy == 'constant':
                df[c] = df[c].fillna(value)
            elif strategy == 'mean':
                if pd.api.types.is_numeric_dtype(df[c]):
                    df[c] = df[c].fillna(df[c].mean())
            elif strategy == 'median':
                if pd.api.types.is_numeric_dtype(df[c]):
                    df[c] = df[c].fillna(df[c].median())
            affected += n_before
    add_log(f"Filled {affected} NA values in columns: {cols} using strategy '{strategy}'")
    return df

def drop_duplicates(df, subset=None):
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    add_log(f"Dropped duplicates. Rows before: {before}, after: {len(df)}")
    return df

def detect_and_parse_dates(df, protected):
    # attempt to parse object columns to datetimes
    parsed_cols = []
    for c in df.columns:
        if c in protected:
            continue
        if df[c].dtype == object:
            try:
                parsed = pd.to_datetime(df[c], errors='coerce', dayfirst=False)
                non_null = parsed.notna().sum()
                if non_null > 0:
                    df[c] = parsed
                    parsed_cols.append((c, non_null))
            except Exception:
                continue
    add_log(f"Parsed date-like columns: {parsed_cols}")
    return df

def convert_numeric(df, protected):
    converted = []
    for c in df.columns:
        if c in protected: continue
        if df[c].dtype == object:
            coerced = pd.to_numeric(df[c], errors='coerce')
            non_null = coerced.notna().sum()
            # if many values converted, accept it
            if non_null > 0:
                df[c] = coerced
                converted.append((c, non_null))
    add_log(f"Converted to numeric (where possible): {converted}")
    return df

def remove_outliers_iqr(df, protected, cols=None, factor=1.5):
    if cols is None:
        cols = [c for c in df.select_dtypes(include=[np.number]).columns if c not in protected]
    removed = 0
    for c in cols:
        q1 = df[c].quantile(0.25)
        q3 = df[c].quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0: continue
        low = q1 - factor * iqr
        high = q3 + factor * iqr
        before = len(df)
        df = df[(df[c] >= low) & (df[c] <= high)]
        removed += before - len(df)
    add_log(f"Removed {removed} rows as outliers on columns: {cols} using IQR factor {factor}")
    return df

def anonymize_columns(df, protected, pattern='****'):
    # naive anonymization: keep first and last char, mask middle for strings and partial digits
    cols = [c for c in df.columns if c not in protected and df[c].dtype == object]
    def mask(s):
        s = str(s)
        if len(s) <= 2:
            return '*' * len(s)
        return s[0] + ('*' * (len(s)-2)) + s[-1]
    df = safe_apply_columnwise(df, cols, mask)
    add_log(f"Anonymized columns (naive mask) for: {cols}")
    return df

# ---------- Session state init ----------
if 'df_original' not in st.session_state:
    st.session_state['df_original'] = None
if 'df_work' not in st.session_state:
    st.session_state['df_work'] = None
if 'log_entries' not in st.session_state:
    st.session_state['log_entries'] = []
if 'recent_log' not in st.session_state:
    st.session_state['recent_log'] = []
if 'backups' not in st.session_state:
    st.session_state['backups'] = []  # store tuples (timestamp, bytes, filename)
if 'last_filename' not in st.session_state:
    st.session_state['last_filename'] = None

# ---------- Page config ----------
st.set_page_config(page_title="Data Lab - Tratamiento de Datos", layout="wide", initial_sidebar_state='expanded')

# ---------- Custom CSS / Dark theme ----------
st.markdown(
    """
    <style>
    /* Background */
    .stApp {
        background: #0b1020;
        color: #e6f0ff;
    }
    /* Cards / Containers */
    .block-container {
        padding-top: 1rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    /* Buttons */
    .stButton > button {
        background: linear-gradient(90deg, #00c6ff 0%, #0072ff 100%);
        color: white;
        border-radius: 10px;
        padding: 8px 16px;
    }
    /* File uploader */
    .stFileUploader>div { background: #0f1724; border-radius: 8px; }
    .stDataFrame table { background: #071027; color: #e6f0ff }
    </style>
    """,
    unsafe_allow_html=True
)

# ---------- Header / Main menu ----------
st.markdown("<h1 style='color:#80e6c8'>🧪 Data Lab - Entorno de Tratamiento de Datos</h1>", unsafe_allow_html=True)
st.markdown("<p style='color:#9fbfd6'>Tema oscuro • Interfaz visual • Backup automático • Log completo</p>", unsafe_allow_html=True)

# GIF that disappears after 3 seconds (use components.html)
gif_html = f"""
<div id="gif-container" style="text-align:center; margin-bottom:10px;">
  <img src="https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExM28yOTZ1Zmg0cG4wem14ZmNuM3YzcjFydG5pdTZreHVtZjIwYWRhbyZlcD12MV9naWZzX3NlYXJjaCZjdD1n/tIeCLkB8geYtW/giphy.gif"
       style="max-width:380px; border-radius:12px; box-shadow: 0 8px 20px rgba(0,0,0,0.6);"/>
</div>
<script>
setTimeout(function(){
  var el = document.getElementById('gif-container');
  if (el) { el.style.display = 'none'; }
}, 3000);
</script>
"""
st.components.v1.html(gif_html, height=220)

# ---------- Sidebar controls ----------
st.sidebar.markdown("## ⚙️ Control")
uploaded_file = st.sidebar.file_uploader("Cargar archivo (.csv, .xlsx, .xls, .txt)", type=['csv','xlsx','xls','txt'], accept_multiple_files=False)
st.sidebar.markdown("---")

# Quick options
preview_rows = st.sidebar.number_input("Filas de vista previa", min_value=5, max_value=100, value=10, step=5)
st.sidebar.markdown("### 🔒 Protección / Columnas")
protected_cols = st.sidebar.multiselect("Seleccionar columnas protegidas (no se les aplicará tratamiento)", options=[])
st.sidebar.markdown("---")

# Treatments checklist
st.sidebar.markdown("### ✅ Tratamientos disponibles (activar según necesidad)")
t_norm_headers = st.sidebar.checkbox("Normalizar encabezados (lowercase, sin espacios/acento)", value=True)
t_trim = st.sidebar.checkbox("Eliminar espacios extra (trim)", value=True)
t_remove_accents = st.sidebar.checkbox("Eliminar acentos", value=True)
t_lower = st.sidebar.checkbox("Convertir texto a minúsculas", value=False)
t_upper = st.sidebar.checkbox("Convertir texto a MAYÚSCULAS", value=False)
t_fill_na = st.sidebar.checkbox("Rellenar valores nulos (NA)", value=True)
fill_strategy = st.sidebar.selectbox("Estrategia rellenar NA", options=['constant','mean','median'], index=0)
fill_value = st.sidebar.text_input("Valor constante para NA", value="N/A")
t_drop_dup = st.sidebar.checkbox("Eliminar duplicados", value=True)
t_parse_dates = st.sidebar.checkbox("Detectar y formatear fechas", value=True)
t_convert_num = st.sidebar.checkbox("Convertir a numérico donde aplique", value=True)
t_outliers = st.sidebar.checkbox("Eliminar outliers (IQR)", value=False)
outlier_factor = st.sidebar.slider("Factor IQR", min_value=1.0, max_value=3.0, value=1.5, step=0.1)
t_anonymize = st.sidebar.checkbox("Anonymizar columnas (mask)", value=False)
st.sidebar.markdown("---")
st.sidebar.markdown("### 🛟 Backups y restauración")
if st.sidebar.button("Restaurar archivo original"):
    if st.session_state['df_original'] is not None:
        st.session_state['df_work'] = st.session_state['df_original'].copy()
        add_log("Restored to original file state via user action.")
        st.sidebar.success("Archivo original restaurado.")
    else:
        st.sidebar.error("No hay archivo original para restaurar.")

if st.sidebar.button("Restaurar última versión (auto-backup)"):
    if st.session_state['backups']:
        timestamp, bytes_blob, filename = st.session_state['backups'][-1]
        try:
            df_restored = pd.read_pickle(io.BytesIO(bytes_blob))
            st.session_state['df_work'] = df_restored
            add_log(f"Restored last backup from {timestamp}.")
            st.sidebar.success("Última versión restaurada.")
        except Exception:
            st.sidebar.error("No se pudo restaurar el backup.")
    else:
        st.sidebar.error("No hay backups disponibles.")
st.sidebar.markdown("---")

# Action buttons
if st.sidebar.button("Iniciar tratamiento de datos", key="start_treat"):
    st.session_state['start_treat'] = True
else:
    if 'start_treat' not in st.session_state:
        st.session_state['start_treat'] = False

st.sidebar.markdown("---")
st.sidebar.markdown("### 📥 Exportar / Log")
if st.sidebar.button("Descargar log (.txt)"):
    if st.session_state['log_entries']:
        log_txt = "\n".join(st.session_state['log_entries'])
        st.sidebar.download_button("Descargar log", data=log_txt, file_name=f"log_{now().split(' ')[0]}.txt", mime="text/plain")
    else:
        st.sidebar.error("No hay registros todavía.")

st.sidebar.markdown("---")
if st.sidebar.button("Volver al menú principal"):
    # In a simple app we'll just clear current data
    st.session_state['df_original'] = None
    st.session_state['df_work'] = None
    st.session_state['log_entries'] = []
    st.session_state['backups'] = []
    st.experimental_rerun()

# ---------- Load file ----------
if uploaded_file is not None:
    try:
        file_name = uploaded_file.name
        st.session_state['last_filename'] = file_name
        if file_name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(uploaded_file)
        elif file_name.lower().endswith('.csv') or file_name.lower().endswith('.txt'):
            # try comma-separated, fallback to sep='\t'
            try:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file)
            except Exception:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, sep='\t')
        else:
            st.error("Formato de archivo no soportado.")
            df = None

        if df is not None:
            st.session_state['df_original'] = df.copy()
            st.session_state['df_work'] = df.copy()
            # create initial backup (pickle bytes)
            buf = io.BytesIO()
            pd.to_pickle(df, buf)
            st.session_state['backups'].append((now(), buf.getvalue(), file_name))
            add_log(f"Archivo '{file_name}' cargado. Filas: {len(df)}, Columnas: {len(df.columns)}")
    except Exception as e:
        st.error(f"Error al leer el archivo: {e}")

# Update protected_cols options if df present
if st.session_state['df_work'] is not None:
    all_cols = list(st.session_state['df_work'].columns)
    protected_cols = st.sidebar.multiselect("Seleccionar columnas protegidas (no se les aplicará tratamiento)", options=all_cols, default=protected_cols)
else:
    all_cols = []

# ---------- Main area: Preview, stats, actions ----------
col1, col2 = st.columns([2,1])

with col1:
    st.markdown("### 📄 Vista preliminar")
    if st.session_state['df_work'] is None:
        st.info("Carga un archivo para ver su vista previa y comenzar a trabajar.")
    else:
        st.dataframe(df_preview(st.session_state['df_work'], n=preview_rows), height=360)

with col2:
    st.markdown("### 📊 Resumen rápido")
    if st.session_state['df_work'] is None:
        st.write("—")
    else:
        df = st.session_state['df_work']
        c1, c2 = st.columns(2)
        with c1:
            st.metric("Filas", f"{len(df):,}")
            st.metric("Columnas", f"{len(df.columns):,}")
        with c2:
            st.metric("Valores Nulos", f"{int(df.isna().sum().sum()):,}")
            st.metric("Duplicados", f"{int(df.duplicated().sum()):,}")
        st.markdown("**Tipos de datos (muestra 8):**")
        dtypes = pd.DataFrame({'column': df.columns, 'dtype': df.dtypes.astype(str)})
        st.dataframe(dtypes.head(8), height=180)

# ---------- Apply treatments when requested ----------
if st.session_state['start_treat']:
    if st.session_state['df_work'] is None:
        st.error("No hay archivo cargado para aplicar tratamientos.")
        st.session_state['start_treat'] = False
    else:
        # Save an auto-backup before big operation
        buf = io.BytesIO()
        pd.to_pickle(st.session_state['df_work'], buf)
        st.session_state['backups'].append((now(), buf.getvalue(), st.session_state['last_filename']))
        add_log("Auto-backup guardado antes de aplicar tratamientos.")

        df = st.session_state['df_work'].copy()

        # Apply treatments in an order that makes sense
        if t_norm_headers:
            df = normalize_headers(df)
        # update protected cols names in case headers changed
        protected = [c for c in protected_cols if c in df.columns]

        if t_trim:
            df = trim_spaces(df, protected)
        if t_remove_accents:
            df = remove_accents(df, protected)
        if t_lower:
            df = to_lowercase(df, protected)
        if t_upper:
            df = to_uppercase(df, protected)
        if t_convert_num:
            df = convert_numeric(df, protected)
        if t_parse_dates:
            df = detect_and_parse_dates(df, protected)
        if t_fill_na:
            df = fill_nas(df, protected, strategy=fill_strategy, value=fill_value)
        if t_drop_dup:
            df = drop_duplicates(df)
        if t_outliers:
            df = remove_outliers_iqr(df, protected, factor=outlier_factor)
        if t_anonymize:
            df = anonymize_columns(df, protected)

        # finalize
        st.session_state['df_work'] = df
        add_log("Tratamientos aplicados correctamente.")
        st.success("Tratamientos aplicados. Revisa la vista previa y el log.")
        st.session_state['start_treat'] = False

# ---------- Post-treatment: preview + export ----------
st.markdown("---")
st.markdown("### ✅ Resultados y exportación")
if st.session_state['df_work'] is not None:
    df_final = st.session_state['df_work']
    st.dataframe(df_preview(df_final, n=preview_rows), height=260)

    colx, coly, colz = st.columns([1,1,1])
    with colx:
        excel_data = to_excel_bytes(df_final)
        st.download_button("📥 Descargar .xlsx", data=excel_data, file_name=f"processed_{st.session_state['last_filename'] or 'data'}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with coly:
        csv_data = to_csv_bytes(df_final)
        st.download_button("📥 Descargar .csv", data=csv_data, file_name=f"processed_{st.session_state['last_filename'] or 'data'}.csv", mime="text/csv")
    with colz:
        json_data = to_json_bytes(df_final)
        st.download_button("📥 Descargar .json", data=json_data, file_name=f"processed_{st.session_state['last_filename'] or 'data'}.json", mime="application/json")
    # Parquet
    parquet_bytes = to_parquet_bytes(df_final)
    st.download_button("📥 Descargar .parquet", data=parquet_bytes, file_name=f"processed_{st.session_state['last_filename'] or 'data'}.parquet", mime="application/octet-stream")

    # Also allow saving a pickle backup
    buf2 = io.BytesIO()
    pd.to_pickle(df_final, buf2)
    st.download_button("📥 Descargar backup (.pkl)", data=buf2.getvalue(), file_name=f"backup_{now().replace(':','-')}.pkl", mime="application/octet-stream")

    # Download log
    if st.session_state['log_entries']:
        log_txt = "\n".join(st.session_state['log_entries'])
        st.download_button("📥 Descargar registro (log).txt", data=log_txt, file_name=f"log_{now().split(' ')[0]}.txt", mime="text/plain")
else:
    st.info("No hay resultados para exportar. Carga un archivo y aplica tratamientos.")

# ---------- Mini log viewer ----------
st.markdown("---")
st.markdown("### 📝 Registro de operaciones (últimos 10 eventos)")
if st.session_state['recent_log']:
    for entry in reversed(st.session_state['recent_log']):
        if "ERROR" in entry or "No se" in entry:
            st.error(entry)
        else:
            st.write(entry)
else:
    st.write("_Aún no hay operaciones registradas._")

# ---------- End of app ----------
st.markdown("---")
st.markdown("<small style='color:#7fb7d8'>Data Lab • creado para procesamiento profesional y trazabilidad • Tema oscuro</small>", unsafe_allow_html=True)
