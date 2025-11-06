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
if "transformations_applied" not in st.session_state:
    st.session_state.transformations_applied = False
if "columnas_protegidas" not in st.session_state:
    st.session_state.columnas_protegidas = []
if "columnas_eliminar" not in st.session_state:
    st.session_state.columnas_eliminar = []

# --- FUNCIÓN DE LOG ---
def add_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.log.append(f"[{timestamp}] {message}")

# --- FUNCIÓN DE CARGA ---
def cargar_archivo(archivo):
    extension = archivo.name.split(".")[-1].lower()
    try:
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
        
        # Verificar que el DataFrame no esté vacío
        if df.empty:
            st.error("⚠️ El archivo está vacío.")
            return None
            
        add_log(f"Archivo cargado: {archivo.name} - {len(df)} registros, {len(df.columns)} columnas")
        return df
    except Exception as e:
        st.error(f"⚠️ Error al cargar archivo: {e}")
        return None

# --- FUNCIÓN DE RESTAURACIÓN ---
def restaurar_archivo():
    if st.session_state.original_df is not None:
        st.session_state.processed_df = st.session_state.original_df.copy()
        st.session_state.transformations_applied = False
        st.session_state.columnas_protegidas = []
        st.session_state.columnas_eliminar = []
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

# --- FUNCIÓN DE VERIFICACIÓN DE TRANSFORMACIONES ---
def verificar_transformaciones(df_original, df_procesado, tratamiento):
    """Verifica que las transformaciones se aplicaron correctamente"""
    
    cambios_detectados = []
    
    if tratamiento == "Eliminar espacios extra":
        for col in df_procesado.select_dtypes(include=["object"]).columns:
            if col in df_original.columns:
                # Verificar espacios al inicio y final
                original_con_espacios = df_original[col].astype(str).apply(
                    lambda x: x != x.strip() if isinstance(x, str) else False
                ).any()
                
                procesado_con_espacios = df_procesado[col].astype(str).apply(
                    lambda x: x != x.strip() if isinstance(x, str) else False
                ).any()
                
                if original_con_espacios and not procesado_con_espacios:
                    cambios_detectados.append(f"✅ Espacios eliminados en columna '{col}'")
                elif original_con_espacios and procesado_con_espacios:
                    cambios_detectados.append(f"❌ Espacios NO eliminados en columna '{col}'")
    
    elif tratamiento == "Eliminar acentos":
        for col in df_procesado.select_dtypes(include=["object"]).columns:
            if col in df_original.columns:
                # Verificar si hay acentos (simplificado)
                original_str = df_original[col].astype(str).str.cat()
                procesado_str = df_procesado[col].astype(str).str.cat()
                
                if original_str != procesado_str:
                    cambios_detectados.append(f"✅ Cambios detectados en columna '{col}'")
    
    return cambios_detectados

# --- FUNCIONES DE TRATAMIENTO COMPLETAMENTE REESCRITAS ---
def aplicar_tratamientos(df, opciones, protegidas):
    """Aplica tratamientos de manera más robusta y verificable"""
    
    if df is None or df.empty:
        st.error("❌ No hay datos para procesar")
        return df
        
    df_tratado = df.copy()
    add_log("Inicio de tratamiento de datos...")
    
    # Crear bandera para verificar cambios
    cambios_realizados = False
    
    # 1. Eliminar duplicados
    if "Eliminar duplicados" in opciones:
        registros_antes = len(df_tratado)
        df_tratado = df_tratado.drop_duplicates()
        registros_despues = len(df_tratado)
        if registros_despues < registros_antes:
            cambios_realizados = True
            add_log(f"Duplicados eliminados: {registros_antes - registros_despues} registros removidos")

    # 2. Eliminar espacios extra - ENFOQUE MÁS AGRESIVO
    if "Eliminar espacios extra" in opciones:
        columnas_procesadas = []
        for col in df_tratado.columns:
            if col not in protegidas and df_tratado[col].dtype == 'object':
                # Guardar estado antes
                antes = df_tratado[col].copy()
                
                # Aplicar transformación de manera más agresiva
                df_tratado[col] = df_tratado[col].astype(str)
                df_tratado[col] = df_tratado[col].str.strip()
                df_tratado[col] = df_tratado[col].str.replace(r'\s+', ' ', regex=True)
                
                # Verificar si hubo cambios
                if not antes.equals(df_tratado[col]):
                    columnas_procesadas.append(col)
                    cambios_realizados = True
        
        if columnas_procesadas:
            add_log(f"Espacios extra eliminados en columnas: {', '.join(columnas_procesadas)}")

    # 3. Normalizar encabezados
    if "Normalizar encabezados" in opciones:
        nuevos_nombres = {}
        for col in df_tratado.columns:
            nuevo_nombre = unidecode(str(col).strip().lower().replace(" ", "_").replace("-", "_"))
            nuevos_nombres[col] = nuevo_nombre
        
        df_tratado.rename(columns=nuevos_nombres, inplace=True)
        cambios_realizados = True
        add_log("Encabezados normalizados.")

    # 4. Rellenar valores nulos
    if "Rellenar nulos" in opciones:
        nulos_rellenados = 0
        for col in df_tratado.columns:
            if col not in protegidas:
                nulos_antes = df_tratado[col].isna().sum()
                if nulos_antes > 0:
                    if df_tratado[col].dtype == "object":
                        df_tratado[col].fillna("N/A", inplace=True)
                    elif pd.api.types.is_numeric_dtype(df_tratado[col]):
                        # Para coordenadas, usar 0
                        if any(term in col.lower() for term in ['lat', 'lon', 'long', 'latitude', 'longitude']):
                            df_tratado[col].fillna(0, inplace=True)
                        else:
                            df_tratado[col].fillna(df_tratado[col].median(), inplace=True)
                    nulos_rellenados += nulos_antes
                    cambios_realizados = True
        
        if nulos_rellenados > 0:
            add_log(f"Valores nulos rellenados: {nulos_rellenados} valores.")

    # 5. Eliminar acentos - ENFOQUE MÁS DIRECTIVO
    if "Eliminar acentos" in opciones:
        columnas_procesadas = []
        for col in df_tratado.columns:
            if col not in protegidas and df_tratado[col].dtype == 'object':
                # Guardar estado antes
                antes = df_tratado[col].copy()
                
                # Aplicar unidecode directamente
                df_tratado[col] = df_tratado[col].astype(str)
                df_tratado[col] = df_tratado[col].apply(lambda x: unidecode(x))
                
                # Verificar si hubo cambios
                if not antes.equals(df_tratado[col]):
                    columnas_procesadas.append(col)
                    cambios_realizados = True
        
        if columnas_procesadas:
            add_log(f"Acentos eliminados en columnas: {', '.join(columnas_procesadas)}")

    # 6. Convertir texto a minúsculas
    if "Texto a minúsculas" in opciones:
        columnas_procesadas = []
        for col in df_tratado.columns:
            if col not in protegidas and df_tratado[col].dtype == 'object':
                # Guardar estado antes
                antes = df_tratado[col].copy()
                
                # Aplicar lowercase
                df_tratado[col] = df_tratado[col].astype(str)
                df_tratado[col] = df_tratado[col].str.lower()
                
                # Verificar si hubo cambios
                if not antes.equals(df_tratado[col]):
                    columnas_procesadas.append(col)
                    cambios_realizados = True
        
        if columnas_procesadas:
            add_log(f"Texto convertido a minúsculas en columnas: {', '.join(columnas_procesadas)}")

    # 7. Eliminar outliers
    if "Eliminar outliers" in opciones:
        registros_antes = len(df_tratado)
        columnas_numericas = df_tratado.select_dtypes(include=[np.number]).columns
        columnas_procesadas = []
        
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
                            columnas_procesadas.append(col)
                            cambios_realizados = True
        
        registros_eliminados = registros_antes - len(df_tratado)
        if registros_eliminados > 0:
            add_log(f"Outliers eliminados: {registros_eliminados} registros en {len(columnas_procesadas)} columnas")

    if cambios_realizados:
        add_log("Tratamiento de datos completado con cambios.")
        st.session_state.transformations_applied = True
        st.success("✅ Tratamiento completado con éxito.")
        
        # Mostrar resumen
        with st.expander("📊 Resumen de cambios aplicados"):
            st.write(f"**Registros finales:** {len(df_tratado)}")
            st.write(f"**Columnas finales:** {len(df_tratado.columns)}")
            st.write(f"**Tratamientos aplicados:** {len(opciones)}")
            
            # Verificación específica de espacios
            if "Eliminar espacios extra" in opciones:
                st.write("**Verificación de espacios:**")
                for col in df_tratado.select_dtypes(include=["object"]).columns[:3]:  # Mostrar solo 3 columnas
                    if len(df_tratado) > 0:
                        ejemplo = df_tratado[col].iloc[0]
                        st.write(f"- '{col}': '{ejemplo}'")
    else:
        st.info("ℹ️ No se detectaron cambios después del tratamiento.")

    return df_tratado

# --- INTERFAZ PRINCIPAL ---
st.title("🧠 Entorno de Tratamiento de Datos Profesional")

archivo = st.sidebar.file_uploader("📂 Cargar archivo", type=["xlsx", "xls", "csv", "txt", "ods"])

if archivo:
    df = cargar_archivo(archivo)
    if df is not None:
        # Solo actualizar si es la primera carga o si se restauró
        if st.session_state.original_df is None:
            st.session_state.original_df = df.copy()
            st.session_state.processed_df = df.copy()
            st.session_state.transformations_applied = False
            st.session_state.columnas_protegidas = []
            st.session_state.columnas_eliminar = []

        st.subheader("👁️ Vista preliminar del archivo original")
        st.dataframe(st.session_state.original_df.head(10), use_container_width=True)
        
        # Mostrar información del dataset
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Registros", len(st.session_state.original_df))
        with col2:
            st.metric("Columnas", len(st.session_state.original_df.columns))
        with col3:
            st.metric("Transformaciones aplicadas", 
                     "Sí" if st.session_state.transformations_applied else "No")

        # --- OPCIONES DE PROCESAMIENTO ---
        columnas = list(st.session_state.processed_df.columns)
        
        # Actualizar listas para remover columnas que ya no existen
        st.session_state.columnas_protegidas = [p for p in st.session_state.columnas_protegidas if p in columnas]
        st.session_state.columnas_eliminar = [e for e in st.session_state.columnas_eliminar if e in columnas]

        st.sidebar.subheader("🛡️ Protección y eliminación")

        # Columnas protegidas - PERSISTENTE
        st.session_state.columnas_protegidas = st.sidebar.multiselect(
            "Seleccionar columnas protegidas", 
            columnas,
            default=st.session_state.columnas_protegidas,
            key="multiselect_protegidas"
        )

        # Columnas a eliminar - PERSISTENTE (excluyendo las protegidas)
        columnas_disponibles_eliminar = [c for c in columnas if c not in st.session_state.columnas_protegidas]
        st.session_state.columnas_eliminar = st.sidebar.multiselect(
            "Eliminar columnas", 
            columnas_disponibles_eliminar,
            default=st.session_state.columnas_eliminar,
            key="multiselect_eliminar"
        )

        # --- BOTONES DE ACCIÓN ---
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("🚀 Iniciar tratamiento", use_container_width=True):
                if opciones:
                    nuevo_df = aplicar_tratamientos(
                        st.session_state.processed_df, opciones, st.session_state.columnas_protegidas
                    )
                    if nuevo_df is not None:
                        st.session_state.processed_df = nuevo_df
                        st.rerun()
                else:
                    st.warning("⚠️ Selecciona al menos un tratamiento")
        
        with col2:
            if st.button("🔄 Restaurar original", use_container_width=True):
                restaurar_archivo()
                st.rerun()

        # --- BOTÓN PARA ELIMINAR COLUMNAS ---
        if st.session_state.columnas_eliminar:
            if st.sidebar.button("🗑️ Eliminar columnas seleccionadas", type="primary", use_container_width=True):
                columnas_a_eliminar = st.session_state.columnas_eliminar.copy()
                
                # Verificar que las columnas existen antes de eliminarlas
                columnas_existentes = [col for col in columnas_a_eliminar if col in st.session_state.processed_df.columns]
                
                if columnas_existentes:
                    st.session_state.processed_df = st.session_state.processed_df.drop(columns=columnas_existentes)
                    add_log(f"Columnas eliminadas: {', '.join(columnas_existentes)}")
                    st.success(f"🗑️ Columnas eliminadas: {', '.join(columnas_existentes)}")
                    
                    # Actualizar las listas de selección
                    st.session_state.columnas_protegidas = [p for p in st.session_state.columnas_protegidas if p not in columnas_existentes]
                    st.session_state.columnas_eliminar = []
                    
                    st.rerun()
                else:
                    st.error("❌ Las columnas seleccionadas ya no existen en el dataset")
                    st.session_state.columnas_eliminar = []

        # --- OPCIONES DE TRATAMIENTO ---
        st.sidebar.subheader("⚙️ Tratamientos disponibles")
        opciones = st.sidebar.multiselect(
            "Selecciona tratamientos a aplicar:",
            ["Eliminar duplicados", "Eliminar espacios extra", "Normalizar encabezados",
             "Rellenar nulos", "Eliminar acentos", "Texto a minúsculas", "Eliminar outliers"],
            key="multiselect_tratamientos"
        )

        # --- MOSTRAR DATOS PROCESADOS ---
        if st.session_state.transformations_applied:
            st.subheader("📊 Vista de datos procesados")
            st.dataframe(st.session_state.processed_df.head(10), use_container_width=True)
            
            # Mostrar diferencias
            st.subheader("🔍 Comparación de cambios")
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Original**")
                st.write(f"- Registros: {len(st.session_state.original_df)}")
                st.write(f"- Columnas: {len(st.session_state.original_df.columns)}")
            
            with col2:
                st.write("**Procesado**")
                st.write(f"- Registros: {len(st.session_state.processed_df)}")
                st.write(f"- Columnas: {len(st.session_state.processed_df.columns)}")

        # --- SECCIÓN DE DESCARGA MEJORADA ---
        st.sidebar.markdown("---")
        st.sidebar.subheader("📤 Exportar resultados")
        
        if st.session_state.processed_df is not None and not st.session_state.processed_df.empty:
            # Información general
            total_registros = len(st.session_state.processed_df)
            st.sidebar.info(f"📊 Registros listos: {total_registros}")
            
            # Opciones de formato
            formato = st.sidebar.selectbox("Formato de exportación", ["xlsx", "csv", "json"])
            
            # Filtro para descarga
            st.sidebar.subheader("📅 Filtro para descarga")
            aplicar_filtro = st.sidebar.radio(
                "Filtro de fechas:",
                ["Descargar sin filtro", "Aplicar filtro de fechas"],
                index=0
            )
            
            df_para_descargar = st.session_state.processed_df.copy()
            mensaje_descarga = f"Descargando {total_registros} registros"
            
            if aplicar_filtro == "Aplicar filtro de fechas":
                # Selección de columna de fecha
                fecha_columna = st.sidebar.selectbox(
                    "Columna de fecha:",
                    st.session_state.processed_df.columns,
                    help="Selecciona la columna que contiene las fechas"
                )
                
                if fecha_columna:
                    info_fechas, invalidas = mostrar_info_fechas(st.session_state.processed_df, fecha_columna)
                    st.sidebar.info(info_fechas)
                    
                    filtro_tipo = st.sidebar.selectbox("Tipo de filtro:", ["año", "mes", "rango"])
                    
                    año, mes, fecha_inicio, fecha_fin = None, None, None, None
                    
                    if filtro_tipo == "año":
                        try:
                            df_temp = st.session_state.processed_df.copy()
                            df_temp['fecha_temp'] = pd.to_datetime(df_temp[fecha_columna], errors='coerce')
                            df_temp = df_temp.dropna(subset=['fecha_temp'])
                            años_disponibles = sorted(df_temp['fecha_temp'].dt.year.dropna().unique())
                            if años_disponibles:
                                año = st.sidebar.selectbox("Seleccionar año:", años_disponibles)
                        except:
                            st.sidebar.warning("No se pudieron obtener años")
                    
                    elif filtro_tipo == "mes":
                        mes = st.sidebar.selectbox("Mes:", range(1, 13), format_func=lambda x: datetime(2023, x, 1).strftime("%B"))
                    
                    elif filtro_tipo == "rango":
                        col1, col2 = st.sidebar.columns(2)
                        with col1:
                            fecha_inicio = st.date_input("Fecha inicio:")
                        with col2:
                            fecha_fin = st.date_input("Fecha fin:")
                    
                    if año or mes or (fecha_inicio and fecha_fin):
                        df_para_descargar = aplicar_filtro_fechas_descarga(
                            st.session_state.processed_df, fecha_columna, filtro_tipo, año, mes, fecha_inicio, fecha_fin
                        )
                        mensaje_descarga = f"Descargando {len(df_para_descargar)} registros (filtrados)"
            
            # Botón de descarga
            st.sidebar.markdown("---")
            st.sidebar.info(mensaje_descarga)
            
            if st.sidebar.button("💾 Generar archivo para descarga", use_container_width=True):
                if len(df_para_descargar) > 0:
                    buffer = io.BytesIO()
                    
                    try:
                        if formato == "xlsx":
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                df_para_descargar.to_excel(writer, index=False, sheet_name='Datos_Procesados')
                            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            filename = "datos_procesados.xlsx"
                        elif formato == "csv":
                            df_para_descargar.to_csv(buffer, index=False, encoding='utf-8')
                            mime = "text/csv"
                            filename = "datos_procesados.csv"
                        elif formato == "json":
                            df_para_descargar.to_json(buffer, orient="records", force_ascii=False)
                            mime = "application/json"
                            filename = "datos_procesados.json"
                        
                        buffer.seek(0)
                        
                        # Crear el botón de descarga
                        st.sidebar.download_button(
                            label="⬇️ Descargar archivo",
                            data=buffer.getvalue(),
                            file_name=filename,
                            mime=mime,
                            use_container_width=True
                        )
                        
                        # Registrar en el log
                        if aplicar_filtro == "Aplicar filtro de fechas":
                            add_log(f"Archivo exportado como {formato} con {len(df_para_descargar)} registros (filtrado)")
                        else:
                            add_log(f"Archivo exportado como {formato} con {len(df_para_descargar)} registros")
                            
                    except Exception as e:
                        st.sidebar.error(f"❌ Error al generar archivo: {e}")
                else:
                    st.sidebar.error("❌ No hay datos para descargar")

        # --- DESCARGA DEL LOG ---
        st.sidebar.subheader("📝 Registro de operaciones")
        if st.session_state.log:
            if st.sidebar.button("🧾 Descargar log de operaciones", use_container_width=True):
                log_txt = "\n".join(st.session_state.log)
                st.sidebar.download_button(
                    "⬇️ Descargar log",
                    data=log_txt,
                    file_name="registro_operaciones.txt",
                    mime="text/plain",
                    use_container_width=True
                )

        # --- BOTÓN MENÚ PRINCIPAL ---
        if st.sidebar.button("🏠 Volver al menú principal", use_container_width=True):
            st.session_state.original_df = None
            st.session_state.processed_df = None
            st.session_state.log = []
            st.session_state.transformations_applied = False
            st.session_state.columnas_protegidas = []
            st.session_state.columnas_eliminar = []
            st.rerun()

else:
    st.info("👈 Carga un archivo para comenzar el tratamiento de datos.")
