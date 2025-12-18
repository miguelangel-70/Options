# app_unificada.py
"""
Aplicación Unificada - Dashboard de Volatilidad
Fusión de backend.py y frontend.py para Streamlit Cloud
Sin servidor FastAPI - Acceso directo a base de datos
"""

import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import base64
from datetime import datetime
import sqlite3
import os
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
import shutil
import time


# ============================================================================
# CONFIGURACIÓN INICIAL
# ============================================================================

st.set_page_config(
    page_title="Dashboard de Volatilidad",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de archivos
EXCEL_PATH = "base_datos.xlsx"
DB_PATH = "base_datos.db"

# ============================================================================
# FUNCIONES DE BASE DE DATOS (del backend.py)
# ============================================================================

def get_data_source():
    """Determina la fuente de datos"""
    if os.path.exists(DB_PATH):
        return "sqlite"
    elif os.path.exists(EXCEL_PATH):
        return "excel"
    else:
        return "none"

import time




def init_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    """Configura PRAGMAs de rendimiento"""
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA cache_size = -64000;")
        conn.execute("PRAGMA mmap_size = 268435456;")
        logger.info("PRAGMAs de optimización configurados")
    except Exception as e:
        logger.warning(f"Error configurando PRAGMAs: {e}")


def ensure_oi_indexes(conn: sqlite3.Connection) -> None:
    """Crea índices optimizados"""
    try:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_oi_asset_dates
            ON open_interest(asset, extraction_date, expiration_date);
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_oi_asset_exp
            ON open_interest(asset, expiration_date);
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_oi_asset_exp_strike
            ON open_interest(asset, expiration_date, strike);
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_oi_evolution
            ON open_interest(asset, expiration_date, strike, extraction_date);
        """)
        
        logger.info("Índices de optimización verificados/creados")
    except Exception as e:
        logger.warning(f"Error creando índices: {e}")

def create_sqlite_from_excel():
    """Crea SQLite desde Excel - Compatible con múltiples formatos"""
    if not os.path.exists(EXCEL_PATH):
        return False
    
    try:
        logger.info("Creando base de datos SQLite desde Excel...")
        
        conn = sqlite3.connect(DB_PATH)
        init_sqlite_pragmas(conn)
        
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS open_interest (
                asset TEXT NOT NULL,
                extraction_date INTEGER NOT NULL,
                expiration_date INTEGER NOT NULL,
                strike REAL NOT NULL,
                call_oi INTEGER NOT NULL DEFAULT 0,
                put_oi INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (asset, extraction_date, expiration_date, strike)
            ) WITHOUT ROWID;
        """
        conn.execute(create_table_sql)
        ensure_oi_indexes(conn)
        
        excel_file = pd.ExcelFile(EXCEL_PATH)
        
        def date_to_int(date_series):
            return pd.to_datetime(date_series).dt.strftime('%Y%m%d').astype(int)
        
        for sheet_name in excel_file.sheet_names:
            logger.info(f"Procesando hoja: {sheet_name}")
            
            df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)
            
            # DETECCIÓN AUTOMÁTICA DE FORMATO - CORREGIDA
            if "Trade Date" in df.columns and "call_oi" in df.columns and "put_oi" in df.columns:
                # FORMATO SP500 ORIGINAL
                logger.info(f"Detectado formato SP500 para {sheet_name}")
                
                # Renombrar al formato estándar
                df = df.rename(columns={
                    "Trade Date": "Fecha de Extracción",
                    "Expiration Date": "Expiration Date",
                    "call_oi": "Call Open Interest",
                    "put_oi": "Put Open Interest"
                })
                # Strike ya está con el nombre correcto
                
            elif "Fecha de Extracción" in df.columns:
                # FORMATO EUROSTOXX/VIX
                logger.info(f"Detectado formato EUROSTOXX/VIX para {sheet_name}")
                
                # Verificar y renombrar columnas si es necesario
                if "Call Open Interest" not in df.columns or "Put Open Interest" not in df.columns:
                    if "Open Interest" in df.columns and "Open Interest.1" in df.columns:
                        df = df.rename(columns={
                            "Open Interest": "Call Open Interest",
                            "Open Interest.1": "Put Open Interest"
                        })
            else:
                logger.warning(f"Formato no reconocido en {sheet_name}, saltando...")
                continue
            
            # AHORA TODAS LAS HOJAS TIENEN EL FORMATO ESTÁNDAR
            # Verificar que tenemos las columnas correctas
            required_columns = ["Fecha de Extracción", "Expiration Date", "Strike", "Call Open Interest", "Put Open Interest"]
            
            for col in required_columns:
                if col not in df.columns:
                    logger.error(f"Columna faltante {col} en {sheet_name}")
                    raise ValueError(f"Columna {col} no encontrada en {sheet_name}")
            
            # Procesamiento común
            df["strike"] = pd.to_numeric(df["Strike"], errors="coerce")
            df["call_oi"] = pd.to_numeric(df["Call Open Interest"], errors="coerce").fillna(0).astype(int)
            df["put_oi"] = pd.to_numeric(df["Put Open Interest"], errors="coerce").fillna(0).astype(int)
            
            df["extraction_date"] = date_to_int(df["Fecha de Extracción"])
            df["expiration_date"] = date_to_int(df["Expiration Date"])
            
            # ELIMINAR DUPLICADOS ANTES DE INSERTAR
            # Esto es importante porque puede haber duplicados en el Excel
            df = df.dropna(subset=["extraction_date", "expiration_date", "strike"])
            
            # Identificar duplicados exactos
            duplicates = df.duplicated(subset=["extraction_date", "expiration_date", "strike"], keep='first')
            if duplicates.any():
                logger.warning(f"Encontrados {duplicates.sum()} registros duplicados en {sheet_name}, eliminando...")
                df = df[~duplicates]
            
            df["asset"] = sheet_name.upper().strip()
            
            # Seleccionar columnas finales
            df_final = df[["asset", "extraction_date", "expiration_date", "strike", "call_oi", "put_oi"]]
            
            # Insertar en lotes
            chunk_size = 2000
            for i in range(0, len(df_final), chunk_size):
                chunk = df_final.iloc[i:i + chunk_size]
                try:
                    chunk.to_sql("open_interest", conn, if_exists="append", index=False)
                except sqlite3.IntegrityError as e:
                    # Si hay duplicados, intentar insertar uno por uno
                    logger.warning(f"Error de integridad en lote {i}: {e}")
                    for _, row in chunk.iterrows():
                        try:
                            row_df = pd.DataFrame([row])
                            row_df.to_sql("open_interest", conn, if_exists="append", index=False)
                        except sqlite3.IntegrityError:
                            # Si el registro ya existe, saltarlo
                            logger.debug(f"Registro duplicado saltado: {row['asset']}, {row['extraction_date']}, {row['expiration_date']}, {row['strike']}")
                            continue
            
            logger.info(f"{len(df_final)} registros insertados para asset={sheet_name}")
        
        logger.info("Optimizando base de datos...")
        conn.execute("ANALYZE;")
        conn.execute("VACUUM;")
        conn.close()
        
        logger.info("Base de datos SQLite creada exitosamente")
        mostrar_mensaje("success", "Base de datos SQLite creada exitosamente.")
        return True
        
    except Exception as e:
        logger.error(f"Error creando SQLite: {e}")
        if os.path.exists(DB_PATH):
            try:
                os.remove(DB_PATH)
            except:
                pass
        return False


def get_database_connection():
    """Obtiene conexión a base de datos"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_sqlite_pragmas(conn)
    return conn


def execute_sql_query(query: str, params: tuple = None):
    """Ejecuta consulta SQL"""
    conn = get_database_connection()
    try:
        cursor = conn.execute(query, params or ())
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

def upload_excel_file(uploaded_file):
    """Carga archivo Excel"""
    try:
        contents = uploaded_file.getvalue()
        
        # Limpiar base de datos existente
        limpiar_base_datos_existente()
        
        # Guardar Excel
        with open(EXCEL_PATH, "wb") as f:
            f.write(contents)
        
        # Crear nueva BD desde Excel
        success = create_sqlite_from_excel()
        
        if not success:
            return None
        
        conn = get_database_connection()
        cursor = conn.execute("SELECT DISTINCT asset FROM open_interest ORDER BY asset")
        assets = [row['asset'] for row in cursor.fetchall()]
        conn.close()
        
        get_estado.clear()
        get_fechas_extraccion.clear()
        get_fechas_vencimiento.clear()
        get_strikes.clear()
        mostrar_mensaje("success", f"Archivo {uploaded_file.name} cargado exitosamente. Base de datos SQLite creada.")
        time.sleep(2)
        return {
            "mensaje": f"Archivo {uploaded_file.name} cargado exitosamente. Base de datos SQLite creada.",
            "hojas_disponibles": assets,
            "hoja_activa": assets[0] if assets else None
        }
        
    except Exception as e:
        logger.error(f"Error cargando Excel: {e}")
        try:
            if os.path.exists(EXCEL_PATH):
                os.remove(EXCEL_PATH)
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
        except:
            pass
        return None

        
def cargar_calendario_vencimientos():
    """Carga el calendario de vencimientos y crea diccionario de mapeo"""
    try:
        # Primero intentar cargar desde archivo CSV/Excel
        calendario_paths = [
            "calendario_vencimientos.csv",
            "calendario_vencimientos.xlsx",
            "vencimientos.csv",
            "calendario.csv",
            "OEX_cierre_semanal.xlsx"
        ]
        
        calendario_df = None
        
        for path in calendario_paths:
            if os.path.exists(path):
                try:
                    logger.info(f"Intentando cargar calendario desde: {path}")
                    
                    if path.endswith('.csv'):
                        # Intentar diferentes delimitadores
                        try:
                            calendario_df = pd.read_csv(path, encoding='utf-8')
                        except:
                            calendario_df = pd.read_csv(path, encoding='latin-1')
                    else:  # Excel
                        # Leer todas las hojas hasta encontrar datos de calendario
                        xl = pd.ExcelFile(path)
                        for sheet in xl.sheet_names:
                            try:
                                df_temp = pd.read_excel(path, sheet_name=sheet)
                                # Verificar si tiene columnas relevantes
                                if any(col in df_temp.columns for col in ['Mes', 'Periodo', 'Fecha_expiracion', 'Fecha']):
                                    calendario_df = df_temp
                                    logger.info(f"Calendario encontrado en hoja: {sheet}")
                                    break
                            except:
                                continue
                    
                    if calendario_df is not None and not calendario_df.empty:
                        logger.info(f"Calendario cargado desde {path}")
                        break
                        
                except Exception as e:
                    logger.warning(f"No se pudo cargar desde {path}: {e}")
        
        # Si no se encontró archivo, usar calendario embebido
        if calendario_df is None or calendario_df.empty:
            logger.info("Usando calendario embebido por defecto")
            calendario_df = pd.DataFrame({
                'Mes': ['Diciembre', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 
                       'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre'],
                'Periodo': [2025, 2026, 2026, 2026, 2026, 2026, 
                          2026, 2026, 2026, 2026, 2026, 2026],
                'Fecha_expiracion': ['19.12.2025', '16.01.2026', '20.02.2026', '20.03.2026',
                                    '17.04.2026', '15.05.2026', '18.06.2026', '17.07.2026',
                                    '21.08.2026', '18.09.2026', '16.10.2026', '20.11.2026']
            })
        
        # Normalizar nombres de columnas
        calendario_df.columns = calendario_df.columns.str.strip().str.lower()
        
        # Renombrar columnas si es necesario
        column_mapping = {
            'mes': 'mes',
            'periodo': 'periodo',
            'año': 'periodo',
            'year': 'periodo',
            'fecha_expiracion': 'fecha_expiracion',
            'fecha': 'fecha_expiracion',
            'expiration': 'fecha_expiracion',
            'vencimiento': 'fecha_expiracion'
        }
        
        for old_col in calendario_df.columns:
            for key, value in column_mapping.items():
                if key in old_col.lower():
                    calendario_df = calendario_df.rename(columns={old_col: value})
        
        # Verificar columnas requeridas
        required_cols = ['mes', 'periodo', 'fecha_expiracion']
        missing_cols = [col for col in required_cols if col not in calendario_df.columns]
        
        if missing_cols:
            logger.error(f"Columnas faltantes en calendario: {missing_cols}")
            mostrar_mensaje("error", f"Faltan columnas en calendario: {missing_cols}")
            return {}
        
        # Convertir fechas
        calendario_df['fecha_expiracion_dt'] = pd.to_datetime(
            calendario_df['fecha_expiracion'], 
            dayfirst=True,
            errors='coerce'
        )
        
        # Si la conversión falla, intentar otros formatos
        if calendario_df['fecha_expiracion_dt'].isna().any():
            calendario_df['fecha_expiracion_dt'] = pd.to_datetime(
                calendario_df['fecha_expiracion'], 
                errors='coerce'
            )
        
        # Filtrar filas con fechas inválidas
        calendario_df = calendario_df.dropna(subset=['fecha_expiracion_dt'])
        
        # Crear diccionario de búsqueda
        calendario_dict = {}
        
        # Mapeo de meses abreviados en inglés y español
        meses_abrev = {
            # Español
            'ENE': 'Enero', 'FEB': 'Febrero', 'MAR': 'Marzo', 'ABR': 'Abril',
            'MAY': 'Mayo', 'JUN': 'Junio', 'JUL': 'Julio', 'AGO': 'Agosto',
            'SEP': 'Septiembre', 'OCT': 'Octubre', 'NOV': 'Noviembre', 'DIC': 'Diciembre',
            # Inglés
            'JAN': 'Enero', 'FEB': 'Febrero', 'MAR': 'Marzo', 'APR': 'Abril',
            'MAY': 'Mayo', 'JUN': 'Junio', 'JUL': 'Julio', 'AUG': 'Agosto',
            'SEP': 'Septiembre', 'OCT': 'Octubre', 'NOV': 'Noviembre', 'DEC': 'Diciembre'
        }
        
        for _, row in calendario_df.iterrows():
            mes_completo = str(row['mes']).strip().title()
            periodo = int(row['periodo'])
            fecha_dt = row['fecha_expiracion_dt']
            
            # Clave con mes completo: "Diciembre 2025"
            clave_completa = f"{mes_completo} {periodo}"
            calendario_dict[clave_completa.upper()] = fecha_dt
            
            # Clave con primeras 3 letras: "DIC 2025"
            for abrev_ing, mes_esp in meses_abrev.items():
                if mes_esp.lower() == mes_completo.lower():
                    clave_abrev = f"{abrev_ing} {periodo}"
                    calendario_dict[clave_abrev.upper()] = fecha_dt
        
        logger.info(f"Calendario creado con {len(calendario_dict)} entradas")
        
        # Log para debugging
        logger.info("Mapeos creados (primeros 5):")
        for i, (clave, fecha) in enumerate(list(calendario_dict.items())[:5]):
            logger.info(f"  {clave} -> {fecha.strftime('%Y-%m-%d')}")
        
        return calendario_dict
        
    except Exception as e:
        logger.error(f"Error cargando calendario: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}

# ============================================================================
# FUNCIONES DE LÓGICA DE NEGOCIO (del backend.py)
# ============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def get_estado():
    """Obtiene el estado de la base de datos"""
    data_source = get_data_source()
    
    if data_source == "sqlite":
        try:
            conn = get_database_connection()
            cursor = conn.execute("SELECT DISTINCT asset FROM open_interest ORDER BY asset")
            assets = [row['asset'] for row in cursor.fetchall()]
            conn.close()
        except Exception as e:
            logger.error(f"Error obteniendo assets: {e}")
            assets = []
    elif data_source == "excel":
        try:
            excel_file = pd.ExcelFile(EXCEL_PATH)
            assets = excel_file.sheet_names
        except:
            assets = []
    else:
        assets = []
    
    return {
        "existe_base_datos": data_source != "none",
        "hojas_disponibles": assets,
        "hoja_activa": assets[0] if assets else None,
        "data_source": data_source,
        "esquema": "unificado"
    }


@st.cache_data(ttl=300, show_spinner=False)
def get_fechas_extraccion(asset: str):
    """Obtiene fechas de extracción"""
    logger.info(f"Obteniendo fechas extracción: {asset}")
    
    data_source = get_data_source()
    
    try:
        if data_source == "sqlite":
            query = '''
                SELECT DISTINCT extraction_date 
                FROM open_interest 
                WHERE asset = ? 
                ORDER BY extraction_date DESC
            '''
            results = execute_sql_query(query, (asset,))
            dates = [row['extraction_date'] for row in results]
        else:
            df = get_excel_data(asset)
            dates = sorted(df['extraction_date'].dropna().unique(), reverse=True)
        
        date_strings = []
        for date_val in dates:
            if pd.isna(date_val):
                continue
            
            if isinstance(date_val, (int, np.integer)):
                date_str = str(date_val)
                if len(date_str) == 8:
                    try:
                        dt = datetime.strptime(date_str, '%Y%m%d')
                        date_strings.append(dt.strftime('%Y-%m-%d'))
                    except ValueError:
                        date_strings.append(date_str)
                else:
                    date_strings.append(str(date_val))
            elif isinstance(date_val, (datetime, pd.Timestamp)):
                date_strings.append(date_val.strftime('%Y-%m-%d'))
            else:
                date_strings.append(str(date_val))
        
        logger.info(f"Encontradas {len(date_strings)} fechas de extracción")
        return date_strings
        
    except Exception as e:
        logger.error(f"Error en fechas extracción: {e}")
        return []


@st.cache_data(ttl=300, show_spinner=False)
def get_fechas_vencimiento(asset: str, fecha_extraccion: Optional[str] = None):
    """Obtiene fechas de vencimiento"""
    logger.info(f"Obteniendo fechas vencimiento: {asset}, filtro: {fecha_extraccion}")
    
    data_source = get_data_source()
    
    try:
        if data_source == "sqlite":
            if fecha_extraccion:
                try:
                    fecha_dt = datetime.strptime(fecha_extraccion, '%Y-%m-%d')
                    fecha_int = int(fecha_dt.strftime('%Y%m%d'))
                except ValueError:
                    fecha_int = fecha_extraccion
                
                query = '''
                    SELECT DISTINCT expiration_date 
                    FROM open_interest 
                    WHERE asset = ? AND extraction_date = ? 
                    ORDER BY expiration_date ASC
                '''
                results = execute_sql_query(query, (asset, fecha_int))
            else:
                query = '''
                    SELECT DISTINCT expiration_date 
                    FROM open_interest 
                    WHERE asset = ? 
                    ORDER BY expiration_date ASC
                '''
                results = execute_sql_query(query, (asset,))
            
            dates = [row['expiration_date'] for row in results]
        else:
            df = get_excel_data(asset)
            if fecha_extraccion:
                fecha_obj = pd.to_datetime(fecha_extraccion)
                df = df[df['extraction_date'] == fecha_obj]
            dates = sorted(df['expiration_date'].dropna().unique())
        
        date_strings = []
        for date_val in dates:
            if pd.isna(date_val):
                continue
            
            if isinstance(date_val, (int, np.integer)):
                date_str = str(date_val)
                if len(date_str) == 8:
                    try:
                        dt = datetime.strptime(date_str, '%Y%m%d')
                        date_strings.append(dt.strftime('%Y-%m-%d'))
                    except ValueError:
                        date_strings.append(date_str)
                else:
                    date_strings.append(str(date_val))
            elif isinstance(date_val, (datetime, pd.Timestamp)):
                date_strings.append(date_val.strftime('%Y-%m-%d'))
            else:
                date_strings.append(str(date_val))
        
        logger.info(f"Encontradas {len(date_strings)} fechas de vencimiento")
        return date_strings
        
    except Exception as e:
        logger.error(f"Error en fechas vencimiento: {e}")
        return []


@st.cache_data(ttl=300, show_spinner=False)
def get_strikes(asset: str, fecha_vencimiento: str):
    """Obtiene strikes disponibles"""
    logger.info(f"Obteniendo strikes: {asset}, vencimiento: {fecha_vencimiento}")
    
    data_source = get_data_source()
    
    try:
        if data_source == "sqlite":
            try:
                fecha_dt = datetime.strptime(fecha_vencimiento, '%Y-%m-%d')
                fecha_int = int(fecha_dt.strftime('%Y%m%d'))
            except ValueError:
                fecha_int = fecha_vencimiento
            
            query = '''
                SELECT DISTINCT strike 
                FROM open_interest 
                WHERE asset = ? AND expiration_date = ? 
                ORDER BY strike ASC
            '''
            results = execute_sql_query(query, (asset, fecha_int))
            strikes = [float(row['strike']) for row in results if row['strike'] is not None]
        else:
            df = get_excel_data(asset)
            fecha_ven_obj = pd.to_datetime(fecha_vencimiento)
            df_filtrado = df[df['expiration_date'] == fecha_ven_obj]
            strikes = sorted(df_filtrado['strike'].dropna().unique())
        
        logger.info(f"Encontrados {len(strikes)} strikes")
        return strikes
        
    except Exception as e:
        logger.error(f"Error en strikes: {e}")
        return []


def generate_bar_chart(hoja: str, fecha_extraccion: str, fecha_vencimiento: str):
    """Genera gráfico de barras"""
    logger.info(f"Generando gráfico barras: {hoja}, {fecha_extraccion}, {fecha_vencimiento}")
    
    data_source = get_data_source()
    
    try:
        if data_source == "sqlite":
            try:
                fecha_ext_dt = datetime.strptime(fecha_extraccion, '%Y-%m-%d')
                fecha_ext_int = int(fecha_ext_dt.strftime('%Y%m%d'))
                fecha_ven_dt = datetime.strptime(fecha_vencimiento, '%Y-%m-%d')
                fecha_ven_int = int(fecha_ven_dt.strftime('%Y%m%d'))
            except ValueError:
                fecha_ext_int = fecha_extraccion
                fecha_ven_int = fecha_vencimiento
            
            query = '''
                SELECT strike, call_oi, put_oi
                FROM open_interest 
                WHERE asset = ? AND extraction_date = ? AND expiration_date = ?
                ORDER BY strike ASC
            '''
            results = execute_sql_query(query, (hoja, fecha_ext_int, fecha_ven_int))
            df = pd.DataFrame(results)
        else:
            df = get_excel_data(hoja)
            fecha_ext_obj = pd.to_datetime(fecha_extraccion)
            fecha_ven_obj = pd.to_datetime(fecha_vencimiento)
            df = df[
                (df['extraction_date'] == fecha_ext_obj) & 
                (df['expiration_date'] == fecha_ven_obj)
            ]
        
        if df.empty:
            return None, None
        
        img_base64 = generate_bar_chart_image(df, fecha_vencimiento, fecha_extraccion, hoja)
        table_data = df[['strike', 'call_oi', 'put_oi']].to_dict('records')
        
        logger.info(f"Gráfico generado: {len(df)} registros")
        return img_base64, table_data
        
    except Exception as e:
        logger.error(f"Error generando gráfico: {e}")
        return None, None


def generate_evolution_chart(hoja: str, fecha_vencimiento: str, strike: float):
    """Genera gráfico de evolución"""
    logger.info(f"Generando gráfico evolución: {hoja}, {fecha_vencimiento}, {strike}")
    
    data_source = get_data_source()
    
    try:
        if data_source == "sqlite":
            try:
                fecha_ven_dt = datetime.strptime(fecha_vencimiento, '%Y-%m-%d')
                fecha_ven_int = int(fecha_ven_dt.strftime('%Y%m%d'))
            except ValueError:
                fecha_ven_int = fecha_vencimiento
            
            query = '''
                SELECT extraction_date, call_oi, put_oi
                FROM open_interest 
                WHERE asset = ? AND expiration_date = ? AND strike = ?
                ORDER BY extraction_date ASC
            '''
            results = execute_sql_query(query, (hoja, fecha_ven_int, strike))
            df = pd.DataFrame(results)
            
            if not df.empty and 'extraction_date' in df.columns:
                df['extraction_date'] = pd.to_datetime(
                    df['extraction_date'].astype(str), 
                    format='%Y%m%d', 
                    errors='coerce'
                )
        else:
            df = get_excel_data(hoja)
            fecha_ven_obj = pd.to_datetime(fecha_vencimiento)
            df = df[
                (df['expiration_date'] == fecha_ven_obj) & 
                (df['strike'] == strike)
            ]
        
        if df.empty:
            return None, None
        
        img_base64 = generate_evolution_chart_image(df, fecha_vencimiento, strike, hoja)
        
        df_copy = df.copy()
        if 'extraction_date' in df_copy.columns:
            df_copy['extraction_date'] = df_copy['extraction_date'].dt.strftime('%Y-%m-%d')
        
        historical_data = df_copy[['extraction_date', 'call_oi', 'put_oi']].to_dict('records')
        
        logger.info(f"Gráfico evolución generado: {len(df)} registros")
        return img_base64, historical_data
        
    except Exception as e:
        logger.error(f"Error generando gráfico evolución: {e}")
        return None, None


def generate_bar_chart_image(df: pd.DataFrame, expiration_date: str, extraction_date: str, asset: str) -> str:
    """Genera imagen del gráfico de barras con números de OI"""
    try:
        plt.ioff()
        
        top_calls = df.nlargest(10, 'call_oi')
        top_puts = df.nlargest(10, 'put_oi')
        strikes_top = pd.concat([top_calls, top_puts]).drop_duplicates(subset='strike').sort_values('strike')
        
        fig, ax = plt.subplots(figsize=(12, 7))
        
        if strikes_top.empty:
            ax.text(0.5, 0.5, 'No hay datos suficientes', ha='center', va='center', transform=ax.transAxes)
        else:
            bar_labels = [str(s) for s in strikes_top['strike'].values]
            call_values = -strikes_top['call_oi'].values
            put_values = strikes_top['put_oi'].values
            
            # Calcular buffer para el eje X
            max_call = abs(call_values).max()
            max_put = put_values.max()
            max_oi = max(max_call, max_put)
            x_buffer = max_oi * 0.2
            
            # Dibujar barras
            bars_call = ax.barh(bar_labels, call_values, color='green', alpha=0.7, label='CALL OI')
            bars_put = ax.barh(bar_labels, put_values, color='red', alpha=0.7, label='PUT OI')
            
            # Añadir valores CALL
            for bar in bars_call:
                width = bar.get_width()
                if width != 0:
                    ax.annotate(
                        f'{abs(int(width)):,}',
                        xy=(width, bar.get_y() + bar.get_height() / 2),
                        xytext=(-5, 0),
                        textcoords="offset points",
                        ha='right',
                        va='center',
                        color='#2E7D32',
                        fontsize=10,
                    )
            
            # Añadir valores PUT
            for bar in bars_put:
                width = bar.get_width()
                if width != 0:
                    ax.annotate(
                        f'{int(width):,}',
                        xy=(width, bar.get_y() + bar.get_height() / 2),
                        xytext=(5, 0),
                        textcoords="offset points",
                        ha='left',
                        va='center',
                        color='#C62828',
                        fontsize=10,
                    )
            
            ax.set_xlim(-max_call - x_buffer, max_put + x_buffer)
            ax.axvline(0, color='black', linewidth=1)
            ax.set_xlabel('Open Interest')
            ax.set_ylabel('Strike Price')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        plt.suptitle(f"{asset} - Open Interest\nVencimiento: {expiration_date} | Extracción: {extraction_date}")
        fig.tight_layout()
        
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close(fig)
        
        return img_base64
        
    except Exception as e:
        logger.error(f"Error generando imagen de barras: {e}")
        return ""

def generate_evolution_chart_image(df: pd.DataFrame, expiration_date: str, strike: float, asset: str) -> str:
    """Genera imagen del gráfico de evolución con números de OI"""
    try:
        plt.ioff()
        
        df = df.sort_values('extraction_date')
        fig, ax = plt.subplots(figsize=(12, 8))
        
        dates = df['extraction_date']
        call_oi = df['call_oi']
        put_oi = df['put_oi']
        
        # Dibujar líneas
        ax.plot(dates, call_oi, marker='o', linewidth=2, color='green', label='CALL OI', markersize=6)
        ax.plot(dates, put_oi, marker='s', linewidth=2, color='red', label='PUT OI', markersize=6)
        
        # Agregar valores en los puntos
        for i, (date, call_val, put_val) in enumerate(zip(dates, call_oi, put_oi)):
            ax.annotate(f'{int(call_val):,}', 
                       xy=(date, call_val), 
                       xytext=(0, 10), 
                       textcoords="offset points",
                       ha='center', va='bottom',
                       fontsize=10, color='#2E7D32',
                       bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.7))
            
            ax.annotate(f'{int(put_val):,}', 
                       xy=(date, put_val), 
                       xytext=(0, -15), 
                       textcoords="offset points",
                       ha='center', va='top',
                       fontsize=10, color='#C62828',
                       bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.7))
        
        ax.set_xlabel('Fecha de Extracción')
        ax.set_ylabel('Open Interest')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.suptitle(f"{asset} - Evolución Open Interest\nStrike: {strike} | Vencimiento: {expiration_date}")
        plt.xticks(rotation=45)
        fig.tight_layout()
        
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close(fig)
        
        return img_base64
        
    except Exception as e:
        logger.error(f"Error generando imagen de evolución: {e}")
        return ""

def get_fechas_vencimiento_oex(hoja: str):
    """Obtiene fechas de vencimiento desde OEX"""
    try:
        oex_file_path = "OEX_cierre_semanal.xlsx"
        fechas_oex = []
        
        if os.path.exists(oex_file_path):
            try:
                oex_df = pd.read_excel(oex_file_path, sheet_name="precios_cierre_oesx")
                oex_df['Vencimiento_Date'] = oex_df['Vencimiento'].str.replace('OESX ', '')
                oex_df['Vencimiento_Date'] = pd.to_datetime(oex_df['Vencimiento_Date'], errors='coerce')
                fechas_oex = oex_df['Vencimiento_Date'].dropna().unique()
                logger.info(f"OEX datos cargados: {len(fechas_oex)} fechas de vencimiento encontradas")
            except Exception as e:
                logger.warning(f"No se pudo cargar OEX_cierre_semanal: {e}")
                return None
        else:
            logger.warning("Archivo OEX_cierre_semanal.xlsx no encontrado")
            return None
        
        if len(fechas_oex) == 0:
            return None
        
        data_source = get_data_source()
        fechas_comunes = []
        
        if data_source == "sqlite":
            asset = hoja.upper().strip()
            
            query = '''
                SELECT DISTINCT expiration_date 
                FROM open_interest 
                WHERE asset = ? 
                ORDER BY expiration_date ASC
            '''
            results = execute_sql_query(query, (asset,))
            db_dates = [row['expiration_date'] for row in results]
            
            db_dates_dt = []
            for date_val in db_dates:
                if isinstance(date_val, (int, np.integer)):
                    date_str = str(date_val)
                    if len(date_str) == 8:
                        try:
                            dt = datetime.strptime(date_str, '%Y%m%d')
                            db_dates_dt.append(dt)
                        except ValueError:
                            continue
                elif isinstance(date_val, (datetime, pd.Timestamp)):
                    db_dates_dt.append(date_val)
            
            for oex_date in fechas_oex:
                for db_date in db_dates_dt:
                    if oex_date.date() == db_date.date():
                        fechas_comunes.append(oex_date)
                        break
        else:
            df = get_excel_data(hoja)
            db_dates = sorted(df['expiration_date'].dropna().unique())
            
            for oex_date in fechas_oex:
                for db_date in db_dates:
                    if isinstance(db_date, (datetime, pd.Timestamp)) and oex_date.date() == db_date.date():
                        fechas_comunes.append(oex_date)
                        break
        
        date_strings = []
        for date_val in sorted(fechas_comunes):
            date_strings.append(date_val.strftime('%Y-%m-%d'))
        
        logger.info(f"Encontradas {len(date_strings)} fechas de vencimiento comunes")
        
        if len(date_strings) == 0:
            return None
        
        return date_strings
        
    except Exception as e:
        logger.error(f"Error obteniendo fechas de vencimiento OEX: {e}")
        return None


def get_detalle_vencimiento(hoja: str, fecha_vencimiento: str):
    """Obtiene detalle de vencimiento"""
    try:
        oex_data = None
        oex_file_path = "OEX_cierre_semanal.xlsx"
        
        if os.path.exists(oex_file_path):
            try:
                oex_df = pd.read_excel(oex_file_path, sheet_name="precios_cierre_oesx")
                oex_df['Vencimiento_Date'] = oex_df['Vencimiento'].str.replace('OESX ', '')
                oex_df['Vencimiento_Date'] = pd.to_datetime(oex_df['Vencimiento_Date'], errors='coerce')
                oex_data = oex_df
                logger.info(f"OEX datos cargados: {len(oex_df)} registros")
            except Exception as e:
                logger.warning(f"No se pudo cargar OEX_cierre_semanal: {e}")
        
        data_source = get_data_source()
        
        if data_source == "sqlite":
            try:
                fecha_ven_dt = datetime.strptime(fecha_vencimiento, '%Y-%m-%d')
                fecha_ven_int = int(fecha_ven_dt.strftime('%Y%m%d'))
            except ValueError:
                fecha_ven_int = fecha_vencimiento
            
            asset = hoja.upper().strip()
            
            query = '''
                SELECT extraction_date, expiration_date, strike, call_oi, put_oi
                FROM open_interest 
                WHERE asset = ? AND expiration_date = ?
                ORDER BY extraction_date DESC, strike ASC
            '''
            results = execute_sql_query(query, (asset, fecha_ven_int))
            db_data = pd.DataFrame(results)
            
            if not db_data.empty:
                db_data['extraction_date'] = pd.to_datetime(
                    db_data['extraction_date'].astype(str), 
                    format='%Y%m%d', 
                    errors='coerce'
                )
                db_data['expiration_date'] = pd.to_datetime(
                    db_data['expiration_date'].astype(str), 
                    format='%Y%m%d', 
                    errors='coerce'
                )
        else:
            df = get_excel_data(hoja)
            fecha_ven_obj = pd.to_datetime(fecha_vencimiento)
            db_data = df[df['expiration_date'] == fecha_ven_obj]
        
        if db_data.empty:
            return None
        
        strikes_eurostoxx = db_data[
            (db_data['strike'] >= 4500) & 
            (db_data['strike'] <= 6500)
        ]
        
        if strikes_eurostoxx.empty:
            strikes_eurostoxx = db_data
            logger.info("Usando todos los datos disponibles")
        else:
            logger.info(f"Filtrado EUROSTOXX: {len(strikes_eurostoxx)} de {len(db_data)} registros")
        
        info_oex = {}
        if oex_data is not None:
            fecha_busqueda = pd.to_datetime(fecha_vencimiento)
            coincidencias = oex_data[oex_data['Vencimiento_Date'] == fecha_busqueda]
            
            if not coincidencias.empty:
                info_oex = coincidencias.iloc[0].to_dict()
                info_oex = {k: v for k, v in info_oex.items() if pd.notna(v)}
                if 'Dia de la semana' in info_oex:
                    info_oex['Día de la semana'] = info_oex.pop('Dia de la semana')
                if 'Tipo Vencimiento' in info_oex:
                    info_oex['Tipo de vencimiento'] = info_oex.pop('Tipo Vencimiento')
        
        datos_eurostoxx = []
        for _, row in strikes_eurostoxx.iterrows():
            datos_eurostoxx.append({
                "Fecha de Extracción": row['extraction_date'].strftime('%Y-%m-%d') if pd.notna(row['extraction_date']) else 'N/A',
                "Expiration Date": row['expiration_date'].strftime('%Y-%m-%d') if pd.notna(row['expiration_date']) else 'N/A',
                "Strike": float(row['strike']) if pd.notna(row['strike']) else 0,
                "Call Open Interest": int(row['call_oi']) if pd.notna(row['call_oi']) else 0,
                "Put Open Interest": int(row['put_oi']) if pd.notna(row['put_oi']) else 0
            })
        
        total_call_oi = strikes_eurostoxx['call_oi'].sum()
        total_put_oi = strikes_eurostoxx['put_oi'].sum()
        total_oi = total_call_oi + total_put_oi
        
        return {
            "fecha_vencimiento": fecha_vencimiento,
            "datos_eurostoxx": datos_eurostoxx,
            "informacion_oex": info_oex,
            "estadisticas": {
                "total_call_oi": int(total_call_oi),
                "total_put_oi": int(total_put_oi),
                "total_oi": int(total_oi),
                "ratio_put_call": round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0,
                "strikes_disponibles": len(strikes_eurostoxx['strike'].unique()),
                "rango_strikes": f"{strikes_eurostoxx['strike'].min():.0f} - {strikes_eurostoxx['strike'].max():.0f}",
                "total_registros_filtrados": len(strikes_eurostoxx),
                "total_registros_original": len(db_data)
            },
            "total_registros": len(datos_eurostoxx)
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo detalle vencimiento: {e}")
        return None

def upload_excel_file(uploaded_file):
    """Carga archivo Excel"""
    try:
        contents = uploaded_file.getvalue()
        
        # Crear backup si existe BD previa
        if os.path.exists(DB_PATH):
            backup_dir = "backups"
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"base_datos_backup_{timestamp}.db"
            backup_path = os.path.join(backup_dir, backup_filename)
            shutil.copyfile(DB_PATH, backup_path)
            logger.info(f"Backup creado: {backup_path}")
            # ELIMINAR la BD antigua para evitar conflictos
            os.remove(DB_PATH)
        
        # Guardar Excel
        with open(EXCEL_PATH, "wb") as f:
            f.write(contents)
        
        # Crear nueva BD desde Excel
        success = create_sqlite_from_excel()
        
        if not success:
            return None
        
        conn = get_database_connection()
        cursor = conn.execute("SELECT DISTINCT asset FROM open_interest ORDER BY asset")
        assets = [row['asset'] for row in cursor.fetchall()]
        conn.close()
        
        get_estado.clear()
        get_fechas_extraccion.clear()
        get_fechas_vencimiento.clear()
        get_strikes.clear()
        mostrar_mensaje("success", f"Archivo {uploaded_file.name} cargado exitosamente. Base de datos SQLite creada.")
        time.sleep(2)
        return {
            "mensaje": f"Archivo {uploaded_file.name} cargado exitosamente. Base de datos SQLite creada.",
            "hojas_disponibles": assets,
            "hoja_activa": assets[0] if assets else None
        }
        
    except Exception as e:
        logger.error(f"Error cargando Excel: {e}")
        try:
            if os.path.exists(EXCEL_PATH):
                os.remove(EXCEL_PATH)
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
        except:
            pass
        return None
    

def upload_csv_file(uploaded_csv, hoja_destino: str, fecha_extraccion: str):
    """Carga archivo CSV - con conversión especial para SP500"""
    try:
        data_source = get_data_source()
        if data_source != "sqlite":
            return None
        
        # SOLO cargar calendario si es SP500 y BUND
        calendario = None
        if hoja_destino.upper() in ["SP500", "BUND"]:
            calendario = cargar_calendario_vencimientos()
            if not calendario:
                mostrar_mensaje("warning", f"No se encontró calendario de vencimientos para {hoja_destino}")
            else:
                logger.info(f"Calendario cargado para {hoja_destino} con {len(calendario)} fechas")

        contents = uploaded_csv.getvalue()
        
        # Leer CSV
        try:
            # Intentar leer con diferentes encodings
            try:
                df_csv = pd.read_csv(io.BytesIO(contents), skiprows=3, header=0, sep=',')
            except:
                # Intentar con encoding diferente
                df_csv = pd.read_csv(io.BytesIO(contents), skiprows=3, header=0, sep=',', encoding='latin-1')
        except Exception as e:
            logger.error(f"Error leyendo CSV: {e}")
            mostrar_mensaje("error", f"Error leyendo CSV: {str(e)}")
            return None
        
        # Verificar formato esperado
        expected_columns = ['Expiration Date', 'Open Interest', 'Strike', 'Open Interest.1']
        if not all(col in df_csv.columns for col in expected_columns):
            mostrar_mensaje("error", "Formato de CSV incorrecto")
            logger.error(f"Columnas encontradas: {df_csv.columns.tolist()}")
            return None
        
        # Renombrar columnas
        df_csv = df_csv[expected_columns]
        df_csv.columns = ['Expiration Date', 'Call Open Interest', 'Strike', 'Put Open Interest']
        
        logger.info(f"CSV cargado: {len(df_csv)} registros para {hoja_destino}")
        
        # CONVERTIR FECHAS DE VENCIMIENTO SOLO PARA SP500
        if hoja_destino.upper() in ["SP500", "BUND"] and calendario:
            logger.info(f"Aplicando conversión de fechas para {hoja_destino}...")
            fechas_convertidas = []
            
            for fecha_venc in df_csv['Expiration Date']:
                fecha_str = str(fecha_venc).strip().upper()
                
                # Limpiar el string
                fecha_str = fecha_str.replace('  ', ' ').replace('"', '').replace("'", "")
                
                # Intentar diferentes formatos
                fecha_dt = None
                
                # Formato 1: "DEC 2025" o "DIC 2025"
                if fecha_dt is None and len(fecha_str.split()) >= 2:
                    partes = fecha_str.split()
                    mes_abrev = partes[0][:3].upper()  # Primeras 3 letras
                    try:
                        año = int(partes[1])
                        clave = f"{mes_abrev} {año}"
                        
                        if clave in calendario:
                            fecha_dt = calendario[clave]
                            logger.debug(f"Convertido {fecha_str} -> {fecha_dt}")
                    except (ValueError, IndexError):
                        pass
                
                # Formato 2: "DICIEMBRE 2025"
                if fecha_dt is None and len(fecha_str.split()) >= 2:
                    partes = fecha_str.split()
                    mes_completo = partes[0]
                    try:
                        año = int(partes[1])
                        clave = f"{mes_completo} {año}"
                        
                        if clave in calendario:
                            fecha_dt = calendario[clave]
                    except (ValueError, IndexError):
                        pass
                
                # Formato 3: Ya es datetime
                if fecha_dt is None:
                    try:
                        fecha_dt = pd.to_datetime(fecha_str, errors='coerce')
                    except:
                        pass
                
                # Si no se pudo convertir, usar fecha actual + 30 días como fallback
                if fecha_dt is None or pd.isna(fecha_dt):
                    logger.warning(f"No se pudo convertir fecha: {fecha_str}")
                    fecha_dt = pd.to_datetime(fecha_extraccion) + pd.Timedelta(days=30)
                
                fechas_convertidas.append(fecha_dt)
            
            # Reemplazar columna con fechas convertidas
            df_csv['Expiration Date'] = fechas_convertidas
            
            # Verificar conversiones
            fechas_unicas = df_csv['Expiration Date'].dt.strftime('%Y-%m-%d').unique()
            logger.info(f"Fechas de vencimiento convertidas para {hoja_destino}: {fechas_unicas}")

            if len(fechas_unicas) > 0:
                mostrar_mensaje("info", f"📅 {hoja_destino} - Fechas convertidas: {', '.join(fechas_unicas[:3])}")

             
        else:
            # Para otros assets, solo convertir a datetime normal
            logger.info(f"Conversión normal para {hoja_destino}")
            df_csv['Expiration Date'] = pd.to_datetime(df_csv['Expiration Date'], errors='coerce')
        
        # Agregar fecha de extracción
        fecha_dt = pd.to_datetime(fecha_extraccion)
        df_csv.insert(0, 'Fecha de Extracción', fecha_dt.date())
        
        logger.info(f"Fecha extracción: {fecha_extraccion}")
        
        # CREAR BACKUP DEL EXCEL ANTES DE MODIFICAR
        if os.path.exists(EXCEL_PATH):
            backup_dir = "backups"
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"base_datos_backup_{timestamp}.xlsx"
            backup_path = os.path.join(backup_dir, backup_filename)
            shutil.copyfile(EXCEL_PATH, backup_path)
            logger.info(f"Backup Excel creado: {backup_filename}")
            mostrar_mensaje("success", f"Backup Excel creado: {backup_filename}")
        
        # Preparar DataFrame para inserción en BD
        df_csv['extraction_date'] = int(fecha_dt.strftime('%Y%m%d'))
        
        # Asegurar que Expiration Date sea datetime
        if not pd.api.types.is_datetime64_any_dtype(df_csv['Expiration Date']):
            df_csv['Expiration Date'] = pd.to_datetime(df_csv['Expiration Date'], errors='coerce')
        
        df_csv['expiration_date'] = df_csv['Expiration Date'].dt.strftime('%Y%m%d').astype(int)
        df_csv['strike'] = pd.to_numeric(df_csv['Strike'], errors='coerce')
        df_csv['call_oi'] = pd.to_numeric(df_csv['Call Open Interest'], errors='coerce').fillna(0).astype(int)
        df_csv['put_oi'] = pd.to_numeric(df_csv['Put Open Interest'], errors='coerce').fillna(0).astype(int)
        df_csv['asset'] = hoja_destino.upper().strip()
        
        # Eliminar filas con datos inválidos
        df_csv = df_csv.dropna(subset=['strike', 'expiration_date'])
        
        # Seleccionar columnas necesarias para BD
        df_insert = df_csv[['asset', 'extraction_date', 'expiration_date', 'strike', 'call_oi', 'put_oi']]
        
        # Eliminar duplicados
        df_insert = df_insert.drop_duplicates(subset=['asset', 'extraction_date', 'expiration_date', 'strike'])
        
        if df_insert.empty:
            logger.error("No hay datos válidos para insertar")
            mostrar_mensaje("error", "No hay datos válidos para insertar")
            return None
        
        # Insertar en SQLite
        conn = sqlite3.connect(DB_PATH)
        init_sqlite_pragmas(conn)
        
        try:
            # Verificar duplicados
            fecha_int = df_insert['extraction_date'].iloc[0]
            
            query = '''
                SELECT extraction_date, expiration_date 
                FROM open_interest 
                WHERE asset = ?
            '''
            cursor = conn.execute(query, (hoja_destino.upper().strip(),))
            existing_data = pd.DataFrame(cursor.fetchall(), columns=['extraction_date', 'expiration_date'])
            
            if not existing_data.empty:
                combinaciones_existentes = existing_data[['extraction_date', 'expiration_date']].drop_duplicates()
                nuevas_combinaciones = df_insert[['extraction_date', 'expiration_date']].drop_duplicates()
                
                conflictivas = nuevas_combinaciones.merge(
                    combinaciones_existentes, 
                    on=['extraction_date', 'expiration_date'], 
                    how='inner'
                )
                

                if not conflictivas.empty:
                    num_duplicados = len(df_insert.merge(conflictivas, on=['extraction_date', 'expiration_date'], how='inner'))
                    logger.error(f"Ya existen datos para esa combinación en '{hoja_destino}'")
                    mostrar_mensaje("warning", f"Ya existen datos para algunas fechas en '{hoja_destino}'")
                    
                    # Insertar solo los no duplicados
                    df_insert = df_insert.merge(
                        conflictivas, 
                        on=['extraction_date', 'expiration_date'], 
                        how='left', 
                        indicator=True
                    )
                    df_insert = df_insert[df_insert['_merge'] == 'left_only'].drop(columns=['_merge'])
                    
                    # Si no quedan registros para insertar, mostrar popup y retornar
                    if df_insert.empty:
                        conn.close()
                        return {
                            "error": True,
                            "mensaje": f"Todos los registros ({num_duplicados}) ya existían en la base de datos",
                            "mostrar_popup": True,
                            "popup_titulo": "⚠️ Datos Duplicados",
                            "popup_mensaje": f"Todos los {num_duplicados} registros ya existían en '{hoja_destino}'"
                        }







            
            # Insertar datos
            if not df_insert.empty:
                df_insert.to_sql("open_interest", conn, if_exists='append', index=False)
                registros_agregados = len(df_insert)
                conn.commit()
                conn.execute("ANALYZE;")
                logger.info(f"CSV insertado en SQLite: {registros_agregados} registros")
                
                # Mostrar mensaje específico según asset


                if hoja_destino.upper() in ["SP500", "BUND"]:
                    mostrar_mensaje("success", f"{hoja_destino}: {registros_agregados} registros cargados con conversión de fechas")
                    # Opcional: mostrar popup también
                    mostrar_popup("✅ CSV Cargado", 
                                f"{registros_agregados} registros agregados a '{hoja_destino}'", 
                                tipo="success")
                else:
                    mostrar_mensaje("success", f"{hoja_destino}: {registros_agregados} registros cargados")



            else:
                mostrar_mensaje("warning", "Todos los registros ya existían en la base de datos")
                conn.close()
                return {
                    "error": True,
                    "mensaje": "Todos los registros ya existían en la base de datos"
                }
                
        finally:
            conn.close()
        
        # ACTUALIZAR EL ARCHIVO EXCEL
        try:
            if os.path.exists(EXCEL_PATH):
                # Preparar DataFrame para Excel (formato estándar)
                df_excel = df_csv[['Fecha de Extracción', 'Expiration Date', 'Strike', 
                                   'Call Open Interest', 'Put Open Interest']].copy()
                
                # Formatear fechas para Excel
                df_excel['Fecha de Extracción'] = pd.to_datetime(df_excel['Fecha de Extracción']).dt.date
                df_excel['Expiration Date'] = pd.to_datetime(df_excel['Expiration Date']).dt.date
                
                # Leer todas las hojas existentes
                hojas_excel = {}
                try:
                    excel_file = pd.ExcelFile(EXCEL_PATH)
                    for sheet_name in excel_file.sheet_names:
                        if sheet_name != hoja_destino:
                            hojas_excel[sheet_name] = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)
                except Exception as e:
                    logger.warning(f"Error leyendo hojas existentes: {e}")
                
                # Leer y combinar la hoja destino
                if hoja_destino in pd.ExcelFile(EXCEL_PATH).sheet_names:
                    try:
                        base_actual = pd.read_excel(EXCEL_PATH, sheet_name=hoja_destino)
                        
                        # Asegurar formato estándar en datos existentes
                        if "Trade Date" in base_actual.columns:
                            base_actual = base_actual.rename(columns={
                                "Trade Date": "Fecha de Extracción",
                                "call_oi": "Call Open Interest",
                                "put_oi": "Put Open Interest"
                            })
                        
                        # Convertir fechas a formato común
                        base_actual['Expiration Date'] = pd.to_datetime(base_actual['Expiration Date']).dt.date
                        base_actual['Fecha de Extracción'] = pd.to_datetime(base_actual['Fecha de Extracción']).dt.date
                        
                        # Combinar datos
                        base_merged = pd.concat([base_actual, df_excel]).drop_duplicates(
                            ['Fecha de Extracción', 'Expiration Date', 'Strike'],
                            keep='last'
                        )
                    except Exception as e:
                        logger.warning(f"Error leyendo hoja {hoja_destino}: {e}")
                        base_merged = df_excel
                else:
                    base_merged = df_excel
                
                # Agregar la hoja actualizada
                hojas_excel[hoja_destino] = base_merged
                
                # Guardar todas las hojas en el Excel
                with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
                    for hoja, datos in hojas_excel.items():
                        datos.to_excel(writer, sheet_name=hoja, index=False)
                
                logger.info(f"Excel actualizado: {registros_agregados} registros agregados a hoja '{hoja_destino}'")
                mostrar_mensaje("success", f"Excel actualizado con {registros_agregados} registros")
                
        except Exception as e:
            logger.error(f"Error actualizando Excel: {e}")
            # No fallar si solo falla la actualización del Excel
            logger.warning("SQLite actualizado pero Excel no pudo actualizarse")
            mostrar_mensaje("warning", "SQLite actualizado pero hubo un error al actualizar Excel")
        
        # Limpiar caché
        get_fechas_extraccion.clear()
        get_fechas_vencimiento.clear()
        get_strikes.clear()
        
        return {
            "mensaje": f"CSV cargado exitosamente. {registros_agregados} registros agregados al asset '{hoja_destino}'",
            "registros_agregados": registros_agregados,
            "mostrar_popup": True,
            "popup_titulo": "✅ CSV Cargado",
            "popup_mensaje": f"{registros_agregados} registros agregados a '{hoja_destino}'"
        }
        
    except Exception as e:
        logger.error(f"Error cargando CSV: {e}")
        import traceback
        logger.error(traceback.format_exc())
        mostrar_mensaje("error", f"Error al cargar CSV: {str(e)}")
        return None


def verificar_base_datos():
    """Verifica estado de la base de datos"""
    data_source = get_data_source()
    
    if data_source == "sqlite":
        try:
            conn = get_database_connection()
            
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='open_interest'")
            if cursor.fetchone():
                cursor = conn.execute("SELECT DISTINCT asset FROM open_interest")
                assets = [row['asset'] for row in cursor.fetchall()]
                
                cursor = conn.execute("SELECT COUNT(*) as total FROM open_interest")
                total_registros = cursor.fetchone()['total']
                
                cursor = conn.execute("SELECT asset, COUNT(*) as count FROM open_interest GROUP BY asset")
                registros_por_asset = {row['asset']: row['count'] for row in cursor.fetchall()}
                
                conn.close()
                
                return {
                    "existe": True,
                    "tipo": "sqlite",
                    "esquema": "unificado",
                    "assets": assets,
                    "base_datos_activa": DB_PATH,
                    "estadisticas": {
                        "total_tablas": len(assets),
                        "total_registros": total_registros,
                        "registros_por_tabla": registros_por_asset
                    }
                }
            else:
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
                tables = [row['name'] for row in cursor.fetchall()]
                
                db_info = {}
                total_registros = 0
                for table in tables:
                    cursor = conn.execute(f'SELECT COUNT(*) as count FROM "{table}"')
                    count = cursor.fetchone()['count']
                    db_info[table] = count
                    total_registros += count
                
                conn.close()
                
                return {
                    "existe": True,
                    "tipo": "sqlite",
                    "esquema": "antiguo",
                    "tablas": tables,
                    "base_datos_activa": DB_PATH,
                    "estadisticas": {
                        "total_tablas": len(tables),
                        "total_registros": total_registros,
                        "registros_por_tabla": db_info
                    }
                }
        except Exception as e:
            return {
                "existe": False,
                "tipo": "error",
                "mensaje": f"Error accediendo a la base de datos: {str(e)}"
            }
    elif data_source == "excel":
        try:
            excel_file = pd.ExcelFile(EXCEL_PATH)
            hojas = excel_file.sheet_names
            return {
                "existe": False,
                "tipo": "excel",
                "archivo_excel": EXCEL_PATH,
                "hojas_disponibles": hojas,
                "mensaje": "Solo existe archivo Excel. Debe convertirlo a base de datos SQLite."
            }
        except Exception as e:
            return {
                "existe": False,
                "tipo": "excel_error", 
                "mensaje": f"Error leyendo archivo Excel: {str(e)}"
            }
    else:
        return {
            "existe": False,
            "tipo": "none",
            "mensaje": "No se encontró base de datos. Cargue un archivo Excel para comenzar."
        }


# ============================================================================
# INTERFAZ STREAMLIT (del frontend.py)
# ============================================================================


# ==== ESTADO INICIAL ====
if "nombre_base_activa" not in st.session_state:
    st.session_state["nombre_base_activa"] = None
if "nombre_hoja_excel" not in st.session_state:
    st.session_state["nombre_hoja_excel"] = "VIX"
if "hojas_disponibles" not in st.session_state:
    st.session_state["hojas_disponibles"] = []
if "confirmar_salida" not in st.session_state:
    st.session_state["confirmar_salida"] = False
if "menu_seleccionado" not in st.session_state:
    st.session_state["menu_seleccionado"] = "Visualización"
if "menu_counter" not in st.session_state:
    st.session_state["menu_counter"] = 0
if "filtros_visualizacion" not in st.session_state:
    st.session_state["filtros_visualizacion"] = None
if "resultado_visualizacion" not in st.session_state:
    st.session_state["resultado_visualizacion"] = None
if "filtros_estadisticas" not in st.session_state:
    st.session_state["filtros_estadisticas"] = None
if "resultado_estadisticas" not in st.session_state:
    st.session_state["resultado_estadisticas"] = None
if "acceso_cargar_datos" not in st.session_state:
    st.session_state["acceso_cargar_datos"] = False
if "intentos_password" not in st.session_state:
    st.session_state["intentos_password"] = 0
if "bloqueo_temporal" not in st.session_state:
    st.session_state["bloqueo_temporal"] = False

# ==== CARGA AUTOMÁTICA DE ESTADO ====
estado = get_estado()
if estado and estado["existe_base_datos"]:
    st.session_state["hojas_disponibles"] = estado["hojas_disponibles"]
    if not st.session_state.get("nombre_hoja_excel"):
        st.session_state["nombre_hoja_excel"] = estado["hoja_activa"]
    st.session_state["nombre_base_activa"] = "base_datos.xlsx"

# ==== CARGAR CSS ====
if os.path.exists("style.css"):
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ==== MENÚ PRINCIPAL ====
with st.sidebar:
    # Selector de hoja activa
    if st.session_state.get("hojas_disponibles"):
        st.markdown("<h4 style='color: white; margin-bottom: 0.5rem;'>📄 Selección de Asset Activo</h4>", unsafe_allow_html=True)
        
        hoja_seleccionada = st.selectbox(
            "Selecciona asset activo",
            st.session_state["hojas_disponibles"],
            index=st.session_state["hojas_disponibles"].index(
                st.session_state["nombre_hoja_excel"]
            ) if st.session_state["nombre_hoja_excel"] in st.session_state["hojas_disponibles"] else 0,
            key="selector_hoja_activa",
            label_visibility="collapsed"
        )
        
        # Detectar cambio de hoja
        if hoja_seleccionada != st.session_state["nombre_hoja_excel"]:
            st.session_state["nombre_hoja_excel"] = hoja_seleccionada
            
            # Limpiar cache de resultados
            st.session_state["filtros_visualizacion"] = None
            st.session_state["resultado_visualizacion"] = None
            st.session_state["filtros_estadisticas"] = None
            st.session_state["resultado_estadisticas"] = None
            
            # Limpiar cache de peticiones
            get_fechas_extraccion.clear()
            get_fechas_vencimiento.clear()
            get_strikes.clear()
            
            st.rerun()
    
    # Menú de navegación
    if not st.session_state.get("confirmar_salida", False):
        opciones_menu = ["Visualización", "Estadísticas", "Vencimientos", "Cargar Datos", "Configuración"]
        
        menu_key = f"menu_principal_{st.session_state.get('menu_counter', 0)}"
        
        try:
            indice_actual = opciones_menu.index(st.session_state["menu_seleccionado"])
        except ValueError:
            indice_actual = 0
            st.session_state["menu_seleccionado"] = opciones_menu[0]
        
        selected = option_menu(
            "Menú Principal",
            opciones_menu,
            icons=["bar-chart", "graph-up", "calendar", "upload", "gear"],
            menu_icon="cast",
            default_index=indice_actual,
            key=menu_key
        )
        
        # Detectar cambio
        if selected != st.session_state["menu_seleccionado"]:
            st.session_state["menu_seleccionado"] = selected
            st.session_state["menu_counter"] = st.session_state.get("menu_counter", 0) + 1
            st.rerun()
    else:
        selected = st.session_state.get("menu_seleccionado", "Visualización")
    
    # ==== BOTÓN DE SALIR ====
    st.markdown("---")
    
    if st.session_state.get("confirmar_salida", False):
        st.markdown("#### ⚠️ ¿Seguro que desea salir?")
        col_si, col_no = st.columns(2)
        
        with col_si:
            if st.button("✅ Sí, salir", type="primary", width="stretch", key="btn_confirmar_salir"):
                st.balloons()
                st.success("¡Cerrando aplicación!")
                
                st.markdown("""
                <div style="text-align: center; margin-top: 20px; padding: 20px; 
                background-color: #f0f2f6; border-radius: 10px;">
                <h3>✅ Aplicación cerrada correctamente</h3>
                <p><strong>Para cerrar completamente:</strong></p>
                <p>Cierre esta pestaña del navegador</p>
                </div>
                """, unsafe_allow_html=True)
                
                os._exit(0)
        
        with col_no:
            if st.button("❌ Cancelar", width="stretch", key="btn_cancelar_salir"):
                st.session_state["confirmar_salida"] = False
    else:
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            if st.button("🚪 Salir", type="secondary", width="stretch", key="btn_salir_sidebar"):
                st.session_state["confirmar_salida"] = True


# ============================================================================
# SISTEMA UNIFICADO DE MENSAJES Y POP-UPS
# ============================================================================

def mostrar_mensaje(tipo: str, texto: str, duracion: int = 4):
    """
    Muestra mensajes toast (notificaciones emergentes)
    Tipos: success, warning, error, info
    """
    iconos = {
        "success": "✅",
        "warning": "⚠️",
        "error": "❌",
        "info": "ℹ️"
    }
    
    icono = iconos.get(tipo, "ℹ️")
    
    # Usar st.toast() si está disponible (Streamlit >= 1.28)
    try:
        st.toast(f"{icono} {texto}", icon=icono)
    except:
        # Fallback para versiones anteriores
        if tipo == "success":
            st.success(texto)
        elif tipo == "warning":
            st.warning(texto)
        elif tipo == "error":
            st.error(texto)
        else:
            st.info(texto)

def mostrar_popup(titulo: str, mensaje: str, tipo: str = "info", tiempo_auto_cierre: int = 3000):
    """
    Muestra un pop-up modal usando JavaScript
    Tipos: success, warning, error, info, question
    """
    colores = {
        "success": "#4CAF50",
        "warning": "#FF9800",
        "error": "#F44336",
        "info": "#2196F3",
        "question": "#9C27B0"
    }
    
    iconos = {
        "success": "✅",
        "warning": "⚠️",
        "error": "❌",
        "info": "ℹ️",
        "question": "❓"
    }
    
    color = colores.get(tipo, "#2196F3")
    icono = iconos.get(tipo, "ℹ️")
    
    # Crear popup con JavaScript
    js_code = f"""
    <script>
    // Crear overlay
    const overlay = document.createElement('div');
    overlay.id = 'custom-popup-overlay';
    overlay.style.position = 'fixed';
    overlay.style.top = '0';
    overlay.style.left = '0';
    overlay.style.width = '100%';
    overlay.style.height = '100%';
    overlay.style.backgroundColor = 'rgba(0,0,0,0.5)';
    overlay.style.zIndex = '9998';
    overlay.style.display = 'flex';
    overlay.style.justifyContent = 'center';
    overlay.style.alignItems = 'center';
    overlay.style.backdropFilter = 'blur(3px)';
    
    // Crear popup
    const popup = document.createElement('div');
    popup.style.backgroundColor = 'white';
    popup.style.padding = '25px 35px';
    popup.style.borderRadius = '12px';
    popup.style.boxShadow = '0 10px 40px rgba(0,0,0,0.3)';
    popup.style.zIndex = '9999';
    popup.style.textAlign = 'center';
    popup.style.border = '3px solid {color}';
    popup.style.maxWidth = '450px';
    popup.style.margin = '20px';
    popup.style.animation = 'fadeIn 0.3s ease-out';
    
    // Contenido del popup
    popup.innerHTML = `
        <div style="font-size: 52px; margin-bottom: 15px; color: {color};">{icono}</div>
        <h2 style="color: #333; margin-bottom: 12px; font-size: 22px;">{titulo}</h2>
        <p style="color: #555; font-size: 16px; line-height: 1.5;">{mensaje}</p>
        <button id="popup-ok-btn" 
                style="margin-top: 25px; padding: 12px 30px; background: {color}; 
                       color: white; border: none; border-radius: 6px; 
                       cursor: pointer; font-size: 16px; font-weight: bold;
                       transition: all 0.2s;">
            OK
        </button>
    `;
    
    // Estilos de animación
    const style = document.createElement('style');
    style.innerHTML = `
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: scale(0.9); }}
            to {{ opacity: 1; transform: scale(1); }}
        }}
        #popup-ok-btn:hover {{
            transform: scale(1.05);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }}
    `;
    document.head.appendChild(style);
    
    overlay.appendChild(popup);
    document.body.appendChild(overlay);
    
    // Evento para cerrar
    document.getElementById('popup-ok-btn').onclick = function() {{
        overlay.remove();
    }};
    
    // Cerrar al hacer clic fuera del popup
    overlay.onclick = function(e) {{
        if (e.target === overlay) {{
            overlay.remove();
        }}
    }};
    
    // Auto-cierre después de X segundos
    setTimeout(() => {{
        if (document.body.contains(overlay)) {{
            overlay.remove();
        }}
    }}, {tiempo_auto_cierre});
    </script>
    """
    
    st.markdown(js_code, unsafe_allow_html=True)

def mostrar_alerta_simple(mensaje: str):
    """
    Muestra una alerta simple del navegador (útil para mensajes críticos)
    """
    js_code = f"""
    <script>
    alert("{mensaje}");
    </script>
    """
    st.markdown(js_code, unsafe_allow_html=True)


# ==== CONTENIDO PRINCIPAL ====

if not st.session_state.get("confirmar_salida", False):
    
    # ==== VISUALIZACIÓN ====
    if selected == "Visualización":
        st.markdown("<h2 class='fade-in'>Visualización</h2>", unsafe_allow_html=True)
        
        if not st.session_state.get("hojas_disponibles"):
            st.info("No se ha cargado ninguna base de datos. Por favor, use la opción 'Cargar Datos'.")
            st.stop()
        
        hoja_actual = st.session_state["nombre_hoja_excel"]
        
        with st.spinner("Cargando fechas de extracción..."):
            fechas_extraccion = get_fechas_extraccion(hoja_actual)
        
        if not fechas_extraccion:
            st.warning("No hay datos disponibles en este asset.")
            st.stop()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fecha_extraccion = st.selectbox(
                "Seleccione fecha de extracción:",
                fechas_extraccion,
                key="fecha_extraccion_vis"
            )
        
        with st.spinner("Cargando fechas de vencimiento..."):
            fechas_vencimiento = get_fechas_vencimiento(hoja_actual, fecha_extraccion)
        
        if not fechas_vencimiento:
            st.warning("No hay fechas de vencimiento disponibles.")
            st.stop()
        
        with col2:
            fecha_vencimiento = st.selectbox(
                "Seleccione fecha de vencimiento:",
                fechas_vencimiento,
                key="fecha_vencimiento_vis"
            )
        
        # Generación automática
        filtros_actuales = (hoja_actual, fecha_extraccion, fecha_vencimiento)
        
        if (st.session_state.get("filtros_visualizacion") != filtros_actuales or 
            st.session_state.get("resultado_visualizacion") is None):
            
            with st.spinner("Generando gráfico..."):
                img_base64, table_data = generate_bar_chart(hoja_actual, fecha_extraccion, fecha_vencimiento)
                
                if img_base64:
                    st.session_state["resultado_visualizacion"] = {
                        "imagen_base64": img_base64,
                        "datos_tabla": table_data
                    }
                    st.session_state["filtros_visualizacion"] = filtros_actuales
                else:
                    st.session_state["resultado_visualizacion"] = None
        
        resultado = st.session_state.get("resultado_visualizacion")
        if resultado:
            st.subheader(f"{hoja_actual} - Open Interest\nExtracción: {fecha_extraccion} | Vencimiento: {fecha_vencimiento}")
            
            try:
                img_data = base64.b64decode(resultado["imagen_base64"])
                st.image(img_data, width="stretch")
                
                file_name = f"{hoja_actual} - IO vencimiento ({fecha_vencimiento}) - extraccion ({fecha_extraccion}).png"
                st.download_button(
                    "📥 Descargar imagen", 
                    data=img_data, 
                    file_name=file_name, 
                    mime="image/png"
                )
            except Exception as e:
                st.error(f"Error al mostrar la imagen: {str(e)}")
            
            if st.checkbox("Mostrar tabla de datos completos", value=False):
                df_tabla = pd.DataFrame(resultado["datos_tabla"])
                st.dataframe(df_tabla)
        else:
            st.warning("No se pudo generar el gráfico. Intente con otros filtros.")
    
    # ==== ESTADÍSTICAS ====
    elif selected == "Estadísticas":
        st.markdown("<h2 class='fade-in'>Estadísticas</h2>", unsafe_allow_html=True)
        
        if not st.session_state.get("hojas_disponibles"):
            st.info("No se ha cargado ninguna base de datos. Por favor, use la opción 'Cargar Datos'.")
            st.stop()
        
        hoja_actual = st.session_state["nombre_hoja_excel"]
        
        col_controles, col_grafico = st.columns([1, 2])
        
        with col_controles:
            st.markdown("### Selección de parámetros")
            
            with st.spinner("Cargando fechas de vencimiento..."):
                fechas_vencimiento = get_fechas_vencimiento(hoja_actual, None)
            
            if not fechas_vencimiento:
                st.warning("No hay datos disponibles.")
                st.stop()
            
            fecha_vencimiento_stats = st.selectbox(
                "Seleccione fecha de vencimiento:",
                fechas_vencimiento,
                key="fecha_vencimiento_stats"
            )
            
            with st.spinner("Cargando strikes..."):
                strikes_disponibles = get_strikes(hoja_actual, fecha_vencimiento_stats)
            
            if not strikes_disponibles:
                st.warning("No hay strikes disponibles.")
                st.stop()
            
            strike_seleccionado = st.selectbox(
                "Seleccione strike:",
                strikes_disponibles,
                key="strike_seleccionado_stats"
            )
        
        with col_grafico:
            st.markdown("### Evolución del Open Interest")
            
            filtros_actuales = (hoja_actual, fecha_vencimiento_stats, strike_seleccionado)
            
            if (st.session_state.get("filtros_estadisticas") != filtros_actuales or 
                st.session_state.get("resultado_estadisticas") is None):
                
                with st.spinner("Generando gráfico de evolución..."):
                    img_base64, historical_data = generate_evolution_chart(hoja_actual, fecha_vencimiento_stats, strike_seleccionado)
                    
                    if img_base64:
                        st.session_state["resultado_estadisticas"] = {
                            "imagen_base64": img_base64,
                            "datos_historicos": historical_data
                        }
                        st.session_state["filtros_estadisticas"] = filtros_actuales
                    else:
                        st.session_state["resultado_estadisticas"] = None
            
            resultado = st.session_state.get("resultado_estadisticas")
            if resultado:
                try:
                    img_data = base64.b64decode(resultado["imagen_base64"])
                    st.image(img_data, width="stretch")
                    
                    file_name = f"{hoja_actual} - Evolucion Strike {strike_seleccionado} - Vencimiento ({fecha_vencimiento_stats}).png"
                    st.download_button(
                        "📥 Descargar gráfico de evolución", 
                        data=img_data, 
                        file_name=file_name, 
                        mime="image/png"
                    )
                except Exception as e:
                    st.error(f"Error al mostrar la imagen: {str(e)}")
                
                if st.checkbox("Mostrar datos históricos", value=False, key="mostrar_datos_stats"):
                    df_historico = pd.DataFrame(resultado["datos_historicos"])
                    st.dataframe(df_historico, width="stretch")
            else:
                st.warning("No se pudo generar el gráfico. Intente con otros parámetros.")
    
    # ==== VENCIMIENTOS ====

    elif selected == "Vencimientos":
        st.markdown("<h2 class='fade-in'>📅 Vencimientos - EUROSTOXX</h2>", unsafe_allow_html=True)
        
        hoja_actual = st.session_state.get("nombre_hoja_excel", "")
        
        if "eurostoxx" not in hoja_actual.lower():
            st.error("""
            ⚠️ **Selecciona EUROSTOXX en el menú lateral**
            
            Para usar la sección de Vencimientos, debes tener seleccionada una hoja de datos EUROSTOXX 
            en el selector del menú lateral.
            """)
            st.stop()
        
        if not st.session_state.get("hojas_disponibles"):
            st.info("No se ha cargado ninguna base de datos. Por favor, use la opción 'Cargar Datos'.")
            st.stop()
        
        with st.spinner("Cargando fechas de vencimiento desde OEX_cierre_semanal.xlsx..."):
            fechas_vencimiento = get_fechas_vencimiento_oex(hoja_actual)
            
            if not fechas_vencimiento:
                st.error("""
                ❌ **No se encontraron vencimientos compatibles**
                
                **Posibles causas:**
                1. El archivo `OEX_cierre_semanal.xlsx` no existe
                2. No hay fechas comunes entre OEX y la base de datos
                """)
                st.stop()
        
        st.success(f"✅ **Vencimientos cargados desde OEX_cierre_semanal.xlsx**")
        st.info(f"**📋 Filtro aplicado:** {len(fechas_vencimiento)} vencimientos disponibles")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.markdown("### Seleccionar Vencimiento")
            fecha_vencimiento_seleccionada = st.selectbox(
                "Fecha de vencimiento:",
                fechas_vencimiento,
                key="selector_vencimiento"
            )
            
            if st.button("🔍 Cargar Datos del Vencimiento", type="primary", width="stretch", key="btn_cargar_vencimiento"):
                st.session_state["vencimiento_seleccionado"] = fecha_vencimiento_seleccionada
                st.rerun()
        
        if st.session_state.get("vencimiento_seleccionado"):
            fecha_vencimiento = st.session_state["vencimiento_seleccionado"]
            
            with st.spinner(f"Cargando datos EUROSTOXX para vencimiento {fecha_vencimiento}..."):
                resultado = get_detalle_vencimiento(hoja_actual, fecha_vencimiento)
            
            if resultado:
                estadisticas = resultado.get("estadisticas", {})
                
                st.success(f"✅ **Base de datos EUROSTOXX activa:** {hoja_actual}")
                st.info(f"""
                **🔍 Filtro EUROSTOXX aplicado:** 
                - Strikes en rango: {estadisticas.get('rango_strikes', 'N/A')}
                - Registros mostrados: {estadisticas.get('total_registros_filtrados', 0)} de {estadisticas.get('total_registros_original', 0)} totales
                """)
                
                st.markdown("---")
                st.markdown("### 📋 Información del Vencimiento")
                
                info_oex = resultado.get("informacion_oex", {})
                
                if info_oex:
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        dia_semana = info_oex.get('Día de la semana', 'N/A')
                        st.metric("📅 Día de la Semana", dia_semana)
                    
                    with col2:
                        tipo_vencimiento = info_oex.get('Tipo de vencimiento', 'N/A')
                        st.metric("🎯 Tipo de Vencimiento", tipo_vencimiento)
                    
                    with col3:
                        precio_cierre = info_oex.get('Precio Cierre', 'N/A')
                        if isinstance(precio_cierre, (int, float)):
                            st.metric("💰 Precio Cierre", f"€{precio_cierre:,.2f}")
                        else:
                            st.metric("💰 Precio Cierre", str(precio_cierre))
                    
                    with col4:
                        hora_cierre = info_oex.get('Hora_cierre', 'N/A')
                        if isinstance(hora_cierre, (datetime, pd.Timestamp)):
                            st.metric("🕒 Hora Cierre", hora_cierre.strftime('%H:%M:%S'))
                        else:
                            st.metric("🕒 Hora Cierre", str(hora_cierre))
                    
                    comentario = info_oex.get('Comentario', '')
                    if comentario and comentario != 'N/A':
                        st.success(f"**💬 Comentario:** {comentario}")
                else:
                    st.warning(f"⚠️ **No se encontró información adicional en OEX_cierre_semanal.xlsx**")
                
                st.markdown("---")
                st.markdown("### 📊 Estadísticas EUROSTOXX - Open Interest")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_call = estadisticas.get('total_call_oi', 0)
                    st.metric("📞 CALL OI Total", f"{total_call:,}")
                
                with col2:
                    total_put = estadisticas.get('total_put_oi', 0)
                    st.metric("📟 PUT OI Total", f"{total_put:,}")
                
                with col3:
                    total_oi = estadisticas.get('total_oi', 0)
                    st.metric("📊 OI Total", f"{total_oi:,}")
                
                with col4:
                    ratio_pc = estadisticas.get('ratio_put_call', 0)
                    st.metric("⚖️ Ratio Put/Call", f"{ratio_pc:.2f}")
                
                st.markdown("---")
                st.markdown(f"### 📋 Datos Detallados EUROSTOXX - {fecha_vencimiento}")
                
                datos_eurostoxx = resultado.get("datos_eurostoxx", [])
                if datos_eurostoxx:
                    df_detalle = pd.DataFrame(datos_eurostoxx)
                    
                    tab1, tab2 = st.tabs(["📊 Vista Tabla", "📈 Vista Gráfica"])
                    
                    with tab1:
                        columnas_disponibles = ['Fecha de Extracción', 'Strike', 'Call Open Interest', 'Put Open Interest']
                        columnas_seleccionadas = st.multiselect(
                            "Seleccionar columnas:",
                            columnas_disponibles,
                            default=columnas_disponibles
                        )
                        
                        if columnas_seleccionadas:
                            df_filtrado = df_detalle[columnas_seleccionadas]
                            st.dataframe(df_filtrado, width="stretch", height=400)
                            
                            csv = df_filtrado.to_csv(index=False)
                            st.download_button(
                                "📥 Descargar CSV EUROSTOXX",
                                data=csv,
                                file_name=f"eurostoxx_vencimiento_{fecha_vencimiento}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                    
                    with tab2:
                        if 'Fecha de Extracción' in df_detalle.columns:
                            fechas_extraccion = df_detalle['Fecha de Extracción'].unique()
                            if len(fechas_extraccion) > 0:
                                ultima_fecha = sorted(fechas_extraccion, reverse=True)[0]
                                df_ultima = df_detalle[df_detalle['Fecha de Extracción'] == ultima_fecha]
                                
                                if not df_ultima.empty:
                                    precio_cierre_oex = None
                                    if resultado and resultado.get("informacion_oex"):
                                        precio_cierre_oex = resultado["informacion_oex"].get('Precio Cierre')
                                        if precio_cierre_oex is not None and isinstance(precio_cierre_oex, (int, float)) and precio_cierre_oex > 0:
                                            st.success(f"💰 **Precio de Cierre OEX:** {precio_cierre_oex:,.2f}")
                                        else:
                                            precio_cierre_oex = None
                                            st.warning("ℹ️ No se encontró precio de cierre válido en OEX")
                                    
                                    if precio_cierre_oex is not None:
                                        strikes_ordenados = df_ultima.sort_values('Strike')
                                        total_strikes = len(strikes_ordenados)
                                        
                                        idx_cierre = (strikes_ordenados['Strike'] - precio_cierre_oex).abs().idxmin()
                                        strike_cierre_idx = strikes_ordenados.index.get_loc(idx_cierre)
                                        
                                        strikes_a_cada_lado = max(1, total_strikes // 4)
                                        
                                        inicio = max(0, strike_cierre_idx - strikes_a_cada_lado)
                                        fin = min(total_strikes, strike_cierre_idx + strikes_a_cada_lado + 1)
                                        
                                        df_grafico = strikes_ordenados.iloc[inicio:fin].copy()
                                        
                                        st.info(f"**🎯 Rango mostrado:** {df_grafico['Strike'].min():.0f} - {df_grafico['Strike'].max():.0f} (OEX: {precio_cierre_oex:,.2f})")
                                    else:
                                        strikes_total = len(df_ultima)
                                        mostrar_cada = max(1, strikes_total // 2)
                                        df_grafico = df_ultima.iloc[::mostrar_cada].copy()
                                    
                                    df_grafico = df_grafico.sort_values('Strike')
                                    
                                    st.markdown("#### 🎛️ Configuración del Gráfico")
                                    
                                    col_config1, col_config2 = st.columns(2)
                                    
                                    with col_config1:
                                        if precio_cierre_oex is not None:
                                            strikes_total = len(df_ultima)
                                            porcentaje_mostrar = st.slider(
                                                "**Porcentaje de strikes a mostrar:**",
                                                min_value=10,
                                                max_value=100,
                                                value=50,
                                                step=10
                                            )
                                            
                                            strikes_a_cada_lado = max(1, int((strikes_total * porcentaje_mostrar / 100) // 2))
                                            strikes_ordenados = df_ultima.sort_values('Strike')
                                            idx_cierre = (strikes_ordenados['Strike'] - precio_cierre_oex).abs().idxmin()
                                            strike_cierre_idx = strikes_ordenados.index.get_loc(idx_cierre)
                                            
                                            inicio = max(0, strike_cierre_idx - strikes_a_cada_lado)
                                            fin = min(strikes_total, strike_cierre_idx + strikes_a_cada_lado + 1)
                                            
                                            df_grafico = strikes_ordenados.iloc[inicio:fin].copy()
                                    
                                    with col_config2:
                                        ordenar_por_oi = st.checkbox("Ordenar por Open Interest (solo visual)", value=False)
                                    
                                    strikes_labels = df_grafico['Strike'].astype(str).tolist()
                                    strikes_values = df_grafico['Strike'].tolist()
                                    call_oi = df_grafico['Call Open Interest'].tolist()
                                    put_oi = df_grafico['Put Open Interest'].tolist()
                                    
                                    base_oi = []
                                    overlay_oi = []
                                    base_colors = []
                                    overlay_colors = []
                                    
                                    for i, (call, put) in enumerate(zip(call_oi, put_oi)):
                                        if call >= put:
                                            base_oi.append(call)
                                            overlay_oi.append(put)
                                            base_colors.append('green')
                                            overlay_colors.append('red')
                                        else:
                                            base_oi.append(put)
                                            overlay_oi.append(call)
                                            base_colors.append('red')
                                            overlay_colors.append('green')
                                    
                                    if ordenar_por_oi:
                                        oi_total = [call + put for call, put in zip(call_oi, put_oi)]
                                        indices_ordenados = sorted(range(len(oi_total)), key=lambda i: oi_total[i], reverse=True)
                                        
                                        strikes_labels = [strikes_labels[i] for i in indices_ordenados]
                                        strikes_values = [strikes_values[i] for i in indices_ordenados]
                                        base_oi = [base_oi[i] for i in indices_ordenados]
                                        overlay_oi = [overlay_oi[i] for i in indices_ordenados]
                                        base_colors = [base_colors[i] for i in indices_ordenados]
                                        overlay_colors = [overlay_colors[i] for i in indices_ordenados]
                                    
                                    fig, ax = plt.subplots(figsize=(16, 9))
                                    
                                    x_pos = range(len(strikes_labels))
                                    bar_width = 0.8
                                    
                                    bars_base = ax.bar(x_pos, base_oi, alpha=1.0, color=base_colors, 
                                                      width=bar_width, edgecolor='black', linewidth=0.5)
                                    
                                    bars_overlay = ax.bar(x_pos, overlay_oi, alpha=1.0, color=overlay_colors,
                                                         width=bar_width, edgecolor='black', linewidth=0.5)
                                    
                                    if precio_cierre_oex is not None:
                                        strike_differences = [abs(strike - precio_cierre_oex) for strike in strikes_values]
                                        if strike_differences:
                                            closest_idx = strike_differences.index(min(strike_differences))
                                            
                                            ax.axvline(x=closest_idx, color='red', linestyle='-', linewidth=3, alpha=0.8)
                                            
                                            y_max = max(base_oi) if base_oi else 0
                                            ax.annotate(f'OEX: {precio_cierre_oex:,.2f}', 
                                                       xy=(closest_idx, y_max * 0.95), 
                                                       xytext=(closest_idx, y_max * 0.98),
                                                       ha='center', va='bottom',
                                                       fontsize=11, fontweight='bold', color='red',
                                                       bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                                                       arrowprops=dict(arrowstyle='->', color='red', lw=1.5))
                                    
                                    ax.set_xlabel('Strike Price', fontsize=12, fontweight='bold')
                                    ax.set_ylabel('Open Interest', fontsize=12, fontweight='bold')
                                    
                                    titulo_orden = " (ordenados por OI)" if ordenar_por_oi else " (ordenados por strike)"
                                    titulo_principal = f'EUROSTOXX - Open Interest por Strike{titulo_orden}'
                                    if precio_cierre_oex is not None:
                                        titulo_principal += f' | Precio Cierre OEX: {precio_cierre_oex:,.2f}'
                                    
                                    ax.set_title(
                                        titulo_principal + f'\nVencimiento: {fecha_vencimiento} | Extracción: {ultima_fecha}',
                                        fontsize=14, 
                                        fontweight='bold', 
                                        pad=20
                                    )
                                    
                                    from matplotlib.patches import Patch
                                    legend_elements = [
                                        Patch(facecolor='green', label='CALL OI'),
                                        Patch(facecolor='red', label='PUT OI')
                                    ]
                                    
                                    ax.legend(handles=legend_elements, fontsize=11, loc='upper right')
                                    
                                    ax.set_xticks(x_pos)
                                    ax.set_xticklabels(strikes_labels, rotation=45, ha='right', fontsize=9)
                                    
                                    ax.grid(True, alpha=0.3, axis='y')
                                    
                                    plt.tight_layout()
                                    
                                    st.pyplot(fig)
                                    plt.close(fig)
                                    
                                    st.markdown("---")
                                    st.markdown("#### 📊 Análisis de Dominancia")
                                    
                                    calls_dominantes = base_colors.count('green')
                                    puts_dominantes = base_colors.count('red')
                                    total_strikes_mostrados = len(base_colors)
                                    
                                    col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)
                                    
                                    with col_stats1:
                                        st.metric("🎯 Strikes CALL Dominantes", f"{calls_dominantes}")
                                    
                                    with col_stats2:
                                        st.metric("🎯 Strikes PUT Dominantes", f"{puts_dominantes}")
                                    
                                    with col_stats3:
                                        if total_strikes_mostrados > 0:
                                            porc_calls = (calls_dominantes / total_strikes_mostrados) * 100
                                            st.metric("📈 % CALL Dominantes", f"{porc_calls:.1f}%")
                                    
                                    with col_stats4:
                                        if total_strikes_mostrados > 0:
                                            porc_puts = (puts_dominantes / total_strikes_mostrados) * 100
                                            st.metric("📈 % PUT Dominantes", f"{porc_puts:.1f}%")
                                    
                                    col_download1, col_download2, col_download3 = st.columns([1, 1, 1])
                                    
                                    with col_download2:
                                        buf = io.BytesIO()
                                        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
                                        buf.seek(0)
                                        
                                        st.download_button(
                                            "📥 Descargar Gráfico",
                                            data=buf.getvalue(),
                                            file_name=f"eurostoxx_vencimiento_{fecha_vencimiento}.png",
                                            mime="image/png",
                                            use_container_width=True
                                        )
            else:
                st.error("Error al cargar los datos del vencimiento seleccionado.")
                
    # ==== CARGAR DATOS ====
    elif selected == "Cargar Datos":
        if not st.session_state.get("acceso_cargar_datos", False):
            st.markdown("<h2 class='fade-in'>Cargar Datos</h2>", unsafe_allow_html=True)
            
            if st.session_state.get("bloqueo_temporal", False):
                st.error("🔒 **Acceso temporalmente bloqueado**")
                
                if st.button("🔄 Intentar nuevamente", type="primary", key="btn_reintentar_auth"):
                    st.session_state["bloqueo_temporal"] = False
                    st.session_state["intentos_password"] = 0
                    st.rerun()
                st.stop()
            
            st.warning("🔒 **Sección protegida**")
            st.info("Esta sección requiere autorización para modificar la base de datos.")
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                with st.form("password_form"):
                    st.markdown("### 🔑 Autenticación requerida")
                    password = st.text_input(
                        "Contraseña de acceso:",
                        type="password",
                        placeholder="Ingrese la contraseña..."
                    )
                    
                    submit = st.form_submit_button("🔓 Acceder a Cargar Datos", type="primary", width="stretch", key="btn_acceder_cargar")
                    
                    if submit:
                        if password:
                            PASSWORD_CORRECTA = "admin123"
                            
                            if password == PASSWORD_CORRECTA:
                                st.session_state["acceso_cargar_datos"] = True
                                st.session_state["intentos_password"] = 0
                                mostrar_mensaje("success", "Acceso concedido")
                                st.rerun()
                            else:
                                st.session_state["intentos_password"] = st.session_state.get("intentos_password", 0) + 1
                                intentos_restantes = 3 - st.session_state["intentos_password"]
                                
                                if intentos_restantes > 0:
                                    mostrar_mensaje("error", f"Contraseña incorrecta. {intentos_restantes} intentos restantes.")
                                else:
                                    mostrar_mensaje("error", "Demasiados intentos fallidos.")
                                    st.session_state["bloqueo_temporal"] = True
                                    st.rerun()
                        else:
                            mostrar_mensaje("warning", "Por favor ingrese una contraseña")
            
            with col2:
                st.markdown("### ℹ️ Información")
                st.markdown("""
                **Funciones protegidas:**
                - Cargar nuevos archivos Excel
                - Ampliar base de datos con CSV  
                - Convertir formatos de datos
                """)
            
            st.stop()
        
        # Contenido de Cargar Datos (autenticado)
        st.markdown("<h2 class='fade-in'>Cargar Datos</h2>", unsafe_allow_html=True)
        
        col_encabezado, col_cerrar = st.columns([3, 1])
        with col_encabezado:
            st.success("✅ **Acceso administrativo activo**")
        with col_cerrar:
            if st.button("🔒 Cerrar acceso", type="secondary", width="stretch", key="btn_cerrar_acceso"):
                st.session_state["acceso_cargar_datos"] = False
                mostrar_mensaje("info", "🔒 Acceso a Cargar Datos cerrado")
                st.rerun()
        
        st.markdown("---")
        
        with st.spinner("Verificando estado de la base de datos..."):
            estado_db = verificar_base_datos()
        
        st.markdown("### 📊 Estado de la Base de Datos")
        
        if estado_db:
            if estado_db.get("existe", False):
                if estado_db.get("esquema") == "unificado":
                    st.success("✅ **Base de Datos SQLite Activa (Esquema Unificado)**")
                else:
                    st.success("✅ **Base de Datos SQLite Activa**")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Archivo de BD", estado_db.get("base_datos_activa", "SQLite"))
                
                with col2:
                    st.metric("Assets/Tablas", estado_db.get("estadisticas", {}).get("total_tablas", 0))
                
                with col3:
                    st.metric("Total Registros", estado_db.get("estadisticas", {}).get("total_registros", 0))
                
                with st.expander("📋 Ver detalles"):
                    estadisticas = estado_db.get("estadisticas", {})
                    if estadisticas.get("registros_por_tabla"):
                        for tabla, registros in estadisticas["registros_por_tabla"].items():
                            st.write(f"- **{tabla}**: {registros} registros")
            
            elif estado_db.get("tipo") == "excel":
                st.warning("⚠️ **Archivo Excel Detectado (Sin Base de Datos)**")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Archivo Excel", estado_db.get("archivo_excel", "Excel"))
                
                with col2:
                    st.metric("Hojas Disponibles", len(estado_db.get("hojas_disponibles", [])))
                
                st.info("💡 **Recomendación**: Convierta el Excel a Base de Datos SQLite")
            
            else:
                st.error("❌ **No hay Base de Datos**")
                st.info("💡 **Instrucción**: Cargue un archivo Excel para crear la base de datos")
        
        st.markdown("---")
        
        opcion_menu = st.radio("**Seleccione una operación:**", 
                               ("Cargar nueva base de datos (Excel)", 
                                "Ampliar base de datos existente (CSV)",
                                "Convertir Excel a Base de Datos"))
        
        if opcion_menu == "Cargar nueva base de datos (Excel)":
            st.markdown("#### 📥 Cargar Nuevo Excel y Crear BD")
            
            uploaded_file = st.file_uploader("Seleccione archivo Excel (XLSX)", type=["xlsx"], key="excel_uploader")
            
            if uploaded_file:
                st.info(f"**Archivo seleccionado:** {uploaded_file.name} ({uploaded_file.size / 1024:.1f} KB)")
                
                if st.button("🚀 Cargar Excel y Crear Base de Datos", type="primary", width="stretch", key="btn_cargar_excel"):
                    with st.spinner("Cargando archivo y creando base de datos SQLite..."):
                        resultado = upload_excel_file(uploaded_file)
                        
                        if resultado:
                            st.session_state["hojas_disponibles"] = resultado["hojas_disponibles"]
                            st.session_state["nombre_hoja_excel"] = resultado["hoja_activa"]
                            st.session_state["nombre_base_activa"] = uploaded_file.name
                            
                            st.session_state["filtros_visualizacion"] = None
                            st.session_state["resultado_visualizacion"] = None
                            st.session_state["filtros_estadisticas"] = None
                            st.session_state["resultado_estadisticas"] = None
                            time.sleep(2)
                            mostrar_mensaje("success", resultado["mensaje"])
                            st.rerun()
        
        elif opcion_menu == "Ampliar base de datos existente (CSV)":
            st.markdown("#### 📈 Ampliar BD Existente con CSV")
            
            if not estado_db or not estado_db.get("existe", False):
                st.error("❌ **No hay base de datos activa**")
                st.info("Primero debe cargar una base de datos Excel.")
                st.stop()
            
            if "fecha_extraccion_csv" not in st.session_state:
                st.session_state["fecha_extraccion_csv"] = datetime.today().date()
            
            st.session_state["fecha_extraccion_csv"] = st.date_input(
                "**Fecha de extracción** para los nuevos datos:",
                value=st.session_state["fecha_extraccion_csv"]
            )
            
            uploaded_csv = st.file_uploader("Seleccione archivo CSV", type=["csv"], key="csv_uploader")
            
            if uploaded_csv is not None:
                st.info(f"**Archivo seleccionado:** {uploaded_csv.name} ({uploaded_csv.size / 1024:.1f} KB)")
                
                if estado_db.get("assets"):
                    opciones_destino = estado_db["assets"]
                elif estado_db.get("tablas"):
                    opciones_destino = estado_db["tablas"]
                else:
                    opciones_destino = st.session_state.get("hojas_disponibles", ["Datos"])
                
                hoja_csv = st.selectbox("**Asset/Hoja destino** para los datos:", opciones_destino)
                
                if st.button("📥 Cargar CSV en Base de Datos", type="primary", width="stretch", key="btn_cargar_csv_principal"):
                    with st.spinner("Procesando y cargando datos CSV..."):
                        fecha_str = st.session_state["fecha_extraccion_csv"].strftime('%Y-%m-%d')
                        resultado = upload_csv_file(uploaded_csv, hoja_csv, fecha_str)
                        
                        if st.button("📥 Cargar CSV en Base de Datos", type="primary", width="stretch"):
                            with st.spinner("Procesando y cargando datos CSV..."):
                                fecha_str = st.session_state["fecha_extraccion_csv"].strftime('%Y-%m-%d')
                                resultado = upload_csv_file(uploaded_csv, hoja_csv, fecha_str)
                                
                                if resultado:
                                    if resultado.get("error"):
                                        mostrar_mensaje("error", resultado["mensaje"])
                                        
                                        # Mostrar popup si viene en resultado
                                        if resultado.get("mostrar_popup"):
                                            mostrar_popup(
                                                resultado.get("popup_titulo", "⚠️ Advertencia"),
                                                resultado.get("popup_mensaje", ""),
                                                tipo="warning"
                                            )
                                        
                                        time.sleep(1)
                                    else:
                                        # Limpiar caché
                                        get_fechas_extraccion.clear()
                                        get_fechas_vencimiento.clear()
                                        get_strikes.clear()
                                        st.session_state["filtros_visualizacion"] = None
                                        st.session_state["resultado_visualizacion"] = None
                                        st.session_state["filtros_estadisticas"] = None
                                        st.session_state["resultado_estadisticas"] = None
                                        
                                        # Mostrar mensaje toast
                                        mostrar_mensaje("success", resultado["mensaje"])
                                        
                                        # Mostrar popup si viene en resultado
                                        if resultado.get("mostrar_popup"):
                                            mostrar_popup(
                                                resultado.get("popup_titulo", "✅ Éxito"),
                                                resultado.get("popup_mensaje", ""),
                                                tipo="success"
                                            )
                                        
                                        time.sleep(1)
                                        st.rerun()
                                else:
                                    mostrar_mensaje("error", "Error al cargar el archivo CSV. Verifique el formato.")
                                    time.sleep(1)



        
        else:  # Convertir Excel a Base de Datos
            st.markdown("#### 🔄 Convertir Excel a BD SQLite")
            
            if estado_db and estado_db.get("tipo") == "excel":
                st.success("✅ **Excel detectado - Listo para conversión**")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Archivo Excel", estado_db.get("archivo_excel", "Excel"))
                with col2:
                    st.metric("Hojas", len(estado_db.get("hojas_disponibles", [])))
                
                st.info("""
                **Beneficios de usar SQLite:**
                - ✅ Mayor velocidad de consultas
                - ✅ Menor uso de memoria
                - ✅ Operaciones más eficientes
                """)
                
                if st.button("🔄 Convertir Excel a Base de Datos SQLite", type="primary", width="stretch", key="btn_convertir_excel"):
                    with st.spinner("Convirtiendo Excel a base de datos SQLite..."):
                        # Backup de BD existente si existe
                        if os.path.exists(DB_PATH):
                            backup_dir = "backups"
                            os.makedirs(backup_dir, exist_ok=True)
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            backup_filename = f"base_datos_backup_{timestamp}.db"
                            backup_path = os.path.join(backup_dir, backup_filename)
                            shutil.copyfile(DB_PATH, backup_path)
                            time.sleep(2)
                            mostrar_mensaje("success", f"Backup creado: {backup_filename}")
                            # Eliminar BD antigua
                            os.remove(DB_PATH)
                        
                        success = create_sqlite_from_excel()
                        if success:
                            mostrar_mensaje("success", "Base de datos SQLite creada exitosamente")
                            get_estado.clear()
                            get_fechas_extraccion.clear()
                            get_fechas_vencimiento.clear()
                            get_strikes.clear()
                            st.rerun()
                        else:
                            mostrar_mensaje("error", "Error al crear la base de datos")

            else:
                st.info("""
                **Esta opción convierte un archivo Excel existente a base de datos SQLite.**
                
                Actualmente no hay archivos Excel para convertir.
                """)
    

    # ==== CONFIGURACIÓN ====
    elif selected == "Configuración":
        st.markdown("<h2 class='fade-in'>Configuración</h2>", unsafe_allow_html=True)
        
        st.markdown("### Información del Sistema")
        
        estado_info = get_estado()
        if estado_info:
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Estado", "🟢 Conectada" if estado_info["existe_base_datos"] else "🔴 Sin conexión")
                st.metric("Fuente de datos", estado_info.get("data_source", "Desconocida"))
            
            with col2:
                st.metric("Assets disponibles", len(estado_info.get("hojas_disponibles", [])))
                st.metric("Esquema", estado_info.get("esquema", "Desconocido"))
        
        st.markdown("---")
        st.markdown("### Migración de Esquema")
        
        st.info("""
        **Esquema Unificado de Base de Datos**
        
        El nuevo esquema unificado almacena todos los datos en una sola tabla optimizada.
        
        - 🚀 Mayor velocidad en consultas
        - 💾 Menor uso de memoria
        - 🔧 Mantenimiento simplificado
        """)
        
        estado_db = verificar_base_datos()
        if estado_db and estado_db.get("esquema") == "unificado":
            st.success("✅ **Esquema Unificado Activo**")
        
        st.markdown("---")
        st.markdown("### Limpiar Caché")
        
        if st.button("🧹 Limpiar Caché de la Aplicación", type="secondary", key="btn_limpiar_cache"):
            get_estado.clear()
            get_fechas_extraccion.clear()
            get_fechas_vencimiento.clear()
            get_strikes.clear()
            st.session_state["filtros_visualizacion"] = None
            st.session_state["resultado_visualizacion"] = None
            st.session_state["filtros_estadisticas"] = None
            st.session_state["resultado_estadisticas"] = None
            
            mostrar_mensaje("success", "Caché limpiado correctamente")
            st.rerun()
