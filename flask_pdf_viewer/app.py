import os

from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo

import math

import pandas as pd

import pyodbc  # puedes dejarlo; no estorba

from pandas.errors import DatabaseError as PDDatabaseError

from flask import Flask, abort, redirect, render_template, request, Response, url_for

from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user

from fpdf import FPDF



# --- NUEVO: logs limpios + SQLAlchemy ---

import logging

import warnings

import urllib.parse

from sqlalchemy import create_engine, text

# ----------------------------------------





# ------------------------------------------------------------------------------

# Config

# ------------------------------------------------------------------------------

FONT = "Helvetica"  # evita warnings por Arial en FPDF 2.7+

app = Flask(__name__)

try:
    _LIMA_TZ = ZoneInfo("America/Lima")
except Exception:
    # Lima no usa DST actualmente; fallback fijo UTC-5.
    _LIMA_TZ = timezone(timedelta(hours=-5))


def _now_lima() -> datetime:
    return datetime.now(_LIMA_TZ)


def _now_lima_str() -> str:
    return _now_lima().strftime("%Y-%m-%d %H:%M")

@app.context_processor
def inject_globals():
    # Útil para templates (login/footer)
    return {"current_year": _now_lima().year}


app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

DB_CONN_STRING = os.environ.get("DB_CONN_STRING")

if not DB_CONN_STRING:

    raise SystemExit("Falta DB_CONN_STRING")



# --- Consola más limpia ---

logging.getLogger("werkzeug").setLevel(logging.ERROR)

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

warnings.filterwarnings(

    "ignore",

    message="pandas only supports SQLAlchemy connectable",

    category=UserWarning,

)



# --- Engine SQLAlchemy con pool (usa tu ODBC string) ---

_odbc = urllib.parse.quote_plus(DB_CONN_STRING)

ENGINE = create_engine(

    f"mssql+pyodbc:///?odbc_connect={_odbc}",

    pool_pre_ping=True,

    pool_size=5,

    max_overflow=5,

    pool_recycle=1800,

    future=True,

)



# --------------------------------------------------------------------------

# Helper de medición simple (perf)

# --------------------------------------------------------------------------

def _tick(label: str):

    import time

    t0 = time.perf_counter()

    def _tock():

        dt = (time.perf_counter() - t0) * 1000

        app.logger.info(f"[PERF] {label}: {dt:.1f} ms")

    return _tock





# ------------------------------------------------------------------------------

# Helpers de título/fecha

# ------------------------------------------------------------------------------

TITLE_BASE = "RESUMEN - INFORME DE PRODUCCIÓN DEL DÍA"



def _fmt_ddmmaaaa(fecha_str: str) -> str:

    """Convierte fechas comunes a dd/mm/aaaa."""

    if not fecha_str:

        return ""

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):

        try:

            d = datetime.strptime(fecha_str, fmt)

            return d.strftime("%d/%m/%Y")

        except ValueError:

            continue

    return fecha_str  # fallback sin romper



def title_with_date(meta: dict) -> str:

    fecha_raw = meta.get("fecha", "")

    fecha_formateada = _fmt_ddmmaaaa(fecha_raw)



    fecha_dt = None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):

        try:

            fecha_dt = datetime.strptime(fecha_raw, fmt)

            break

        except ValueError:

            pass



    if fecha_dt is None:

        return f"{TITLE_BASE} - {fecha_formateada}"



    dias_semana = [

        "LUNES", "MARTES", "MIÉRCOLES",

        "JUEVES", "VIERNES", "SÁBADO", "DOMINGO"

    ]

    nombre_dia = dias_semana[fecha_dt.weekday()]

    return f"{TITLE_BASE} {nombre_dia} - {fecha_formateada}"





# ------------------------------------------------------------------------------

# FORMATO NUMÉRICO (RECUPERADO + MILES)

# ------------------------------------------------------------------------------

def _float_or_0(v) -> float:

    try:

        if v is None:

            return 0.0

        if hasattr(pd, "isna") and pd.isna(v):

            return 0.0

        # si viene con coma (1,234) lo limpiamos

        return float(str(v).replace(",", ""))

    except Exception:

        return 0.0



def _n(v, decimals: int = 0) -> str:

    """

    NUMÉRICO con:

    - redondeo (por defecto a 0 decimales)

    - separador de miles con coma

    - vacío si NaN/None

    """

    try:

        if v is None or (hasattr(pd, "isna") and pd.isna(v)):

            return ""

        x = _float_or_0(v)

        if decimals == 0:

            return f"{int(round(x)):,}"

        return f"{round(x, decimals):,.{decimals}f}"

    except Exception:

        return ""



def _t(v) -> str:

    """Texto seguro."""

    try:

        if v is None or (hasattr(pd, "isna") and pd.isna(v)):

            return ""

        return str(v)

    except Exception:

        return ""



# Alias de compatibilidad (si te quedó algún _fmt suelto)

def _fmt(v):

    return _n(v, 0)


def _canal_detail_label(canal_value) -> str:
    canal_lbl = (str(canal_value) or "").strip()
    canal_lbl = canal_lbl.upper() if canal_lbl else "SIN CANAL"
    return {
        "ZN": "LOCAL",
        "ZE": "EXPORTACIÓN",
    }.get(canal_lbl, canal_lbl)



# ----------------------------------------------------------------------
# NormalizaciÃ³n de columnas y jerarquÃ­a (evita columnas vacÃ­as por espacios)
# ----------------------------------------------------------------------
def _norm_key(s: str) -> str:
    s = str(s).replace("\u00a0", " ")
    s = " ".join(s.split()).upper()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for c in df.columns:
        s = str(c).replace("\u00a0", " ")
        s2 = " ".join(s.split())
        if s2 != s:
            renamed[c] = s2
    if renamed:
        df = df.rename(columns=renamed)
    return df


def _ensure_jer_col(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    if col_name not in df.columns:
        upmap = {_norm_key(c): c for c in df.columns}
        key = _norm_key(col_name)
        if key in upmap:
            df[col_name] = df[upmap[key]]
        else:
            df[col_name] = df.get("NOMBRE", "")

    ser = df[col_name]
    empty_mask = ser.isna() | (ser.astype(str).str.strip() == "")

    def _fill_from(src_col: str):
        nonlocal empty_mask
        if src_col in df.columns:
            src = df[src_col]
            src_ok = ~(src.isna() | (src.astype(str).str.strip() == ""))
            use = empty_mask & src_ok
            if use.any():
                df.loc[use, col_name] = src.loc[use]
                empty_mask = empty_mask & ~use

    _fill_from("NOMBRE")

    for c in df.columns:
        if c == col_name:
            continue
        if _norm_key(c).startswith("JERARQUIA2"):
            _fill_from(c)
    return df


def _ensure_col(df: pd.DataFrame, target: str, candidates, default="") -> pd.DataFrame:
    if target not in df.columns:
        for c in candidates:
            if c in df.columns:
                df[target] = df[c]
                break
        else:
            df[target] = default
    return df





# ------------------------------------------------------------------------------

# Auth (demo)

# ------------------------------------------------------------------------------

login_manager = LoginManager(app)

login_manager.login_view = "login"



class DemoUser(UserMixin):

    id = "admin"

    name = "admin"



@login_manager.user_loader

def load_user(uid):

    return DemoUser() if uid == "admin" else None





# ------------------------------------------------------------------------------

# Data + mantenimiento

# ------------------------------------------------------------------------------


class MaintenanceError(RuntimeError):

    """Bloqueo/timeout del SP o indisponibilidad temporal."""

    pass


class DatabaseConfigError(RuntimeError):

    """Credenciales o cadena de conexion invalidas para SQL Server."""

    pass


def _pdf_bytes(pdf: FPDF) -> bytes:
    """Devuelve bytes del PDF (compatible con fpdf2)."""
    out = pdf.output()
    if isinstance(out, (bytes, bytearray)):
        return bytes(out)
    # fpdf2 suele retornar str en latin-1
    return str(out).encode("latin-1", errors="replace")


def build_maintenance_pdf(msg: str) -> bytes:

    """PDF visible en el iframe cuando el SP está bloqueado/indisponible."""

    pdf = FPDF(orientation="P", unit="mm", format="A4")

    pdf.set_margins(20, 20, 20)

    pdf.add_page()

    pdf.set_font(FONT, "B", 16)

    pdf.cell(0, 10, "Servicio en mantenimiento", new_x="LMARGIN", new_y="NEXT", align="C")

    pdf.ln(4)

    pdf.set_font(FONT, "", 12)

    pdf.multi_cell(0, 7, msg)

    pdf.ln(6)

    pdf.set_font(FONT, "", 10)

    pdf.cell(0, 6, f"Fecha y hora: {_now_lima_str()}", new_x="LMARGIN", new_y="NEXT", align="R")

    return _pdf_bytes(pdf)

def call_sp(sociedad: str, fecha: str) -> pd.DataFrame:
    """
    Lee data desde L3_DEVELOPMENTSTAGE_MTH.excel.L3_STOCK_PRD_VENTAS_TB
    filtrando por AUDIT_DATE (por día).
    """
    try:
        sql = text("""
            SET LOCK_TIMEOUT 3000;

            DECLARE @d date = CONVERT(date, :fec);

            SELECT *
            FROM L3_DEVELOPMENTSTAGE_MTH.excel.L3_STOCK_PRD_VENTAS_TB
            WHERE [AUDIT_DATE] >= @d
              AND [AUDIT_DATE] <  DATEADD(day, 1, @d);
        """)

        with ENGINE.connect() as conn:
            df = pd.read_sql_query(sql, conn, params={"fec": fecha})

        return _normalize_columns(df)

    except Exception as e:

        msg = str(e).lower()

        if any(t in msg for t in ("1222", "timeout", "deadlock", "bloqueo", "lock timeout")):

            raise MaintenanceError("SP bloqueado o timeout") from e

        if any(t in msg for t in ("18456", "28000", "login failed", "invalid connection string attribute", "odbc driver 18 for sql server")):

            raise DatabaseConfigError("Error de autenticacion o configuracion de base de datos") from e

        raise



def filter_sector(df: pd.DataFrame, sector: str) -> pd.DataFrame:

    if "SECTOR" not in df.columns:

        return df.copy()

    if sector and sector.upper() != "TODOS":

        return df[df["SECTOR"].astype(str).str.upper() == sector.upper()].copy()

    return df





# ------------------------------------------------------------------------------

# Utilidad: detectar columnas de orden (robusto)

# ------------------------------------------------------------------------------

def _pick_order_col(df: pd.DataFrame, candidates) -> str | None:

    for c in candidates:

        if c in df.columns:

            return c

    # fallback por upper-case match (por si vienen con espacios raros)

    upmap = {_norm_key(col): col for col in df.columns}

    for c in candidates:

        cu = _norm_key(c)

        if cu in upmap:

            return upmap[cu]

    return None





# ------------------------------------------------------------------------------

# Grupo 1

# ------------------------------------------------------------------------------

def build_pdf_group1(meta, df: pd.DataFrame) -> bytes:

    def ensure(col, default=0):

        if col not in df.columns:

            df[col] = default



    # --- USAR SIEMPRE STOCK TOTAL ---

    if "Stock Total" not in df.columns:

        df["Stock Total"] = df.get("Stock", 0).fillna(0)



    num_cols = ["Stock Total","PRD_Dia","PRD_Mes","PRD_Año",

                "NAC_Dia","NAC_Mes","NAC_Año",

                "EXP_Dia","EXP_Mes","EXP_Año"]

    for c in num_cols:

        ensure(c, 0)



    ensure("PPTO_MES", 0)

    ensure("PPTO", 0)



    if "UND" not in df.columns:

        df["UND"] = df.get("UNIDAD DE MEDIDA BASE", "")

    _ensure_jer_col(df, "JERARQUIA2 G1")

    if "SECTOR" not in df.columns:

        df["SECTOR"] = "SIN SECTOR"



    sums_cols = num_cols + ["PPTO_MES", "PPTO"]



    agg = (

        df.groupby(["SECTOR","JERARQUIA2 G1","UND"], dropna=False)[sums_cols]

          .sum(numeric_only=True).reset_index()

          .sort_values(["SECTOR","JERARQUIA2 G1","UND"])

    )



    pdf = FPDF(orientation="P", unit="mm", format="A4")

    pdf.set_auto_page_break(auto=True, margin=10)

    pdf.add_page()



    usable_w = pdf.w - pdf.l_margin - pdf.r_margin

    pdf.set_x(pdf.l_margin); pdf.set_font(FONT, "B", 12)

    pdf.cell(usable_w, 7, title_with_date(meta), new_y="NEXT", align="C")

    pdf.set_x(pdf.l_margin); pdf.set_font(FONT, size=9)

    pdf.cell(usable_w, 5,

        f"Sociedad: {meta['sociedad']}     Sector: {meta['sector']}   Grupo: {meta.get('grupo','1')}",

        new_y="NEXT", align="L"

    )

    pdf.set_x(pdf.l_margin)

    pdf.cell(usable_w, 5, f"Generado: {_now_lima_str()}",

             new_y="NEXT", align="L")

    pdf.ln(1)



    # ---------------------------

    #  ENCABEZADOS Y TABLA

    # ---------------------------

    w_sector, w_name, w_und, w_stock, w_pptomes, w_sub = 12, 58, 9, 16, 11.5, 12

    total_w = w_sector + w_name + w_und + w_stock + w_pptomes + (3+2+2+1+3)*w_sub

    scale = usable_w / total_w

    w_sector, w_name, w_und, w_stock, w_pptomes, w_sub = [

        round(x*scale,2) for x in (w_sector, w_name, w_und, w_stock, w_pptomes, w_sub)

    ]



    H, H1, H2 = 5.0, 5.8, 4.8

    HDR_MAIN, HDR_SUB = 6.6, 6.2

    BODY, BODY_B = 5.8, 6.2



    def header_table():

        x0 = pdf.l_margin; y0 = pdf.get_y()



        def box(x, y, w, h, txt="", bold=False, align="C", is_sub=False):

            pdf.rect(x, y, w, h)

            if txt:

                pdf.set_xy(x, y + 0.45)

                pdf.set_font(FONT, "B" if bold else "",

                             HDR_SUB if is_sub else HDR_MAIN)

                pdf.cell(w, h, txt, border=0, align=align)



        def box_ppto_mes(x, y, w, h):

            pdf.rect(x, y, w, h)

            pdf.set_font(FONT, "B", HDR_MAIN)

            pdf.set_xy(x, y + 0.35)

            pdf.cell(w, H1-0.5, "PPTO", border=0, align="C")

            pdf.set_font(FONT, "B", HDR_SUB)

            pdf.set_xy(x, y + H1 - 0.05)

            pdf.cell(w, H2, "MES", border=0, align="C")



        x = x0

        box(x, y0, w_sector, H1+H2, "Sector", True, "L"); x += w_sector

        box(x, y0, w_name,   H1+H2, "JERARQUIA2 G1", True, "L"); x += w_name

        box(x, y0, w_und,    H1+H2, "UND", True); x += w_und

        box(x, y0, w_stock,  H1+H2, "Stock", True); x += w_stock

        box_ppto_mes(x, y0, w_pptomes, H1+H2); x += w_pptomes



        def group(title, labels):

            nonlocal x

            width = len(labels)*w_sub

            box(x, y0, width, H1, title, True)

            xx = x

            for lab in labels:

                box(xx, y0+H1, w_sub, H2, lab, is_sub=True)

                xx += w_sub

            x += width



        group("PRD", ("Día","Mes","Año"))

        group("NACIONALES", ("Día","Mes"))

        group("EXPORTACIÓN", ("Día","Mes"))

        group("PPTO", ("Mes",))

        group("TOTALES", ("Día","Mes","Año"))

        pdf.set_y(y0 + H1 + H2)



    header_table()



    def print_row(values, bold=False, fill=False, skip_sector_border=False):

        widths = [w_sector, w_name, w_und, w_stock, w_pptomes] + [w_sub]*(3+2+2+1+3)

        pdf.set_fill_color(230,230,230) if fill else pdf.set_fill_color(255,255,255)

        pdf.set_font(FONT, "B" if bold else "", BODY_B if bold else BODY)

        for idx, (w, v) in enumerate(zip(widths, values)):

            align = "L" if idx in (0,1) else "C"

            if idx == 0 and skip_sector_border:

                pdf.cell(w, H, v, border=0, align=align, fill=fill)

            else:

                pdf.cell(w, H, v, border=1, align=align, fill=fill)

        pdf.ln()



    bottom = pdf.h - pdf.b_margin

    for sector, sub in agg.groupby("SECTOR"):



        block_rows = len(sub) + 1

        block_height = block_rows * H



        if pdf.get_y() + block_height > bottom:

            pdf.add_page()

            header_table()



        x_left = pdf.l_margin; y_top = pdf.get_y()

        pdf.rect(x_left, y_top, w_sector, block_height)



        label = str(sector or "").upper()

        pdf.set_font(FONT, "B", 6.0)

        per_line = max(2.6, min(3.8, (block_height - 2) / max(1, len(label))))

        y_cursor = y_top + max(0, (block_height - per_line*len(label)) / 2)



        for ch in label:

            pdf.set_xy(x_left, y_cursor)

            pdf.cell(w_sector, per_line, ch, border=0, align="C")

            y_cursor += per_line



        pdf.set_xy(pdf.l_margin, y_top)



        for _, r in sub.iterrows():

            tot_dia = _float_or_0(r["NAC_Dia"]) + _float_or_0(r["EXP_Dia"])

            tot_mes = _float_or_0(r["NAC_Mes"]) + _float_or_0(r["EXP_Mes"])

            tot_anio= _float_or_0(r["NAC_Año"]) + _float_or_0(r["EXP_Año"])

            row = [

                "",

                _t(r["JERARQUIA2 G1"])[:60],

                _t(r["UND"]),

                _n(r["Stock Total"]),

                _n(r["PPTO_MES"]),

                _n(r["PRD_Dia"]), _n(r["PRD_Mes"]), _n(r["PRD_Año"]),

                _n(r["NAC_Dia"]), _n(r["NAC_Mes"]),

                _n(r["EXP_Dia"]), _n(r["EXP_Mes"]),

                _n(r["PPTO"]),

                _n(tot_dia), _n(tot_mes), _n(tot_anio)

            ]

            print_row(row, skip_sector_border=True)



        sec_tot = {k: _float_or_0(sub[k].sum()) for k in sums_cols}

        row_tot = [

            "", "TOTAL", "",

            _n(sec_tot["Stock Total"]),

            _n(sec_tot["PPTO_MES"]),

            _n(sec_tot["PRD_Dia"]), _n(sec_tot["PRD_Mes"]), _n(sec_tot["PRD_Año"]),

            _n(sec_tot["NAC_Dia"]), _n(sec_tot["NAC_Mes"]),

            _n(sec_tot["EXP_Dia"]), _n(sec_tot["EXP_Mes"]),

            _n(sec_tot["PPTO"]),

            _n(sec_tot["NAC_Dia"]+sec_tot["EXP_Dia"]),

            _n(sec_tot["NAC_Mes"]+sec_tot["EXP_Mes"]),

            _n(sec_tot["NAC_Año"]+sec_tot["EXP_Año"]),

        ]

        print_row(row_tot, bold=True, fill=True, skip_sector_border=True)



    return _pdf_bytes(pdf)





# ------------------------------------------------------------------------------

# Grupo 2

# ------------------------------------------------------------------------------

def build_pdf_group2(meta, df: pd.DataFrame) -> bytes:

    if "CENTRO" not in df.columns:

        df["CENTRO"] = df.get("PTOEXPEDICION", df.get("PTO EXPEDICION", df.get("PTO_EXPEDICION", "")))

    if "SECTOR" not in df.columns:

        df["SECTOR"] = "SIN SECTOR"

    _ensure_jer_col(df, "JERARQUIA2 G2")

    if "UND" not in df.columns:

        df["UND"] = df.get("UNIDAD DE MEDIDA BASE", "")



    if "Stock Total" not in df.columns:

        df["Stock Total"] = df.get("Stock", 0).fillna(0)



    num_cols = [

        "Stock Total", "PPTO_MES",

        "PRD_Dia", "PRD_Mes", "PRD_Año",

        "NAC_Dia", "NAC_Mes",

        "EXP_Dia", "EXP_Mes",

        "PPTO",

        "TOT_Dia", "TOT_Mes", "TOT_Año",

    ]

    for c in num_cols:

        if c not in df.columns:

            df[c] = 0



    if (df["TOT_Dia"] == 0).all():

        df["TOT_Dia"] = df["NAC_Dia"].fillna(0) + df["EXP_Dia"].fillna(0)

    if (df["TOT_Mes"] == 0).all():

        df["TOT_Mes"] = df["NAC_Mes"].fillna(0) + df["EXP_Mes"].fillna(0)



    import numpy as np

    if "ORDEN_CENTRO G4" in df.columns:

        ord_c = pd.to_numeric(df["ORDEN_CENTRO G4"], errors="coerce")

    elif "ORDEN_CENTRO_G4" in df.columns:

        ord_c = pd.to_numeric(df["ORDEN_CENTRO_G4"], errors="coerce")

    elif "ORDEN_CENTRO" in df.columns:

        ord_c = pd.to_numeric(df["ORDEN_CENTRO"], errors="coerce")

    else:

        ord_c = pd.Series(np.nan, index=df.index)



    df["__ord_c_g4"] = ord_c

    df = df.sort_values(["__ord_c_g4", "CENTRO", "SECTOR", "JERARQUIA2 G2", "UND"], ascending=[True]*5)



    pdf = FPDF(orientation="P", unit="mm", format="A4")

    pdf.set_margins(10, 12, 10)

    pdf.set_auto_page_break(True, margin=12)



    first_page = True

    def draw_page_header():

        nonlocal first_page

        if not first_page:

            return

        usable_w = pdf.w - pdf.l_margin - pdf.r_margin

        pdf.set_font(FONT, "B", 12)

        pdf.cell(usable_w, 7, title_with_date(meta), ln=1, align="C")

        pdf.set_font(FONT, "", 8.3)

        pdf.cell(usable_w, 4.5, f"Sociedad: {meta['sociedad']}     Sector: {meta['sector']}   Grupo: 2", ln=1, align="L")

        pdf.cell(usable_w, 4.5, f"Generado: {_now_lima_str()}", ln=1, align="L")

        pdf.ln(1)

        first_page = False



    pdf.add_page()

    draw_page_header()



    usable_w = pdf.w - pdf.l_margin - pdf.r_margin

    w_sector, w_jer2, w_und, w_stock, w_pptomes, w_sub = 22, 58, 9, 17, 14, 11.4

    total_w = w_sector + w_jer2 + w_und + w_stock + w_pptomes + (3+2+2+1+3)*w_sub

    scale = usable_w / total_w

    w_sector, w_jer2, w_und, w_stock, w_pptomes, w_sub = [round(x * scale, 2) for x in (w_sector, w_jer2, w_und, w_stock, w_pptomes, w_sub)]



    H = 3.9; H1 = 4.6; H2 = 3.6

    FONT_HDR_MAIN_B = 5.6; FONT_HDR_SUB_B  = 5.2

    FONT_ROW = 5.4; FONT_ROW_BOLD = 5.6



    def ensure_space(need):

        if pdf.get_y() + need > (pdf.h - pdf.b_margin):

            pdf.add_page()

            return True

        return False



    def box(x, y, w, h, txt="", align="C", is_sub=False):

        pdf.rect(x, y, w, h)

        if txt != "":

            pdf.set_xy(x, y + (0.35 if h >= H1 else 0.25))

            pdf.set_font(FONT, "B", FONT_HDR_MAIN_B if not is_sub else FONT_HDR_SUB_B)

            pdf.cell(w, h, txt, border=0, align=align)



    def box_ppto_mes(x, y, w, h):

        pdf.rect(x, y, w, h)

        pdf.set_font(FONT, "B", FONT_HDR_MAIN_B)

        pdf.set_xy(x, y + 0.45)

        pdf.cell(w, H1 - 0.7, "PPTO", border=0, align="C")

        pdf.set_font(FONT, "B", FONT_HDR_SUB_B)

        pdf.set_xy(x, y + H1 - 0.1)

        pdf.cell(w, H2, "MES", border=0, align="C")



    def table_header():

        ensure_space(H1 + H2 + 0.5)

        y0 = pdf.get_y(); x  = pdf.l_margin

        box(x, y0, w_sector, H1+H2, "SECTOR", align="L"); x += w_sector

        box(x, y0, w_jer2,   H1+H2, "JERARQUIA2 G2", align="L"); x += w_jer2

        box(x, y0, w_und,    H1+H2, "UND"); x += w_und

        box(x, y0, w_stock,  H1+H2, "Stock"); x += w_stock

        box_ppto_mes(x, y0, w_pptomes, H1+H2); x += w_pptomes



        def group(title, labels):

            nonlocal x

            width = len(labels)*w_sub

            box(x, y0, width, H1, title)

            xx = x

            for lab in labels:

                box(xx, y0+H1, w_sub, H2, lab, is_sub=True)

                xx += w_sub

            x += width



        group("PRD",        ("Día","Mes","Año"))

        group("NACIONALES", ("Día","Mes"))

        group("EXPORTACIÓN",("Día","Mes"))

        group("PPTO",       ("Mes",))

        group("TOTALES",    ("Día","Mes","Año"))

        pdf.set_y(y0 + H1 + H2)



    def draw_sector_label(x_left: float, y_top: float, height: float, sector_name: str):

        label = (sector_name or "").upper()

        fs = 6.6; min_fs = 4.4

        pdf.set_font(FONT, "B", fs)

        max_w = w_sector - 1.5

        while pdf.get_string_width(label) > max_w and fs > min_fs:

            fs -= 0.2

            pdf.set_font(FONT, "B", fs)

        line_h = fs + 1.1

        y_text = y_top + max(0, (height - line_h) / 2.0)

        pdf.set_xy(x_left, y_text)

        pdf.cell(w_sector, line_h, label, border=0, align="C")



    def print_row(row_vals, bold=False, fill=False, skip_sector_border=False):

        widths = [w_sector, w_jer2, w_und, w_stock, w_pptomes] + [w_sub]*(3+2+2+1+3)

        ensure_space(H + 0.15)

        pdf.set_fill_color(235,235,235) if fill else pdf.set_fill_color(255,255,255)

        pdf.set_font(FONT, "B" if bold else "", FONT_ROW_BOLD if bold else FONT_ROW)

        for i, (w,v) in enumerate(zip(widths, row_vals)):

            align = "L" if i in (0,1) else "C"

            if i == 0 and skip_sector_border:

                pdf.cell(w, H, "", border=0, fill=False, align=align)

            else:

                pdf.cell(w, H, v, border=1, fill=False, align=align)

        pdf.ln(0)



    def band_title(texto):

        pdf.set_font(FONT, "B", 7.2)

        pdf.set_fill_color(220,235,255)

        pdf.cell(0, H+0.5, texto or "Sin centro", border=1, ln=1, fill=True)

        table_header()



    for centro, df_c in df.groupby("CENTRO", dropna=False, sort=False):

        band_title(centro)

        for sector, sub_sec in df_c.groupby("SECTOR", dropna=False, sort=False):

            sub_agg = (

                sub_sec.groupby(["JERARQUIA2 G2","UND"], dropna=False)[num_cols]

                .sum(numeric_only=True)

                .reset_index()

                .sort_values(["JERARQUIA2 G2","UND"])

            )



            rows_data = len(sub_agg)

            block_rows = rows_data + 1

            block_height = block_rows*H



            if pdf.get_y() + block_height > (pdf.h - pdf.b_margin):

                pdf.add_page()

                band_title(centro)



            x_left = pdf.l_margin; y_top  = pdf.get_y()

            pdf.rect(x_left, y_top, w_sector, block_height)

            draw_sector_label(x_left, y_top, block_height, sector)

            pdf.set_xy(pdf.l_margin, y_top)



            for _, r in sub_agg.iterrows():

                row = [

                    "",

                    _t(r["JERARQUIA2 G2"])[:70],

                    _t(r["UND"]),

                    _n(r["Stock Total"]),

                    _n(r["PPTO_MES"]),

                    _n(r["PRD_Dia"]), _n(r["PRD_Mes"]), _n(r["PRD_Año"]),

                    _n(r["NAC_Dia"]), _n(r["NAC_Mes"]),

                    _n(r["EXP_Dia"]), _n(r["EXP_Mes"]),

                    _n(r["PPTO"]),

                    _n(r["TOT_Dia"]), _n(r["TOT_Mes"]), _n(r["TOT_Año"]),

                ]

                print_row(row, skip_sector_border=True)



            tot = sub_agg[num_cols].sum(numeric_only=True)

            row_tot = [

                "",

                "TOTAL",

                "",

                _n(tot["Stock Total"]),

                _n(tot["PPTO_MES"]),

                _n(tot["PRD_Dia"]), _n(tot["PRD_Mes"]), _n(tot["PRD_Año"]),

                _n(tot["NAC_Dia"]), _n(tot["NAC_Mes"]),

                _n(tot["EXP_Dia"]), _n(tot["EXP_Mes"]),

                _n(tot["PPTO"]),

                _n(tot["TOT_Dia"]), _n(tot["TOT_Mes"]), _n(tot["TOT_Año"]),

            ]

            print_row(row_tot, bold=True, fill=True, skip_sector_border=True)



    return _pdf_bytes(pdf)





# ----------------------------------------------------------------------
# Grupo 3 (Jerarquía 2 con celda única centrada, sumarizado por código)
# Orden (IDs numéricos del SP):
#   1) ORDEN_CENTRO G3 asc
#   2) ORDEN_JERARQUIA2 G3 asc
#   3) ORDEN_MATERIAL G3 asc
# Centro visible = CENTRO.
# ----------------------------------------------------------------------
def build_pdf_group3(meta, df: pd.DataFrame) -> bytes:
    import numpy as np

    # ------------------------ helper: pick order col (robusto) ------------------------
    def _pick_order_col(df_: pd.DataFrame, candidates):
        for c in candidates:
            if c in df_.columns:
                if df_[c].astype(str).str.strip().ne("").any():
                    return c
        for c in candidates:
            if c in df_.columns:
                return c
        return None

    # ------------------------ Normalización base ------------------------
    _ensure_jer_col(df, "JERARQUIA2 G3")
    if "CENTRO" not in df.columns:
        df["CENTRO"] = df.get("CENTROSAP", df.get("PLANTA", ""))
    if "SECTOR" not in df.columns:
        df["SECTOR"] = "SIN SECTOR"
    if "UND" not in df.columns:
        df["UND"] = df.get("UNIDAD DE MEDIDA BASE", "")

    def _ensure_col(df_, target, candidates, default=""):
        if target not in df_.columns:
            for c in candidates:
                if c in df_.columns:
                    df_[target] = df_[c]
                    break
            else:
                df_[target] = default

    # Código y descripción de material
    _ensure_col(
        df, "COD_MAT",
        ["COD_MATERIAL", "CODIGO", "CODIGO MATERIAL", "CodMaterial", "Cod_Mat"],
        default=""
    )
    _ensure_col(
        df, "DESC_MAT",
        ["MATERIAL", "DESC_MATERIAL", "DESCRIPCION", "DESCRIPCION MATERIAL",
         "Material", "Texto breve de material"],
        default=""
    )
    _ensure_col(
        df, "CANAL G3",
        ["CANAL G3", "CANAL", "CANAL_VENTA", "CANALVENTA", "CANAL_VENTAS", "CANAL VENTAS"],
        default="SIN CANAL"
    )
    df["CANAL G3"] = df["CANAL G3"].fillna("SIN CANAL")
    df.loc[df["CANAL G3"].astype(str).str.strip() == "", "CANAL G3"] = "SIN CANAL"

    # ------------------------ ELIMINAR MATERIALES CAUTIVOS ------------------------
    df = df[
        ~df["DESC_MAT"].astype(str).str.upper().str.contains("CAUTIVO", na=False)
        & ~df["COD_MAT"].astype(str).str.upper().str.contains("CAUTIVO", na=False)
    ].copy()

    # ------------------------ STOCK TOTAL ------------------------
    if "Stock Total" not in df.columns:
        df["Stock Total"] = df.get("Stock", 0).fillna(0)

    # ------------------------ Numéricos base ------------------------
    num_cols = [
        "Stock Total", "PPTO_MES",
        "PRD_Dia", "PRD_Mes", "PRD_Año",
        "NAC_Dia", "NAC_Mes", "NAC_Año",
        "EXP_Dia", "EXP_Mes", "EXP_Año",
        "PPTO", "TOT_Dia", "TOT_Mes", "TOT_Año"
    ]
    display_num_cols = [num_cols[i] for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14)]
    for c in num_cols:
        if c not in df.columns:
            df[c] = 0

    # Totales (siempre derivados de Nacionales + Exportacion)
    df["TOT_Dia"] = df["NAC_Dia"].fillna(0) + df["EXP_Dia"].fillna(0)
    df["TOT_Mes"] = df["NAC_Mes"].fillna(0) + df["EXP_Mes"].fillna(0)
    df["TOT_Año"] = df["NAC_Año"].fillna(0) + df["EXP_Año"].fillna(0)

    # ------------------------------------------------------------------
    # ORDEN G3 (ROBUSTO) - estilo Grupo 4
    # Regla G3: CENTRO -> JERARQUIA2 G3 -> MATERIAL (IDs numéricos)
    # ------------------------------------------------------------------
    ord_c3_col = _pick_order_col(df, ["ORDEN_CENTRO G3", "ORDEN_CENTRO_G3", "ORDEN_CENTRO"])
    ord_j3_col = _pick_order_col(df, ["ORDEN_JERARQUIA2 G3", "ORDEN_JERARQUIA2_G3", "ORDEN_JERARQUIA2"])
    # nuevo campo de orden de materiales (ascendente)
    ord_m3_col = _pick_order_col(
        df,
        [
            "ORDEN_MATERIAL G3",  # nombre habitual con guion bajo + espacio
            "ORDEN MATERIAL G3",  # variante sin guion bajo
            "ORDEN_MATERIAL_G3",
            "ORDEN_MATERIAL",
            "ORDEN MATERIAL",
        ],
    )

    ord_c = pd.to_numeric(df[ord_c3_col], errors="coerce") if ord_c3_col else pd.Series(np.nan, index=df.index)
    ord_j = pd.to_numeric(df[ord_j3_col], errors="coerce") if ord_j3_col else pd.Series(np.nan, index=df.index)
    ord_m = pd.to_numeric(df[ord_m3_col], errors="coerce") if ord_m3_col else pd.Series(np.nan, index=df.index)

    df["__ord_c3"] = ord_c
    df["__ord_j3"] = ord_j
    df["__ord_m3"] = ord_m

    # ------------------------------------------------------------------
    # AGRUPACIÓN: CENTRO + SECTOR + JERARQUIA2 G3 + COD_MAT
    # Mantener IDs de orden con MIN dentro del grupo (igual que G4)
    # ------------------------------------------------------------------
    agg_dict = {c: "sum" for c in num_cols}
    agg_dict["DESC_MAT"] = "first"
    agg_dict["UND"] = "first"
    agg_dict["CANAL G3"] = "first"
    agg_dict["__ord_c3"] = "min"
    agg_dict["__ord_j3"] = "min"
    agg_dict["__ord_m3"] = "min"

    g = (
        df.groupby(["CENTRO", "SECTOR", "JERARQUIA2 G3", "COD_MAT"], dropna=False)
          .agg(agg_dict)
          .reset_index()
    )

    # descartar filas/materiales cuyo bloque queda 100% en cero
    num_vals = g[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    zero_mask = num_vals.abs().sum(axis=1) == 0
    g = g.loc[~zero_mask].reset_index(drop=True)

    # NaN al final (estilo G4)
    g["__ord_c3_fill"] = g["__ord_c3"].fillna(float("inf"))
    g["__ord_j3_fill"] = g["__ord_j3"].fillna(float("inf"))
    g["__ord_m3_fill"] = g["__ord_m3"].fillna(float("inf"))

    # ORDEN FINAL G3 (Centro -> Jerarquía2 -> Material)
    g = g.sort_values(
        by=["__ord_c3_fill", "__ord_j3_fill", "__ord_m3_fill", "CENTRO", "JERARQUIA2 G3", "COD_MAT"],
        ascending=[True, True, True, True, True, True],
        kind="mergesort"
    ).reset_index(drop=True)

    # ------------------------ PDF --------------------------------------
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_margins(10, 12, 10)
    pdf.set_auto_page_break(True, margin=12)
    first_page = True

    def draw_first_page_header():
        usable_w = pdf.w - pdf.l_margin - pdf.r_margin
        pdf.set_text_color(0, 0, 0)
        pdf.set_draw_color(180, 180, 180)
        pdf.set_x(pdf.l_margin)
        pdf.set_font(FONT, "B", 12)
        pdf.cell(usable_w, 7, title_with_date(meta), new_y="NEXT", align="C")
        pdf.set_x(pdf.l_margin)
        pdf.set_font(FONT, "", 9)
        pdf.cell(
            usable_w, 5,
            f"Sociedad: {meta['sociedad']}     Sector: {meta['sector']}    Grupo: 3",
            new_y="NEXT", align="L"
        )
        pdf.set_x(pdf.l_margin)
        pdf.cell(
            usable_w, 5,
            f"Generado: {_now_lima_str()}",
            new_y="NEXT", align="L"
        )
        y = pdf.get_y()
        pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
        pdf.ln(4)

    def new_page():
        nonlocal first_page
        pdf.add_page()
        if first_page:
            draw_first_page_header()
            first_page = False

    new_page()

    usable_w = pdf.w - pdf.l_margin - pdf.r_margin
    w_jer2, w_cod, w_desc, w_und, w_stock, w_pptomes, w_sub = 34, 14, 47, 9, 12, 10, 11.4
    total_w = (
        w_jer2 + w_cod + w_desc + w_und +
        w_stock + w_pptomes + (3 + 3 + 3 + 3) * w_sub
    )
    scale = usable_w / total_w
    w_jer2, w_cod, w_desc, w_und, w_stock, w_pptomes, w_sub = [
        round(x * scale, 2)
        for x in (w_jer2, w_cod, w_desc, w_und, w_stock, w_pptomes, w_sub)
    ]

    H, H1, H2 = 3.9, 4.6, 3.6
    FONT_HDR_MAIN_B = 5.6
    FONT_HDR_SUB_B = 5.2
    FONT_ROW_TXT = 4.4
    FONT_ROW_NUM = 5.2

    def ensure_space(need):
        if pdf.get_y() + need > (pdf.h - pdf.b_margin):
            new_page()
            return True
        return False

    def header_cell(x, y, w, h, txt, sub=False, align="C"):
        pdf.rect(x, y, w, h)
        if not txt:
            return
        if "\n" in txt:
            top, bottom = txt.split("\n", 1)
            pdf.set_xy(x, y + 0.25)
            pdf.set_font(FONT, "B", FONT_HDR_MAIN_B)
            pdf.cell(w, h / 2, top, border=0, align=align)
            pdf.set_xy(x, y + h / 2 - 0.1)
            pdf.set_font(FONT, "B", FONT_HDR_SUB_B)
            pdf.cell(w, h / 2, bottom, border=0, align=align)
        else:
            pdf.set_xy(x, y + (0.35 if not sub else 0.25))
            pdf.set_font(FONT, "B", FONT_HDR_MAIN_B if not sub else FONT_HDR_SUB_B)
            pdf.cell(w, h, txt, border=0, align=align)

    def table_header():
        ensure_space(H1 + H2 + 0.5)
        y0 = pdf.get_y()
        x = pdf.l_margin

        header_cell(x, y0, w_jer2, H1 + H2, "Jerarquía 2", align="L"); x += w_jer2
        header_cell(x, y0, w_cod,  H1 + H2, "Cod. Mat.", align="C"); x += w_cod
        header_cell(x, y0, w_desc, H1 + H2, "Desc. Mat.", align="C"); x += w_desc
        header_cell(x, y0, w_und,  H1 + H2, "Und", align="C"); x += w_und
        header_cell(x, y0, w_stock, H1 + H2, "Stock", align="C"); x += w_stock
        header_cell(x, y0, w_pptomes, H1 + H2, "PPTO-\nMES", align="C"); x += w_pptomes

        def group(title, labels):
            nonlocal x
            width = len(labels) * w_sub
            header_cell(x, y0, width, H1, title)
            xx = x
            for lab in labels:
                header_cell(xx, y0 + H1, w_sub, H2, lab, sub=True)
                xx += w_sub
            x += width

        group("PRD", ("Día", "Mes", "Año"))
        group("NACIONALES", ("Día", "Mes", "Año"))
        group("EXPORTACIÓN", ("Día", "Mes", "Año"))
        group("TOTALES", ("Día", "Mes", "Año"))

        pdf.set_y(y0 + H1 + H2)

    def print_row(vals, bold=False, fill=False, skip_jer=False, check_space=True):
        widths = [w_jer2, w_cod, w_desc, w_und, w_stock, w_pptomes] + [w_sub] * (3 + 3 + 3 + 3)
        if check_space:
            ensure_space(H + 0.2)

        pdf.set_fill_color(235, 235, 235) if fill else pdf.set_fill_color(255, 255, 255)

        for i, (w, v) in enumerate(zip(widths, vals)):
            if i == 0 and skip_jer:
                pdf.cell(w, H, "", border=0, align="L", fill=False)
                continue

            if i <= 2:
                pdf.set_font(FONT, "B" if bold else "", FONT_ROW_TXT)
                pdf.cell(w, H, v, border=1, align="L", fill=fill)
            else:
                pdf.set_font(FONT, "B" if bold else "", FONT_ROW_NUM)
                pdf.cell(w, H, v, border=1, align="C", fill=fill)

        pdf.ln(0)

    def section_band(centro, sector):
        ensure_space(H + 1.2)
        pdf.set_font(FONT, "B", 8)
        pdf.set_fill_color(220, 235, 255)
        pdf.cell(0, H + 0.8, f"CENTRO: {centro}", border=1, new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.cell(0, H + 0.8, f"SECTOR: {sector}", border=1, new_x="LMARGIN", new_y="NEXT", fill=True)
        table_header()

    bottom = pdf.h - pdf.b_margin

    # ------------------- Impresión por Centro y Sector -------------------
    for (centro, sector), sub_cs in g.groupby(["CENTRO", "SECTOR"], dropna=False, sort=False):
        section_band(centro, sector)

        center_key = _t(centro).strip().upper()
        sector_key = _t(sector).strip().upper()
        show_combined_sales_total = sector_key == "SALES" and center_key in {"OQUENDO", "HUACHO"}
        combined_g3_tot = pd.Series(0.0, index=num_cols, dtype="float64")
        combined_g3_has_human = False
        combined_g3_has_industrial = False

        for jer2, sub in sub_cs.groupby("JERARQUIA2 G3", dropna=False, sort=False):
            is_sales = str(sector).strip().upper() == "SALES"
            canal_groups = list(sub.groupby("CANAL G3", dropna=False, sort=False)) if is_sales else None
            show_canal_subtotals = False
            if canal_groups is not None:
                labels = [_canal_detail_label(canal) for canal, _ in canal_groups]
                show_canal_subtotals = len(set(labels)) > 1

            subtotal_rows = len(canal_groups) if (canal_groups is not None and show_canal_subtotals) else 0
            rows_data = len(sub) + subtotal_rows
            block_rows = rows_data + 1
            block_height = block_rows * H

            if pdf.get_y() + block_height > bottom:
                pdf.add_page()
                section_band(centro, sector)

            # Jerarquía centrada en un solo bloque
            x_left = pdf.l_margin
            y_top = pdf.get_y()
            pdf.rect(x_left, y_top, w_jer2, block_height)

            label = (str(jer2) or "").upper()
            fs = 6.0
            pdf.set_font(FONT, "B", fs)
            max_w = w_jer2 - 1.5
            while pdf.get_string_width(label) > max_w and fs > 4.0:
                fs -= 0.2
                pdf.set_font(FONT, "B", fs)

            line_h = fs + 0.8
            y_text = y_top + max(0, (block_height - line_h) / 2.0)
            pdf.set_xy(x_left, y_text)
            pdf.cell(w_jer2, line_h, label, border=0, align="C")

            pdf.set_xy(pdf.l_margin, y_top)

            # Filas de materiales (+ subtotales por canal para Sales)
            if canal_groups is not None:
                for canal, sub_can in canal_groups:
                    for _, r in sub_can.iterrows():
                        row = [
                            "",
                            str(r["COD_MAT"])[:18],
                            str(r["DESC_MAT"])[:55],
                            _t(r["UND"]),
                            _fmt(r["Stock Total"]),
                            _fmt(r["PPTO_MES"]),
                            _fmt(r["PRD_Dia"]),
                            _fmt(r["PRD_Mes"]),
                            _fmt(r["PRD_Año"]),
                            _fmt(r["NAC_Dia"]),
                            _fmt(r["NAC_Mes"]),
                            _fmt(r["NAC_Año"]),
                            _fmt(r["EXP_Dia"]),
                            _fmt(r["EXP_Mes"]),
                            _fmt(r["EXP_Año"]),
                            _fmt(r["TOT_Dia"]),
                            _fmt(r["TOT_Mes"]),
                            _fmt(r["TOT_Año"]),
                        ]
                        print_row(row, bold=False, fill=False, skip_jer=True, check_space=False)

                    if show_canal_subtotals:
                        tot_can = sub_can[num_cols].sum(numeric_only=True)
                        row_sub = [
                            "",
                            "TOTAL",
                            _canal_detail_label(canal),
                            "",
                            _fmt(tot_can["Stock Total"]),
                            _fmt(tot_can["PPTO_MES"]),
                            _fmt(tot_can["PRD_Dia"]),
                            _fmt(tot_can["PRD_Mes"]),
                            _fmt(tot_can["PRD_Año"]),
                            _fmt(tot_can["NAC_Dia"]),
                            _fmt(tot_can["NAC_Mes"]),
                            _fmt(tot_can["NAC_Año"]),
                            _fmt(tot_can["EXP_Dia"]),
                            _fmt(tot_can["EXP_Mes"]),
                            _fmt(tot_can["EXP_Año"]),
                            _fmt(tot_can["TOT_Dia"]),
                            _fmt(tot_can["TOT_Mes"]),
                            _fmt(tot_can["TOT_Año"]),
                        ]
                        print_row(row_sub, bold=True, fill=True, skip_jer=True, check_space=False)
            else:
                for _, r in sub.iterrows():
                    row = [
                        "",
                        str(r["COD_MAT"])[:18],
                        str(r["DESC_MAT"])[:55],
                        _t(r["UND"]),
                        _fmt(r["Stock Total"]),
                        _fmt(r["PPTO_MES"]),
                        _fmt(r["PRD_Dia"]),
                        _fmt(r["PRD_Mes"]),
                        _fmt(r["PRD_Año"]),
                        _fmt(r["NAC_Dia"]),
                        _fmt(r["NAC_Mes"]),
                        _fmt(r["NAC_Año"]),
                        _fmt(r["EXP_Dia"]),
                        _fmt(r["EXP_Mes"]),
                        _fmt(r["EXP_Año"]),
                        _fmt(r["TOT_Dia"]),
                        _fmt(r["TOT_Mes"]),
                        _fmt(r["TOT_Año"]),
                    ]
                    print_row(row, bold=False, fill=False, skip_jer=True, check_space=False)

            # Totales por jerarquía
            tot = sub[num_cols].sum(numeric_only=True)
            jer2_key = _t(jer2).strip().upper()
            if show_combined_sales_total:
                if jer2_key in {"CONSUMO HUMANO", "SAL CONSUMO HUMANO"}:
                    combined_g3_tot = combined_g3_tot.add(tot, fill_value=0)
                    combined_g3_has_human = True
                elif jer2_key == "SAL INDUSTRIAL":
                    combined_g3_tot = combined_g3_tot.add(tot, fill_value=0)
                    combined_g3_has_industrial = True
            total_abs_label = _t(jer2).upper()[:40] if canal_groups is not None else ""
            row_tot = [
                "",
                "TOTAL",
                total_abs_label,
                "",
                _fmt(tot["Stock Total"]),
                _fmt(tot["PPTO_MES"]),
                _fmt(tot["PRD_Dia"]),
                _fmt(tot["PRD_Mes"]),
                _fmt(tot["PRD_Año"]),
                _fmt(tot["NAC_Dia"]),
                _fmt(tot["NAC_Mes"]),
                _fmt(tot["NAC_Año"]),
                _fmt(tot["EXP_Dia"]),
                _fmt(tot["EXP_Mes"]),
                _fmt(tot["EXP_Año"]),
                _fmt(tot["TOT_Dia"]),
                _fmt(tot["TOT_Mes"]),
                _fmt(tot["TOT_Año"]),
            ]
            print_row(row_tot, bold=True, fill=True, skip_jer=True, check_space=False)

            if (
                show_combined_sales_total
                and jer2_key == "SAL INDUSTRIAL"
                and combined_g3_has_human
                and combined_g3_has_industrial
            ):
                row_combined = [
                    "",
                    "TOTAL",
                    "CONSUMO HUMANO + SAL INDUSTRIAL",
                    "",
                    *[_fmt(combined_g3_tot[k]) for k in display_num_cols],
                ]
                print_row(row_combined, bold=True, fill=True, skip_jer=True, check_space=True)

    return _pdf_bytes(pdf)






# ------------------------------------------------------------------------------

# Grupo 4 (Informe Diario Resumen)

# - Orden: ORDEN_JERARQUIA2 G4 asc, luego ORDEN_CENTRO G4 asc

# - Usa columnas: "Stock Total" y "Stock Disponible"

# ------------------------------------------------------------------------------

def build_pdf_group4(meta, df: pd.DataFrame) -> bytes:

    def ensure(col, default=0):

        if col not in df.columns:

            df[col] = default



    for c in ["PRD_Dia","PRD_Mes","PRD_Año",

              "NAC_Dia","NAC_Mes","NAC_Año",

              "EXP_Dia","EXP_Mes","EXP_Año"]:

        ensure(c, 0)



    if "Stock Total" not in df.columns:

        df["Stock Total"] = 0

    if "Stock Disponible" not in df.columns:

        df["Stock Disponible"] = 0



    if "UND" not in df.columns:

        df["UND"] = df.get("UNIDAD DE MEDIDA BASE", "")

    _ensure_jer_col(df, "JERARQUIA2 G4")



    if "CENTRO_G4" not in df.columns and "CENTRO G4" in df.columns:

        df["CENTRO_G4"] = df["CENTRO G4"]



    if "CENTRO_G4" in df.columns and df["CENTRO_G4"].astype(str).str.strip().ne("").any():

        CENTER_COL = "CENTRO_G4"

        CENTER_LABEL = "CENTRO"

    else:

        if "CENTRO" not in df.columns:

            cand = [c for c in df.columns if c.upper() in ("CENTRO","CENTROSAP","PLANTA","CODCENTRO")]

            df["CENTRO"] = df[cand[0]] if cand else ""

        CENTER_COL = "CENTRO"

        CENTER_LABEL = "CENTRO"



    # -------------------- ORDEN G4 (ROBUSTO) --------------------

    import numpy as np

    ord_j4_col = _pick_order_col(df, ["ORDEN_JERARQUIA2 G4","ORDEN_JERARQUIA2_G4","ORDEN_JERARQUIA2"])

    ord_c4_col = _pick_order_col(df, ["ORDEN_CENTRO G4","ORDEN_CENTRO_G4","ORDEN_CENTRO"])



    ord_j = pd.to_numeric(df[ord_j4_col], errors="coerce") if ord_j4_col else pd.Series(np.nan, index=df.index)

    ord_c = pd.to_numeric(df[ord_c4_col], errors="coerce") if ord_c4_col else pd.Series(np.nan, index=df.index)



    df["__ord_j2"]  = ord_j

    df["__ord_cg4"] = ord_c



    sum_cols = ["Stock Total", "Stock Disponible",

                "PRD_Dia","PRD_Mes","PRD_Año",

                "NAC_Dia","NAC_Mes","NAC_Año",

                "EXP_Dia","EXP_Mes","EXP_Año"]



    agg_dict = {k: "sum" for k in sum_cols}

    agg_dict.update({"UND": "first", "__ord_j2": "min", "__ord_cg4": "min"})



    g = (df.groupby(["JERARQUIA2 G4", CENTER_COL], dropna=False)
           .agg(agg_dict)
           .reset_index())

    # descartar bloques/productos cuyo total y centros están 100% en cero
    num_vals = g[sum_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    zero_mask = num_vals.abs().sum(axis=1) == 0
    g = g.loc[~zero_mask].reset_index(drop=True)



    g["__ord_j2_fill"]  = g["__ord_j2"].fillna(float("inf"))

    g["__ord_cg4_fill"] = g["__ord_cg4"].fillna(float("inf"))



    g = g.sort_values(

        by=["__ord_j2_fill", "__ord_cg4_fill", "JERARQUIA2 G4", CENTER_COL],

        ascending=[True, True, True, True],

        kind="mergesort"

    ).reset_index(drop=True)



    pdf = FPDF(orientation="P", unit="mm", format="A4")

    pdf.set_margins(10, 12, 10)

    # margen inferior ampliado para evitar que el footer se superponga con la tabla
    pdf.set_auto_page_break(True, margin=18)



    def _footer(_self=pdf):

        _self.set_y(-13)

        _self.set_font(FONT, "", 7)

        _self.cell(0, 3, "(*) Stock Disponible = Stock Total - Stock Cautivo - Stock Tránsito", ln=1, align="L")

        _self.cell(0, 5, "Grupo 4: Informe diario resumen", ln=0, align="L")

        _self.set_xy(_self.w - _self.r_margin - 25, _self.h - 9)

        _self.cell(25, 5, f"Pag. {_self.page_no()}", align="R")



    pdf.footer = _footer  # type: ignore



    first_page = True

    def draw_first_page_header():

        usable_w = pdf.w - pdf.l_margin - pdf.r_margin

        pdf.set_x(pdf.l_margin); pdf.set_font(FONT, "B", 12)

        pdf.cell(usable_w, 7, title_with_date(meta), new_y="NEXT", align="C")

        pdf.set_x(pdf.l_margin); pdf.set_font(FONT, "", 9)

        pdf.cell(usable_w, 5, f"Sociedad: {meta['sociedad']}  Sector: {meta['sector']}    Grupo: 4", new_y="NEXT", align="L")

        pdf.set_x(pdf.l_margin)

        pdf.cell(usable_w, 5, f"Generado: {_now_lima_str()}", new_y="NEXT", align="L")

        pdf.set_draw_color(180, 180, 180)

        y = pdf.get_y()

        pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)

        pdf.ln(6)



    def new_page():

        nonlocal first_page

        pdf.add_page()

        if first_page:

            draw_first_page_header()

            first_page = False



    new_page()



    usable_w = pdf.w - pdf.l_margin - pdf.r_margin

    w_code, w_und, w_stock_tot, w_stock_disp, w_sub = 35, 10, 18, 18, 14

    total_w = w_code + w_und + w_stock_tot + w_stock_disp + 9*w_sub

    scale = usable_w / total_w

    w_code, w_und, w_stock_tot, w_stock_disp, w_sub = [round(x*scale, 2) for x in (w_code, w_und, w_stock_tot, w_stock_disp, w_sub)]



    H, H1, H2 = 6, 7, 6

    GAP = 3  # mm



    def ensure_space(need: float, min_rows_after: int = 0):

        bottom = pdf.h - pdf.b_margin

        need_total = need + (min_rows_after * H)

        if pdf.get_y() + need_total > bottom:

            new_page()

            return True

        return False



    def draw_table_header():

        ensure_space(H1 + H2 + 1, min_rows_after=1)

        y0 = pdf.get_y()

        x  = pdf.l_margin



        def box(xb, yb, w, h, txt="", bold=False, align="C"):

            pdf.rect(xb, yb, w, h)

            if txt:

                pdf.set_xy(xb, yb + 0.8)

                pdf.set_font(FONT, "B" if bold else "", 8)

                pdf.cell(w, h, txt, border=0, align=align)



        box(x, y0, w_code, H1+H2, CENTER_LABEL, True, "L"); x += w_code

        box(x, y0, w_und, H1+H2, "Und", True); x += w_und



        box(x, y0, w_stock_tot, H1+H2, "", True)

        pdf.set_font(FONT, "B", 8)

        pdf.set_xy(x, y0 + 1)

        pdf.cell(w_stock_tot, 4, "Stock", 0, 2, "C")

        pdf.cell(w_stock_tot, 4, "Total", 0, 0, "C")

        x += w_stock_tot



        box(x, y0, w_stock_disp, H1+H2, "", True)

        pdf.set_font(FONT, "B", 8)

        pdf.set_xy(x, y0 + 1)

        pdf.cell(w_stock_disp, 4, "(*)Stock", 0, 2, "C")

        pdf.cell(w_stock_disp, 4, "Disponible", 0, 0, "C")

        x += w_stock_disp



        def group(title):

            nonlocal x

            width = 3 * w_sub

            box(x, y0, width, H1, title, True)

            xx = x

            for lab in ("Día", "Mes", "Año"):

                box(xx, y0 + H1, w_sub, H2, lab)

                xx += w_sub

            x += width



        group("Producción")

        group("Ventas locales")

        group("Exportaciones")

        pdf.set_y(y0 + H1 + H2)



    def print_row(code_text, values, bold=False, fill=False):

        widths = [w_code, w_und, w_stock_tot, w_stock_disp] + [w_sub]*9

        ensure_space(H + 1)

        pdf.set_fill_color(235, 235, 235) if fill else pdf.set_fill_color(255, 255, 255)

        pdf.set_font(FONT, "B" if bold else "", 8)

        row_vals = [code_text] + values

        for i, (w, v) in enumerate(zip(widths, row_vals)):

            align = "L" if i == 0 else "R"

            pdf.cell(w, H, v, border=1, align=align, fill=fill)

        pdf.ln(0)



    def section_gap():

        ensure_space(GAP)

        pdf.ln(GAP)



    def band_title(text: str, is_first: bool):

        if not is_first:

            section_gap()

        ensure_space(H + H1 + H2 + 2, min_rows_after=1)

        pdf.set_fill_color(204, 232, 204)

        pdf.set_font(FONT, "B", 9)

        pdf.cell(0, H, text, border=1, ln=1, fill=True)

        draw_table_header()



    current_product = None

    first_block = True



    for _, r in g.iterrows():

        prod = (_t(r["JERARQUIA2 G4"]) or "").upper()

        if prod != current_product:

            current_product = prod

            band_title(current_product, is_first=first_block)

            first_block = False



            block = g[g["JERARQUIA2 G4"] == r["JERARQUIA2 G4"]]

            tot = {k: _float_or_0(block[k].sum()) for k in sum_cols}



            values_total = [

                _t(block["UND"].iloc[0] if len(block) else ""),

                _n(tot["Stock Total"]),

                _n(tot["Stock Disponible"]),

                _n(tot["PRD_Dia"]), _n(tot["PRD_Mes"]), _n(tot["PRD_Año"]),

                _n(tot["NAC_Dia"]), _n(tot["NAC_Mes"]), _n(tot["NAC_Año"]),

                _n(tot["EXP_Dia"]), _n(tot["EXP_Mes"]), _n(tot["EXP_Año"]),

            ]

            print_row("TOTAL", values_total, bold=True, fill=True)



        values = [

            _t(r["UND"]),

            _n(r["Stock Total"]),

            _n(r["Stock Disponible"]),

            _n(r["PRD_Dia"]), _n(r["PRD_Mes"]), _n(r["PRD_Año"]),

            _n(r["NAC_Dia"]), _n(r["NAC_Mes"]), _n(r["NAC_Año"]),

            _n(r["EXP_Dia"]), _n(r["EXP_Mes"]), _n(r["EXP_Año"]),

        ]

        print_row(_t(r[CENTER_COL] or ""), values, bold=False, fill=False)



    return _pdf_bytes(pdf)



# ------------------------------------------------------------------------------
# Grupo 5 (Sales): Centro -> Tipo G5 (subtotal) -> Jerarquia2 (detalle)
# ------------------------------------------------------------------------------
def build_pdf_group5(meta, df: pd.DataFrame) -> bytes:
    import numpy as np

    def _ensure_col(df_, target, candidates, default=""):
        if target not in df_.columns:
            for c in candidates:
                if c in df_.columns:
                    df_[target] = df_[c]
                    break
            else:
                df_[target] = default

    if "CENTRO" not in df.columns:
        df["CENTRO"] = df.get("CENTROSAP", df.get("PLANTA", ""))
    if "SECTOR" not in df.columns:
        df["SECTOR"] = "SIN SECTOR"
    if "UND" not in df.columns:
        df["UND"] = df.get("UNIDAD DE MEDIDA BASE", "")

    _ensure_jer_col(df, "JERARQUIA2")
    _ensure_col(df, "TIPO G5", ["TIPO G5", "TIPO_G5", "TIPOG5", "TIPO"], default="SIN TIPO")
    df["TIPO G5"] = df["TIPO G5"].fillna("SIN TIPO")
    df.loc[df["TIPO G5"].astype(str).str.strip() == "", "TIPO G5"] = "SIN TIPO"
    df["JERARQUIA2"] = df["JERARQUIA2"].fillna("SIN JERARQUIA")
    df.loc[df["JERARQUIA2"].astype(str).str.strip() == "", "JERARQUIA2"] = "SIN JERARQUIA"

    # Grupo 5 aplica solo a Sales y excluye MATERIA PRIMA en TIPO G5/JERARQUIA2.
    df = df[df["SECTOR"].astype(str).str.upper() == "SALES"].copy()
    df = df[df["TIPO G5"].astype(str).str.strip().str.upper() != "MATERIA PRIMA"].copy()
    df = df[df["JERARQUIA2"].astype(str).str.strip().str.upper() != "MATERIA PRIMA"].copy()
    if df.empty:
        return build_maintenance_pdf("Grupo 5 disponible solo para Sales o sin datos para los filtros seleccionados.")

    if "Stock Total" not in df.columns:
        df["Stock Total"] = df.get("Stock", 0).fillna(0)
    if "Stock Disponible" not in df.columns:
        df["Stock Disponible"] = df["Stock Total"].fillna(0)

    sum_cols = [
        "Stock Total", "Stock Disponible",
        "PRD_Dia", "PRD_Mes", "PRD_Año",
        "NAC_Dia", "NAC_Mes", "NAC_Año",
        "EXP_Dia", "EXP_Mes", "EXP_Año",
    ]
    for c in sum_cols:
        if c not in df.columns:
            df[c] = 0

    ord_c5_col = _pick_order_col(df, ["ORDEN_CENTRO G5", "ORDEN_CENTRO_G5", "ORDEN_CENTRO"])
    ord_t5_col = _pick_order_col(df, ["ORDEN_TIPO G5", "ORDEN_TIPO_G5", "ORDEN_TIPO", "ORDEN_GRUPO_MATERIALES G5"])
    ord_j5_col = _pick_order_col(df, ["ORDEN_JERARQUIA2"])

    df["__ord_c5"] = pd.to_numeric(df[ord_c5_col], errors="coerce") if ord_c5_col else pd.Series(np.nan, index=df.index)
    df["__ord_t5"] = pd.to_numeric(df[ord_t5_col], errors="coerce") if ord_t5_col else pd.Series(np.nan, index=df.index)
    df["__ord_j5"] = pd.to_numeric(df[ord_j5_col], errors="coerce") if ord_j5_col else pd.Series(np.nan, index=df.index)

    agg_dict = {c: "sum" for c in sum_cols}
    agg_dict.update({"__ord_c5": "min", "__ord_t5": "min", "__ord_j5": "min"})
    g = (
        df.groupby(["CENTRO", "TIPO G5", "JERARQUIA2", "UND"], dropna=False)
          .agg(agg_dict)
          .reset_index()
    )

    num_vals = g[sum_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    g = g.loc[num_vals.abs().sum(axis=1) > 0].reset_index(drop=True)
    if g.empty:
        return build_maintenance_pdf("No hay datos de Sales para construir el Grupo 5.")

    g["__ord_c5_fill"] = g["__ord_c5"].fillna(float("inf"))
    g["__ord_t5_fill"] = g["__ord_t5"].fillna(float("inf"))
    g["__ord_j5_fill"] = g["__ord_j5"].fillna(float("inf"))
    g = g.sort_values(
        by=["__ord_c5_fill", "__ord_t5_fill", "__ord_j5_fill", "CENTRO", "TIPO G5", "JERARQUIA2"],
        ascending=[True, True, True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_margins(6, 10, 6)
    pdf.set_auto_page_break(True, margin=12)
    first_page = True

    def draw_first_page_header():
        usable_w = pdf.w - pdf.l_margin - pdf.r_margin
        pdf.set_draw_color(180, 180, 180)
        pdf.set_x(pdf.l_margin)
        pdf.set_font(FONT, "B", 11.5)
        pdf.cell(usable_w, 6.5, title_with_date(meta), new_y="NEXT", align="C")
        pdf.set_x(pdf.l_margin)
        pdf.set_font(FONT, "", 8.8)
        pdf.cell(
            usable_w, 4.8,
            f"Sociedad: {meta['sociedad']} Sector: Sales Grupo: 5",
            new_y="NEXT", align="L",
        )
        pdf.set_x(pdf.l_margin)
        pdf.cell(
            usable_w, 4.8,
            f"Generado: {_now_lima_str()}",
            new_y="NEXT", align="L",
        )
        y = pdf.get_y()
        pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
        pdf.ln(3)

    def new_page():
        nonlocal first_page
        pdf.add_page()
        if first_page:
            draw_first_page_header()
            first_page = False

    new_page()

    usable_w = pdf.w - pdf.l_margin - pdf.r_margin
    w_name, w_und, w_stock_tot, w_stock_disp, w_sub = 43, 12, 14, 14, 12
    total_w = w_name + w_und + w_stock_tot + w_stock_disp + (3 + 3 + 3) * w_sub
    scale = usable_w / total_w
    w_name, w_und, w_stock_tot, w_stock_disp, w_sub = [
        round(x * scale, 2) for x in (w_name, w_und, w_stock_tot, w_stock_disp, w_sub)
    ]

    H, H1, H2 = 4.9, 5.4, 4.2
    FONT_HDR = 6.2
    FONT_SUB = 5.8
    FONT_TXT = 6.0
    FONT_NUM = 6.1
    bottom = pdf.h - pdf.b_margin

    def ensure_space(need):
        if pdf.get_y() + need > bottom:
            new_page()
            return True
        return False

    def box(x, y, w, h, txt="", align="C", fs=FONT_HDR):
        pdf.rect(x, y, w, h)
        if txt:
            pdf.set_xy(x, y + 0.2)
            pdf.set_font(FONT, "B", fs)
            pdf.cell(w, h, txt, border=0, align=align)

    def table_header():
        ensure_space(H1 + H2 + 0.4)
        y0 = pdf.get_y()
        x = pdf.l_margin
        box(x, y0, w_name, H1 + H2, "", align="L"); x += w_name
        box(x, y0, w_und, H1 + H2, "Und"); x += w_und

        box(x, y0, w_stock_tot + w_stock_disp, H1, "Stock")
        box(x, y0 + H1, w_stock_tot, H2, "Total", fs=FONT_SUB); x += w_stock_tot
        box(x, y0 + H1, w_stock_disp, H2, "Disponible", fs=FONT_SUB); x += w_stock_disp

        def group(title):
            nonlocal x
            width = 3 * w_sub
            box(x, y0, width, H1, title)
            xx = x
            for lab in ("Dia", "Mes", "Año"):
                box(xx, y0 + H1, w_sub, H2, lab, fs=FONT_SUB)
                xx += w_sub
            x += width

        group("Producción")
        group("Ventas Nacionales")
        group("Exportaciones")
        pdf.set_y(y0 + H1 + H2)

    def section_band(centro, add_gap=False):
        if add_gap:
            pdf.ln(3)
        ensure_space(H + H1 + H2 + 1.0)
        pdf.set_fill_color(204, 232, 204)
        pdf.set_font(FONT, "B", 9)
        pdf.cell(0, H + 0.8, str(centro or "").upper(), border=1, new_y="NEXT", fill=True)
        table_header()

    def print_row(label, und, nums, bold=False, fill=False):
        widths = [w_name, w_und, w_stock_tot, w_stock_disp] + [w_sub] * 9
        vals = [label, und] + nums
        pdf.set_fill_color(235, 235, 235) if fill else pdf.set_fill_color(255, 255, 255)
        for i, (w, v) in enumerate(zip(widths, vals)):
            if i == 0:
                pdf.set_font(FONT, "B" if bold else "", FONT_TXT)
                align = "L"
            elif i == 1:
                pdf.set_font(FONT, "B" if bold else "", FONT_TXT)
                align = "C"
            else:
                pdf.set_font(FONT, "B" if bold else "", FONT_NUM)
                align = "C"
            pdf.cell(w, H, v, border=1, align=align, fill=fill)
        pdf.ln(0)

    first_center = True
    for centro, sub_c in g.groupby("CENTRO", dropna=False, sort=False):
        section_band(centro, add_gap=not first_center)
        first_center = False

        for tipo, sub_t in sub_c.groupby("TIPO G5", dropna=False, sort=False):
            block_rows = len(sub_t) + 1
            block_height = block_rows * H
            if pdf.get_y() + block_height > bottom:
                new_page()
                section_band(centro, add_gap=False)

            tot = sub_t[sum_cols].sum(numeric_only=True)
            und_tipo = _t(sub_t["UND"].iloc[0] if len(sub_t) else "")
            total_vals = [
                _n(tot["Stock Total"]), _n(tot["Stock Disponible"]),
                _n(tot["PRD_Dia"]), _n(tot["PRD_Mes"]), _n(tot["PRD_Año"]),
                _n(tot["NAC_Dia"]), _n(tot["NAC_Mes"]), _n(tot["NAC_Año"]),
                _n(tot["EXP_Dia"]), _n(tot["EXP_Mes"]), _n(tot["EXP_Año"]),
            ]
            print_row(_t(tipo).upper()[:48], und_tipo, total_vals, bold=True, fill=False)

            for _, r in sub_t.iterrows():
                vals = [
                    _n(r["Stock Total"]), _n(r["Stock Disponible"]),
                    _n(r["PRD_Dia"]), _n(r["PRD_Mes"]), _n(r["PRD_Año"]),
                    _n(r["NAC_Dia"]), _n(r["NAC_Mes"]), _n(r["NAC_Año"]),
                    _n(r["EXP_Dia"]), _n(r["EXP_Mes"]), _n(r["EXP_Año"]),
                ]
                print_row(_t(r["JERARQUIA2"]).upper()[:48], _t(r["UND"]), vals, bold=False, fill=False)

    return _pdf_bytes(pdf)





# ------------------------------------------------------------------------------

# Genérico (fallback)

# ------------------------------------------------------------------------------

def build_pdf_generic(meta, df: pd.DataFrame) -> bytes:

    pdf = FPDF(orientation="P", unit="mm", format="A4")

    pdf.add_page()

    usable_w = pdf.w - pdf.l_margin - pdf.r_margin

    pdf.set_x(pdf.l_margin); pdf.set_font(FONT, "B", 16)

    pdf.cell(usable_w, 10, title_with_date(meta), new_y="NEXT", align="C")

    pdf.set_x(pdf.l_margin); pdf.set_font(FONT, size=11)

    pdf.cell(usable_w, 7, f"Sociedad: {meta['sociedad']} | Fecha: {_fmt_ddmmaaaa(meta['fecha'])} | Sector: {meta['sector']} | Grupo: {meta['grupo']}", new_y="NEXT", align="L")

    pdf.set_x(pdf.l_margin)

    pdf.cell(usable_w, 7, f"Generado: {_now_lima_str()}", new_y="NEXT", align="L")

    pdf.ln(1)

    pdf.set_font(FONT, "B", 11)

    pdf.cell(usable_w, 7, "Resumen", new_y="NEXT")

    pdf.set_font(FONT, size=10)

    prd = _n(df.get("PRD_Dia", pd.Series([0])).sum())

    ytd = _n(df.get("PRD_Año", pd.Series([0])).sum())

    nac = _n(df.get("NAC_Dia", pd.Series([0])).sum())

    exp = _n(df.get("EXP_Dia", pd.Series([0])).sum())

    pdf.cell(usable_w, 6, f"Filas: {len(df)} | Prod Día: {prd} | YTD: {ytd} | NAC Día: {nac} | EXP Día: {exp}", new_y="NEXT")

    pdf.ln(1)

    cols = [c for c in df.columns][:12]

    col_w = usable_w / max(1, len(cols))

    pdf.set_font(FONT, "B", 10)

    for c in cols: pdf.cell(col_w, 6, str(c)[:22], border=1)

    pdf.ln()

    pdf.set_font(FONT, size=9)

    for _, r in df.head(200).iterrows():

        for c in cols: pdf.cell(col_w, 5, _t(r.get(c, ""))[:22], border=1)

        pdf.ln()

    return _pdf_bytes(pdf)





# ------------------------------------------------------------------------------

# Dispatcher

# ------------------------------------------------------------------------------

def build_pdf(meta, df: pd.DataFrame) -> bytes:

    graw = str(meta.get("grupo", "1"))

    g = graw.split()[-1]  # soporta "Grupo 4"

    if g == "1": return build_pdf_group1(meta, df)

    if g == "2": return build_pdf_group2(meta, df)

    if g == "3": return build_pdf_group3(meta, df)

    if g == "4": return build_pdf_group4(meta, df)

    if g == "5": return build_pdf_group5(meta, df)

    return build_pdf_generic(meta, df)





# ------------------------------------------------------------------------------

# Stream helper

# ------------------------------------------------------------------------------

def stream_pdf(filename: str, content: bytes, inline: bool=True) -> Response:

    disp = "inline" if inline else "attachment"

    return Response(content, mimetype="application/pdf",

                    headers={"Content-Disposition": f'{disp}; filename="{filename}"'})





# ------------------------------------------------------------------------------

# Mantenimiento: HTML simple (fallback sin template)

# ------------------------------------------------------------------------------

def maintenance_response(msg: str = "Estamos en mantenimiento para mejorar el servicio, por favor intente nuevamente más tarde.") -> Response:

    html = f"""<!doctype html>

<html lang="es">

<head>

<meta charset="utf-8">

<title>Mantenimiento</title>

<meta name="viewport" content="width=device-width, initial-scale=1">

<style>

  :root {{ color-scheme: light dark; }}

  body {{

    margin:0; font-family: system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Helvetica,Arial,sans-serif;

    background: #f2f5f9; display:flex; min-height:100vh; align-items:center; justify-content:center;

  }}

  .card {{

    background:#fff; border:1px solid #e6ebf1; border-radius:14px; padding:22px 20px; max-width:640px; width:92%;

    box-shadow: 0 10px 25px rgba(0,0,0,.06);

  }}

  h1 {{ margin:0 0 8px; font-size:20px; }}

  p  {{ margin:0 0 14px; line-height:1.45 }}

  .muted {{ color:#5f6b7a; font-size:14px }}

  .row {{ display:flex; gap:12px; align-items:center }}

  .badge {{

    background:#ffefc2; color:#8a5a00; border:1px solid #ffd66d; padding:4px 10px; border-radius:999px; font-weight:600; font-size:12px;

  }}

  .btn {{

    display:inline-block; padding:10px 14px; border-radius:10px; border:1px solid #cfd7df; background:#fff; color:#223; text-decoration:none;

  }}

  .btn:hover {{ background:#f7f9fc }}

</style>

</head>

<body>

  <div class="card" role="alert" aria-live="polite">

    <div class="row" style="margin-bottom:10px">

      <span class="badge">Aviso</span>

      <strong>Servicio temporalmente no disponible</strong>

    </div>

    <p>{msg}</p>

    <p class="muted">Si el problema persiste pasados unos minutos, contacte a Soporte o vuelva a intentar más tarde.</p>

    <div style="margin-top:10px">

      <a href="{url_for('reports_new')}" class="btn">Volver</a>

    </div>

  </div>

</body>

</html>"""

    return Response(html, mimetype="text/html")





# ------------------------------------------------------------------------------

# Routes

# ------------------------------------------------------------------------------

@app.get("/")

def root():

    return redirect(url_for("reports_new"))



@app.route("/login", methods=["GET","POST"])

def login():

    if request.method == "POST":

        if request.form.get("username")=="admin" and request.form.get("password")=="admin":

            login_user(DemoUser())

            return redirect(url_for("reports_new"))

        return render_template("login.html", error="Credenciales inválidas")

    return render_template("login.html")



@app.get("/logout")

def logout():

    logout_user()

    return redirect(url_for("login"))



@app.get("/reports/new")

@login_required

def reports_new():

    max_date = (date.today() - timedelta(days=1)).isoformat()

    return render_template("reports_form.html", max_date=max_date, default_soc="PQ00")





# >>> Ruta PDF con manejo de mantenimiento y fallbacks a PDF

@app.get("/pdf/inline")

@login_required

def pdf_inline():

    sociedad = request.args.get("sociedad", "PQ00")

    fecha    = request.args.get("fecha")

    sector   = request.args.get("sector", "TODOS")

    grupo    = request.args.get("grupo", "1")

    if not fecha:

        abort(400, "Falta fecha")



    _stop_total = _tick("TOTAL /pdf/inline")



    try:

        _tck = _tick("DB call_sp")

        raw = call_sp(sociedad, fecha)

        _tck()

    except MaintenanceError:

        pdf_bytes = build_maintenance_pdf(

            "Estamos realizando tareas de mantenimiento o la consulta está temporalmente bloqueada.\n"

            "Por favor, inténtelo nuevamente en unos minutos."

        )

        g_norm = str(grupo).split()[-1]

        return stream_pdf(f"Reporte_{sociedad}_{fecha}_G{g_norm}_MANTENIMIENTO.pdf", pdf_bytes, inline=True)

    except Exception:

        app.logger.exception("Error al ejecutar SP")

        pdf_bytes = build_maintenance_pdf("Ocurrió un problema al ejecutar la consulta.")

        return stream_pdf("Mantenimiento.pdf", pdf_bytes, inline=True)



    if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):

        pdf_bytes = build_maintenance_pdf(

            "No hay datos disponibles en este momento para los filtros seleccionados.\n"

            "Es posible que estemos procesando una carga. Inténtelo nuevamente más tarde."

        )

        g_norm = str(grupo).split()[-1]

        return stream_pdf(f"Reporte_{sociedad}_{fecha}_G{g_norm}_SIN_DATOS.pdf", pdf_bytes, inline=True)



    _tck = _tick("pandas filter_sector")

    df = filter_sector(raw, sector)

    _tck()



    if df is None or df.empty:

        pdf_bytes = build_maintenance_pdf(

            "No hay datos disponibles para el sector seleccionado en este momento.\n"

            "Inténtelo nuevamente más tarde."

        )

        g_norm = str(grupo).split()[-1]

        return stream_pdf(f"Reporte_{sociedad}_{fecha}_G{g_norm}_SIN_DATOS.pdf", pdf_bytes, inline=True)



    meta = {"sociedad": sociedad, "fecha": fecha, "sector": sector, "grupo": grupo}

    try:

        _tck = _tick("build_pdf")

        pdf_bytes = build_pdf(meta, df)

        _tck()

    except Exception:

        app.logger.exception("Error al construir PDF")

        pdf_bytes = build_maintenance_pdf("Ocurrió un problema generando el PDF. Inténtelo nuevamente en unos minutos.")



    _stop_total()

    g_norm = str(grupo).split()[-1]

    return stream_pdf(f"Reporte_{sociedad}_{fecha}_G{g_norm}.pdf", pdf_bytes, inline=True)





@app.get("/pdf/download")

@login_required

def pdf_download():

    q = request.query_string.decode()

    return redirect(url_for("pdf_inline") + (f"?{q}" if q else ""))





# ------------------------------------------------------------------------------

# CSV download (con manejo básico)

# ------------------------------------------------------------------------------

@app.get("/csv/download")

@login_required

def csv_download():

    sociedad = request.args.get("sociedad", "PQ00")

    fecha    = request.args.get("fecha")

    sector   = request.args.get("sector", "TODOS")

    grupo    = (request.args.get("grupo", "1") or "1").split()[-1]

    sep      = request.args.get("sep", ",")

    dec      = request.args.get("decimal", ".")

    bom      = request.args.get("bom", "1")

    cols     = request.args.get("cols")



    if not fecha:

        abort(400, "Falta fecha")



    try:

        df_base = call_sp(sociedad, fecha)

    except MaintenanceError:

        return Response("Servicio en mantenimiento. Intente mas tarde.",

                        mimetype="text/plain; charset=utf-8", status=503)

    except DatabaseConfigError:

        app.logger.exception("Error de conexion/autenticacion SQL para CSV")

        return Response("No fue posible conectar con la base de datos. Revise DB_CONN_STRING, usuario, contrasena y atributos ODBC.",

                        mimetype="text/plain; charset=utf-8", status=503)

    except Exception:

        app.logger.exception("Error CSV SP")

        return Response("Error inesperado al consultar.", mimetype="text/plain; charset=utf-8", status=503)



    try:

        df = filter_sector(df_base, sector).copy()



        def ensure_numeric_columns(columns):

            for col in columns:

                if col not in df.columns:

                    df[col] = 0



        def series_or_default(col_name: str, default=0):

            if col_name in df.columns:

                return df[col_name]

            return pd.Series(default, index=df.index)



        ensure_numeric_columns([

            "Stock", "PPTO_MES",

            "PRD_Dia", "PRD_Mes", "PRD_A?o",

            "NAC_Dia", "NAC_Mes", "NAC_A?o",

            "EXP_Dia", "EXP_Mes", "EXP_A?o",

            "PPTO", "TOT_Dia", "TOT_Mes", "TOT_A?o",

        ])



        if "SECTOR" not in df.columns:

            df["SECTOR"] = "SIN SECTOR"

        if "UND" not in df.columns:

            df["UND"] = df.get("UNIDAD DE MEDIDA BASE", "")



        if grupo == "1":

            _ensure_jer_col(df, "JERARQUIA2 G1")

        elif grupo == "2":

            _ensure_jer_col(df, "JERARQUIA2 G2")

            _ensure_col(df, "CENTRO", ["CENTRO", "CENTROSAP", "PLANTA", "CODCENTRO"], default="")

        elif grupo == "3":

            _ensure_jer_col(df, "JERARQUIA2 G3")

            _ensure_col(df, "CENTRO", ["CENTRO", "CENTROSAP", "PLANTA", "CODCENTRO"], default="")

        elif grupo == "4":

            _ensure_jer_col(df, "JERARQUIA2 G4")

            _ensure_col(df, "CENTRO", ["CENTRO", "CENTRO_G4", "CENTRO G4", "CENTROSAP", "PLANTA", "CODCENTRO"], default="")

            _ensure_col(df, "G4", ["G4", "CENTRO_G4", "CENTRO G4", "CENTRO"], default="")

            if "Stock Total" not in df.columns:

                df["Stock Total"] = pd.to_numeric(series_or_default("Stock"), errors="coerce").fillna(0)

            if "Stock Disponible" not in df.columns:

                df["Stock Disponible"] = df["Stock Total"].fillna(0)



            import numpy as np

            ord_j4_col = _pick_order_col(df, ["ORDEN_JERARQUIA2 G4", "ORDEN_JERARQUIA2_G4", "ORDEN_JERARQUIA2"])

            ord_c4_col = _pick_order_col(df, ["ORDEN_CENTRO G4", "ORDEN_CENTRO_G4", "ORDEN_CENTRO"])



            ord_j = pd.to_numeric(df[ord_j4_col], errors="coerce") if ord_j4_col else pd.Series(np.nan, index=df.index)

            ord_c = pd.to_numeric(df[ord_c4_col], errors="coerce") if ord_c4_col else pd.Series(np.nan, index=df.index)



            sort_by = ["__ord_j2", "__ord_cg4"] + [c for c in ["JERARQUIA2 G4", "G4"] if c in df.columns]

            df = (

                df.assign(__ord_j2=ord_j.fillna(np.inf),

                          __ord_cg4=ord_c.fillna(np.inf))

                  .sort_values(by=sort_by,

                               ascending=[True] * len(sort_by),

                               kind="mergesort")

                  .drop(columns=["__ord_j2", "__ord_cg4"])

            )

        elif grupo == "5":

            _ensure_col(df, "CENTRO", ["CENTRO", "CENTROSAP", "PLANTA", "CODCENTRO"], default="")

            _ensure_jer_col(df, "JERARQUIA2")

            _ensure_col(df, "TIPO G5", ["TIPO G5", "TIPO_G5", "TIPOG5", "TIPO"], default="SIN TIPO")

            df["TIPO G5"] = df["TIPO G5"].fillna("SIN TIPO")

            df.loc[df["TIPO G5"].astype(str).str.strip() == "", "TIPO G5"] = "SIN TIPO"

            df["JERARQUIA2"] = df["JERARQUIA2"].fillna("SIN JERARQUIA")

            df.loc[df["JERARQUIA2"].astype(str).str.strip() == "", "JERARQUIA2"] = "SIN JERARQUIA"

            df = df[df["SECTOR"].astype(str).str.upper() == "SALES"].copy()
            df = df[df["TIPO G5"].astype(str).str.strip().str.upper() != "MATERIA PRIMA"].copy()
            df = df[df["JERARQUIA2"].astype(str).str.strip().str.upper() != "MATERIA PRIMA"].copy()

            if "Stock Total" not in df.columns:

                df["Stock Total"] = pd.to_numeric(series_or_default("Stock"), errors="coerce").fillna(0)

            if "Stock Disponible" not in df.columns:

                df["Stock Disponible"] = df["Stock Total"].fillna(0)



        def detect_material_cols(columns):

            cols_upper = {c.upper(): c for c in columns}

            if "COD_MATERIAL" in cols_upper and "MATERIAL" in cols_upper:

                return cols_upper["COD_MATERIAL"], cols_upper["MATERIAL"]

            code_aliases = ["COD_MAT", "COD_MATERIAL", "CODIGO", "CODIGO MATERIAL",

                            "CODIGO_MATERIAL", "COD_MATNR", "MATNR"]

            name_aliases = ["DESC_MAT", "DESC_MATERIAL", "DESCRIPCION", "DESCRIPCION MATERIAL",

                            "TEXTO BREVE DE MATERIAL", "DESCRIPCION_MATERIAL",

                            "DESCRIPCION_MAT", "MAKTX", "NOMBRE", "NOMBRE MATERIAL", "MATERIAL"]

            code_col = next((cols_upper[a] for a in code_aliases if a in cols_upper), None)

            name_col = next((cols_upper[a] for a in name_aliases if a in cols_upper), None)

            return code_col, name_col



        code_col, name_col = detect_material_cols(df.columns)

        if code_col and "COD_MAT" not in df.columns:

            df = df.rename(columns={code_col: "COD_MAT"})

            code_col = "COD_MAT"

        if name_col and "DESC_MAT" not in df.columns:

            df = df.rename(columns={name_col: "DESC_MAT"})

            name_col = "DESC_MAT"



        NUMS = ["Stock", "PPTO_MES", "PRD_Dia", "PRD_Mes", "PRD_A?o",

                "NAC_Dia", "NAC_Mes", "NAC_A?o",

                "EXP_Dia", "EXP_Mes", "EXP_A?o",

                "PPTO", "TOT_Dia", "TOT_Mes", "TOT_A?o"]



        stock_total_col = "Stock Total" if "Stock Total" in df.columns else "Stock"

        stock_disp_col  = "Stock Disponible" if "Stock Disponible" in df.columns else None



        cols_g4 = ["JERARQUIA2 G4", "CENTRO", "G4", "UND",

                   (_pick_order_col(df, ["ORDEN_JERARQUIA2 G4", "ORDEN_JERARQUIA2_G4", "ORDEN_JERARQUIA2"]) or "ORDEN_JERARQUIA2"),

                   (_pick_order_col(df, ["ORDEN_CENTRO G4", "ORDEN_CENTRO_G4", "ORDEN_CENTRO"]) or "ORDEN_CENTRO"),

                   stock_total_col] + ([stock_disp_col] if stock_disp_col else []) + [

                   "PRD_Dia", "PRD_Mes", "PRD_A?o",

                   "NAC_Dia", "NAC_Mes", "NAC_A?o",

                   "EXP_Dia", "EXP_Mes", "EXP_A?o"]



        cols_g5 = ["CENTRO", "SECTOR", "TIPO G5", "JERARQUIA2", "UND",

                   (_pick_order_col(df, ["ORDEN_CENTRO G5", "ORDEN_CENTRO_G5", "ORDEN_CENTRO"]) or "ORDEN_CENTRO"),

                   (_pick_order_col(df, ["ORDEN_TIPO G5", "ORDEN_TIPO_G5", "ORDEN_TIPO", "ORDEN_GRUPO_MATERIALES G5"]) or "ORDEN_TIPO"),

                   (_pick_order_col(df, ["ORDEN_JERARQUIA2"]) or "ORDEN_JERARQUIA2"),

                   stock_total_col] + ([stock_disp_col] if stock_disp_col else []) + [

                   "PRD_Dia", "PRD_Mes", "PRD_A?o",

                   "NAC_Dia", "NAC_Mes", "NAC_A?o",

                   "EXP_Dia", "EXP_Mes", "EXP_A?o"]



        COLUMNS_BY_GROUP = {

            "1": ["SECTOR", "JERARQUIA2 G1", "UND"] + NUMS,

            "2": ["CENTRO", "SECTOR", "JERARQUIA2 G2", "UND"] + NUMS,

            "3": ["CENTRO", "SECTOR", "JERARQUIA2 G3", "COD_MAT", "DESC_MAT", "UND", stock_total_col, "ORDEN_CENTRO G3", "ORDEN_JERARQUIA2 G3"] + NUMS,

            "4": cols_g4,

            "5": cols_g5,

        }



        if cols:

            wanted = [c.strip() for c in cols.split(",") if c.strip()]

        else:

            wanted = COLUMNS_BY_GROUP.get(grupo, COLUMNS_BY_GROUP["1"])



        prefix = []

        if code_col and code_col not in wanted:

            prefix.append(code_col)

        if name_col and name_col not in wanted:

            prefix.append(name_col)



        ordered, seen = [], set()

        for c in prefix + wanted:

            if c in df.columns and c not in seen:

                ordered.append(c)

                seen.add(c)



        existing = ordered if ordered else list(df.columns)

        df = df[existing]

    except Exception:

        app.logger.exception("Error preparando CSV")

        return Response("Error inesperado al generar el CSV.", mimetype="text/plain; charset=utf-8", status=503)



    csv_text = df.to_csv(index=False, sep=sep, decimal=dec)

    if bom == "1":

        csv_text = "﻿" + csv_text



    sec_safe = (sector or "TODOS").replace(" ", "_")[:40]

    filename = f"Consulta_G{grupo}_{sociedad}_{fecha}_{sec_safe}.csv"



    return Response(csv_text, mimetype="text/csv",

                    headers={"Content-Disposition": f"attachment; filename={filename}"})



# ------------------------------------------------------------------------------

# Handler global

# ------------------------------------------------------------------------------

@app.errorhandler(Exception)

def handle_any_error(e):

    app.logger.exception("Unhandled error")

    if request.path.startswith("/pdf/"):

        pdf_bytes = build_maintenance_pdf("Servicio temporalmente no disponible. Intente nuevamente en unos minutos.")

        return stream_pdf("Mantenimiento.pdf", pdf_bytes, inline=True), 503

    return maintenance_response(), 503





# ------------------------------------------------------------------------------

# Main

# ------------------------------------------------------------------------------

if __name__ == "__main__":

    app.run(host="0.0.0.0", port=8000, debug=True)