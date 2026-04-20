"""
generar_inventario.py
─────────────────────
Lee el Excel de inventario desde Google Sheets y genera index_inventario.html.

Variables de entorno requeridas:
  SHEET_URL_INV   → URL del Google Sheets del inventario
                    (puede ser el link normal de edición o publicación)

Uso local:
  SHEET_URL_INV="https://docs.google.com/spreadsheets/d/..." python generar_inventario.py

GitHub Actions: ver workflow inventario.yml
"""

import os, sys, json, requests
import pandas as pd
import numpy as np
from datetime import datetime, date
from io import BytesIO

# ── Configuración ─────────────────────────────────────────────────────────────
SHEET_URL_INV = os.environ.get("SHEET_URL_INV", "")

# ── Helper: JSON serializable con numpy ───────────────────────────────────────
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer,)):  return int(obj)
        if isinstance(obj, (np.floating,)): return round(float(obj), 2)
        return super().default(obj)

def jd(x):
    return json.dumps(x, cls=NpEncoder, ensure_ascii=False)

# ── Descarga ──────────────────────────────────────────────────────────────────
def download_excel(url, label=""):
    if not url:
        return None
    if "docs.google.com/spreadsheets" in url:
        sheet_id = url.split("/d/")[1].split("/")[0]
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    print(f"Descargando Excel {label}...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return BytesIO(r.content)

# ── Limpieza y lectura ────────────────────────────────────────────────────────
def leer_inventario(excel_bytes):
    """
    Lee el Excel de inventario. Detecta automáticamente si tiene encabezado en
    la primera fila o en la fila 0 (ambos formatos comunes de exportación).
    Retorna un DataFrame limpio.
    """
    excel_bytes.seek(0)
    df = pd.read_excel(excel_bytes)
    df.columns = [str(c).replace('\xa0', ' ').strip() for c in df.columns]

    # Limpiar strings
    str_cols = ['Ubic Inv', 'Estado', 'Origen', 'Equipo', 'Dest', 'Entr.',
                'L.A.3', 'Ubic', 'Bo', 'Usuario Leida']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('\xa0', ' ').str.strip()
            df[col] = df[col].replace({'nan': None, 'None': None, '': None})

    # Asegurar numéricos
    for col in ['PzsInv.', 'Kilo Inv.', 'Dias']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    print(f"  → {len(df)} filas leídas")
    return df

# ── Extracción de datos para gráficos ─────────────────────────────────────────
def extraer_datos(df):
    """Calcula todas las series y KPIs necesarios para el dashboard."""

    total   = len(df)
    kgs_tot = round(float(df['Kilo Inv.'].sum()), 1) if 'Kilo Inv.' in df.columns else 0
    pzs_tot = int(df['PzsInv.'].sum()) if 'PzsInv.' in df.columns else 0
    ok_cnt  = int((df['Estado'] == 'OK').sum()) if 'Estado' in df.columns else 0
    ok_pct  = round(ok_cnt / total * 100, 1) if total else 0
    avg_dias= round(float(df['Dias'].mean()), 1) if 'Dias' in df.columns else 0
    max_dias= int(df['Dias'].max()) if 'Dias' in df.columns else 0
    aban    = int((df['Estado'] == 'ABANDONO').sum()) if 'Estado' in df.columns else 0
    desv    = int((df['Estado'] == 'DESVIACION').sum()) if 'Estado' in df.columns else 0

    # Series de conteo
    def vc(col, n=None):
        s = df[col].value_counts()
        if n: s = s.head(n)
        return {"labels": list(s.index), "data": [int(v) for v in s.values]}

    estado  = vc('Estado')
    origen  = vc('Origen')
    dest    = vc('Dest', 15)
    equipo  = vc('Equipo')
    entr    = vc('Entr.')
    ubic    = vc('Ubic Inv', 12)

    # Histograma de días
    dias      = df['Dias'].dropna()
    buckets   = [0, 7, 15, 30, 60, 90, 180, 365, 9999]
    dia_labs  = ['0-7d', '8-15d', '16-30d', '31-60d', '61-90d', '91-180d', '181-365d', '+365d']
    dia_cnt   = [int(((dias >= buckets[i]) & (dias < buckets[i+1])).sum()) for i in range(len(buckets)-1)]

    # Kilos por estado y por origen
    top_est_list = df['Estado'].value_counts().index[:6].tolist()
    kilo_est = {e: round(float(df[df['Estado']==e]['Kilo Inv.'].sum()), 1) for e in top_est_list}

    kilo_orig_s = df.groupby('Origen')['Kilo Inv.'].sum().sort_values(ascending=False).head(8)
    kilo_orig   = {"labels": list(kilo_orig_s.index), "data": [round(float(v), 1) for v in kilo_orig_s.values]}

    # Cross: piezas por origen × estado (top 6 × top 5)
    top_orig_list = df['Origen'].value_counts().head(6).index.tolist()
    top_est_cross = df['Estado'].value_counts().head(5).index.tolist()
    cross_mat     = df.groupby(['Origen', 'Estado'])['PzsInv.'].sum().unstack(fill_value=0)
    cross_data    = {}
    for o in top_orig_list:
        if o in cross_mat.index:
            cross_data[o] = {e: int(cross_mat.loc[o, e]) if e in cross_mat.columns else 0 for e in top_est_cross}

    # Días promedio por destino (top 15 más lentos)
    dias_dest_s = df.groupby('Dest')['Dias'].mean().sort_values(ascending=False).head(15)
    dias_dest   = {"labels": list(dias_dest_s.index), "data": [round(float(v), 1) for v in dias_dest_s.values]}

    return {
        "estado":    estado,
        "origen":    origen,
        "dest":      dest,
        "equipo":    equipo,
        "entr":      entr,
        "ubic":      ubic,
        "dias_hist": {"labels": dia_labs, "data": dia_cnt},
        "kilo_est":  {"labels": list(kilo_est.keys()), "data": list(kilo_est.values())},
        "kilo_orig": kilo_orig,
        "cross":     cross_data,
        "top_est":   top_est_cross,
        "dias_dest": dias_dest,
        "kpis": {
            "total":    total,
            "kgs_tot":  kgs_tot,
            "pzs_tot":  pzs_tot,
            "ok_cnt":   ok_cnt,
            "ok_pct":   ok_pct,
            "avg_dias": avg_dias,
            "max_dias": max_dias,
            "aban":     aban,
            "desv":     desv,
        }
    }

# ── Generación del HTML ───────────────────────────────────────────────────────
def generar_html(datos, fecha_str, hora_str):
    with open("template_inventario.html", "r", encoding="utf-8") as f:
        t = f.read()

    k = datos["kpis"]

    # KPI strings para el template
    critico_cnt  = k['desv'] + k['aban']
    critico_pct  = round(critico_cnt / k['total'] * 100, 1) if k['total'] else 0
    kgs_criticos = round(
        (datos['kilo_est'].get('data', [0])[0] if 'data' in datos['kilo_est'] else 0)
        + (datos['kilo_est'].get('data', [0])[1] if len(datos['kilo_est'].get('data',[])) > 1 else 0),
        1
    )
    # Safer: sum DESVIACION + ABANDONO kilos from dict
    kilo_dict = dict(zip(datos['kilo_est']['labels'], datos['kilo_est']['data']))
    kgs_crit  = round((kilo_dict.get('DESVIACION', 0) + kilo_dict.get('ABANDONO', 0)) / 1000, 1)
    kgs_crit_pct = round((kilo_dict.get('DESVIACION', 0) + kilo_dict.get('ABANDONO', 0)) / k['kgs_tot'] * 100, 1) if k['kgs_tot'] else 0

    # Zona con más items
    zona_top  = datos['ubic']['labels'][0] if datos['ubic']['labels'] else '—'
    zona_top_n= datos['ubic']['data'][0] if datos['ubic']['data'] else 0
    zona_pct  = round(zona_top_n / k['total'] * 100, 1) if k['total'] else 0

    # Destino más frecuente
    dest_top  = datos['dest']['labels'][0] if datos['dest']['labels'] else '—'
    dest_top_n= datos['dest']['data'][0] if datos['dest']['data'] else 0
    dest_pct  = round(dest_top_n / k['total'] * 100, 1) if k['total'] else 0

    # Destino más viejo
    dest_old      = datos['dias_dest']['labels'][0] if datos['dias_dest']['labels'] else '—'
    dest_old_dias = datos['dias_dest']['data'][0] if datos['dias_dest']['data'] else 0

    # +180 días
    dias_data    = datos['dias_hist']['data']
    plus180      = (dias_data[6] if len(dias_data) > 6 else 0) + (dias_data[7] if len(dias_data) > 7 else 0)
    plus180_pct  = round(plus180 / k['total'] * 100, 1) if k['total'] else 0

    resumen_items = [
        {"icon":"🔴","label":"Items críticos (Desv. + Aban.)", "val":f"{critico_cnt:,} items".replace(',','.'), "sub":f"{critico_pct}% del total", "c":"#EF4444"},
        {"icon":"⏰","label":"Antigüedad crítica (+180 días)",  "val":f"{plus180:,} items".replace(',','.'), "sub":f"{plus180_pct}% del inventario", "c":"#F59E0B"},
        {"icon":"📦","label":"Kilos no resueltos (Desv.+Aban.)","val":f"{kgs_crit} t",    "sub":f"{kgs_crit_pct}% de los kilos totales", "c":"#F97316"},
        {"icon":"📍","label":"Mayor concentración",             "val":f"{zona_top} · {zona_top_n:,} items".replace(',','.'), "sub":f"{zona_pct}% del inventario", "c":"#0EA5E9"},
        {"icon":"✈️","label":"Destino más frecuente",           "val":f"{dest_top} · {dest_top_n:,} items".replace(',','.'), "sub":f"{dest_pct}% del flujo", "c":"#10B981"},
        {"icon":"⚠️","label":"Destino mayor antigüedad",        "val":f"{dest_old} · {dest_old_dias} días", "sub":"promedio histórico", "c":"#EF4444"},
    ]

    html = t \
        .replace("{{HORA}}",         hora_str) \
        .replace("{{FECHA}}",        fecha_str) \
        .replace("{{TOTAL}}",        f"{k['total']:,}".replace(',','.')) \
        .replace("{{KGS_TOT}}",      f"{k['kgs_tot']/1000:.0f}K") \
        .replace("{{PZS_TOT}}",      f"{k['pzs_tot']:,}".replace(',','.')) \
        .replace("{{OK_PCT}}",       f"{k['ok_pct']}%") \
        .replace("{{OK_CNT}}",       str(k['ok_cnt'])) \
        .replace("{{DESV_CNT}}",     str(k['desv'])) \
        .replace("{{DESV_PCT}}",     f"{round(k['desv']/k['total']*100,1)}%") \
        .replace("{{ABAN_CNT}}",     str(k['aban'])) \
        .replace("{{ABAN_PCT}}",     f"{round(k['aban']/k['total']*100,1)}%") \
        .replace("{{AVG_DIAS}}",     str(k['avg_dias'])) \
        .replace("{{MAX_DIAS}}",     f"{k['max_dias']:,}".replace(',','.')) \
        .replace("{{INV_DATA}}",     jd(datos)) \
        .replace("{{RESUMEN_ITEMS}}",jd(resumen_items))

    return html

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not SHEET_URL_INV:
        print("ERROR: Variable SHEET_URL_INV no configurada.")
        sys.exit(1)

    now       = datetime.now()
    fecha_str = now.strftime("%d/%m/%Y")
    hora_str  = now.strftime("%H:%M")

    raw = download_excel(SHEET_URL_INV, "INVENTARIO")
    df  = leer_inventario(raw)
    datos = extraer_datos(df)

    k = datos["kpis"]
    print(f"\n📦 Inventario procesado:")
    print(f"   Total items  : {k['total']:,}")
    print(f"   Kilos totales: {k['kgs_tot']:,} kg")
    print(f"   Estado OK    : {k['ok_pct']}%")
    print(f"   Desviación   : {k['desv']} items")
    print(f"   Abandono     : {k['aban']} items")
    print(f"   Promedio días: {k['avg_dias']}")

    html = generar_html(datos, fecha_str, hora_str)

    with open("index_inventario.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("\n✅ index_inventario.html generado correctamente.")
