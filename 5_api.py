import json

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import PlainTextResponse

import config

app = FastAPI(title="Prevencion de quiebres de stock", version="1.0")


def autorizar(x_api_key: str = Header(default="")):
    if x_api_key != config.API_KEY:
        raise HTTPException(401, "cabecera X-API-Key ausente o incorrecta")


def consultar(sql):
    try:
        return duckdb.sql(sql).df().to_dict("records")
    except duckdb.IOException:
        raise HTTPException(503, "las tablas se estan recalculando, reintenta en unos segundos")


def tabla(nombre):
    ruta = config.ORO / nombre
    if not ruta.exists():
        raise HTTPException(503, f"{nombre} todavia no existe, espera al primer ciclo")
    return f"'{ruta.as_posix()}/**/*.parquet'"


@app.get("/health")
def health():
    return {"estado": "ok"}


@app.get("/kpis", dependencies=[Depends(autorizar)])
def kpis():
    filas = consultar(f"SELECT * FROM {tabla('kpis')}")
    if not filas:
        raise HTTPException(503, "aun no hay indicadores")
    return filas[0]


@app.get("/alertas", dependencies=[Depends(autorizar)])
def alertas(nivel: str = None, tienda_id: str = None,
            horizonte: int = Query(7, ge=1, le=14), limite: int = Query(50, ge=1, le=500)):
    filtros = [f"horizonte = {horizonte}"]
    filtros.append(f"nivel = '{nivel}'" if nivel else "nivel <> 'OK'")
    if tienda_id:
        filtros.append(f"tienda_id = '{tienda_id}'")

    return {"alertas": consultar(f"""
        SELECT tienda, producto, sku, nivel, stock, en_camino, demanda_prevista,
               proyectado, stock_seguridad, probabilidad, cantidad_sugerida, valor_en_riesgo
        FROM   {tabla('hecho_riesgo')}
        WHERE  {' AND '.join(filtros)}
        ORDER BY prioridad DESC
        LIMIT  {limite}
    """)}


@app.get("/calidad", dependencies=[Depends(autorizar)])
def calidad():
    return {
        "resumen": consultar(f"SELECT * FROM {tabla('calidad')}")[0],
        "rechazos": consultar(f"SELECT * FROM {tabla('rechazos')} ORDER BY registros DESC"),
    }


@app.get("/modelo", dependencies=[Depends(autorizar)])
def modelo():
    return {
        "clasificador": consultar(f"SELECT * FROM {tabla('metricas_modelo')}")[0],
        "pronostico": consultar(f"SELECT * FROM {tabla('metricas_pronostico')}")[0],
        "importancia": consultar(f"SELECT * FROM {tabla('importancia')} ORDER BY importancia DESC"),
    }

#de donde y cuando vino el dato
@app.get("/linaje", dependencies=[Depends(autorizar)])
def linaje(limite: int = Query(30, ge=1, le=500)):
    ruta = config.BRONCE / "_manifiesto.jsonl"
    if not ruta.exists():
        raise HTTPException(503, "todavia no hay cargas registradas")
    with open(ruta, encoding="utf-8") as f:
        cargas = [json.loads(linea) for linea in f if linea.strip()]
    return {"cargas": cargas[-limite:][::-1]}


@app.get("/alertas.csv", response_class=PlainTextResponse, dependencies=[Depends(autorizar)])
def alertas_csv():
    ruta = config.SALIDA / "alertas.csv"
    if not ruta.exists():
        raise HTTPException(503, "el pipeline aun no ha exportado alertas")
    return ruta.read_text(encoding="utf-8")