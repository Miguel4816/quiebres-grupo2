import json
from datetime import datetime, timezone
import requests
import config


def guardar(datos, nombre):
    config.BRONCE.mkdir(parents=True, exist_ok=True)
    ruta = config.BRONCE / nombre
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=1)
    return ruta


def anotar(origen, fichero, registros):
    config.BRONCE.mkdir(parents=True, exist_ok=True)
    with open(config.BRONCE / "_manifiesto.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "fecha": datetime.now(timezone.utc).isoformat(),
            "origen": origen,
            "fichero": str(fichero.name),
            "registros": registros,
        }) + "\n")


def bajar(url, params):
    for intento in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            config.log(f"fallo al bajar {url} ({intento + 1}/3): {e}")
    return None


def catalogo():
    ruta = config.BRONCE / "catalogo.json"
    datos = bajar(config.URL_CATALOGO, {"limit": 0, "select": "id,title,sku,category,brand,price,stock,minimumOrderQuantity"})
    if datos is None:
        if not ruta.exists():
            raise RuntimeError("no hay catalogo ni en la API ni en disco")
        config.log("catalogo servido desde la copia local")
        return json.load(open(ruta, encoding="utf-8"))["products"]

    productos = datos["products"]
    guardar({"products": productos}, "catalogo.json")
    anotar(config.URL_CATALOGO, ruta, len(productos))
    config.log(f"catalogo: {len(productos)} productos")
    return productos


def estacionalidad():
    ruta = config.BRONCE / "ine.json"
    datos = bajar(config.URL_INE, {"nult": 48})
    if datos is None:
        if not ruta.exists():
            raise RuntimeError("no hay serie del INE ni en la API ni en disco")
        config.log("serie del INE servida desde la copia local")
        datos = json.load(open(ruta, encoding="utf-8"))
    else:
        guardar(datos, "ine.json")
        anotar(config.URL_INE, ruta, len(datos.get("Data", [])))

    por_mes = {}
    for obs in datos.get("Data", []):
        mes, valor = obs.get("FK_Periodo"), obs.get("Valor")
        if valor is not None and isinstance(mes, int) and 1 <= mes <= 12:
            por_mes.setdefault(mes, []).append(float(valor))

    medias = {m: sum(v) / len(v) for m, v in por_mes.items()}
    general = sum(medias.values()) / len(medias)
    factores = {m: round(medias.get(m, general) / general, 4) for m in range(1, 13)}

    guardar(factores, "factores_mes.json")
    config.log(f"estacionalidad INE: diciembre {factores[12]}, febrero {factores[2]}")
    return factores


if __name__ == "__main__":
    catalogo()
    estacionalidad()
    guardar({"tiendas": config.TIENDAS}, "tiendas.json")
    config.log("ingesta terminada")
