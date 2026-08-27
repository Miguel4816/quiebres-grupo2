import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import config

#recordatorio de que fije este numero en 20262 para que a todos nos salga el mismo numero de quiebres
rng = np.random.default_rng(config.SEMILLA)

def cargar():
    productos = json.load(open(config.BRONCE / "catalogo.json", encoding="utf-8"))["products"]
    factores = json.load(open(config.BRONCE / "factores_mes.json", encoding="utf-8"))
    factores = {int(k): float(v) for k, v in factores.items()}
    return productos, factores


def demanda_dia(producto, tienda, fecha, factores):
    rotacion = config.ROTACION.get(producto["category"], 1.0)
    precio = float(producto["price"])
    # Lo barato rota y lo caro no, para simularlo bien, ponemos 0.12 (se vende poco) y el otro extremo (1.8) que vende mucho. Mientras mas caro, menos veces se vende y viceversa
    factor_precio = max(0.12, min(1.8, 1.8 / (1 + precio / 45)))

    media = rotacion * factor_precio * tienda["tamano"] * 0.32
    media *= config.FACTOR_DIA[fecha.weekday()]
    media *= factores.get(fecha.month, 1.0)

    if media < 0.45:
        return int(rng.integers(1, 4)) if rng.random() < media else 0
    return int(rng.poisson(media))

class Almacen:
    def __init__(self, productos):
        self.stock = {}
        self.minimo = {}
        self.lote = {}
        self.pedidos = []
        for p in productos:
            for t in config.TIENDAS:
                clave = (p["sku"], t["tienda_id"])
                inicial = max(4, int(p["stock"] * t["tamano"] * 0.6))
                self.stock[clave] = inicial
                self.minimo[clave] = max(2, int(inicial * 0.22))
                self.lote[clave] = max(int(p.get("minimumOrderQuantity") or 1), int(inicial * 0.7))


    def vender(self, sku, tienda_id, unidades):
        clave = (sku, tienda_id)
        vendidas = min(unidades, self.stock.get(clave, 0))
        self.stock[clave] -= vendidas
        return vendidas

    def recibir(self, fecha):
        pendientes = []
        for llegada, sku, tienda_id, cantidad in self.pedidos:
            if llegada <= fecha:
                self.stock[(sku, tienda_id)] += cantidad
            else:
                pendientes.append((llegada, sku, tienda_id, cantidad))
        self.pedidos = pendientes

    def pedir(self, fecha):
        en_camino = {(s, t) for f, s, t, f in self.pedidos}
        for (sku, tienda_id), unidades in self.stock.items():
            if (sku, tienda_id) in en_camino or unidades > self.minimo[(sku, tienda_id)]:
                continue
            lead = next(t["lead_time"] for t in config.TIENDAS if t["tienda_id"] == tienda_id)
            # Uno de cada cuatro proveedores se retrasa
            retraso = int(rng.integers(1, 4)) if rng.random() < 0.25 else 0
            self.pedidos.append((fecha + timedelta(days=lead + retraso), sku, tienda_id, self.lote[(sku, tienda_id)]))


    def en_camino(self, sku, tienda_id):
        return sum(c for _, s, t, c in self.pedidos if s == sku and t == tienda_id)

def crear_venta(producto, tienda_id, unidades, momento):
    precio = round(float(producto["price"]), 2)
    return {
        "venta_id": str(uuid.uuid4()),
        "momento": momento.isoformat(),
        "tienda_id": tienda_id,
        "sku": producto["sku"],
        "unidades": unidades,
        "precio": precio,
        "importe": round(precio * unidades, 2),
}


def ensuciar(venta):
    #simulamos en "sucio" en la data, como siempre pasa en un POS
    if rng.random() < config.TASA_CANTIDAD_MALA:
        venta["unidades"] = 0 if rng.random() < 0.5 else -1
    if rng.random() < config.TASA_SKU_DESCONOCIDO:
        venta["sku"] = f"SIN-CATALOGAR-{int(rng.integers(0, 999)):03d}"
    return [venta, dict(venta)] if rng.random() < config.TASA_DUPLICADOS else [venta]



#inventario al cierres y ventas que toca hacer hoy
def plan_del_dia(productos, fecha, factores, almacen):
    ventas, inventario = [], []
    for t in config.TIENDAS:
        for p in productos:
            pedidas = demanda_dia(p, t, fecha, factores)
            vendidas = almacen.vender(p["sku"], t["tienda_id"], pedidas) if pedidas else 0
            if vendidas:
                hora = datetime(fecha.year, fecha.month, fecha.day, 9, tzinfo=timezone.utc)
                hora += timedelta(minutes=int(rng.integers(0, 720)))
                ventas.extend(ensuciar(crear_venta(p, t["tienda_id"], vendidas, hora)))

            queda = almacen.stock[(p["sku"], t["tienda_id"])]
            inventario.append({
                "fecha": fecha.isoformat(),
                "tienda_id": t["tienda_id"],
                "sku": p["sku"],
                "stock": queda,
                "en_camino": almacen.en_camino(p["sku"], t["tienda_id"]),
                "quiebre": queda <= 0,
            })
    return ventas, inventario



def historico(dias):
    productos, factores = cargar()
    almacen = Almacen(productos)
    fin = config.hoy()
    todas_ventas, todo_inventario = [], []

    for i in range(dias):
        fecha = fin - timedelta(days=dias - 1 - i)
        almacen.recibir(fecha)
        ventas, inventario = plan_del_dia(productos, fecha, factores, almacen)
        almacen.pedir(fecha)
        todas_ventas.extend(ventas)
        todo_inventario.extend(inventario)

    config.BRONCE.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(todas_ventas).to_csv(config.BRONCE / "ventas_historico.csv", index=False)
    pd.DataFrame(todo_inventario).to_csv(config.BRONCE / "inventario.csv", index=False)
    json.dump({"stock": {f"{k[0]}|{k[1]}": v for k, v in almacen.stock.items()}},
              open(config.BRONCE / "_estado.json", "w"))

    quiebres = sum(1 for x in todo_inventario if x["quiebre"])
    config.log(f"historico: {len(todas_ventas)} ventas en {dias} dias, {quiebres} observaciones en quiebre")


def vivo():
    from confluent_kafka import Producer

    productos, factores = cargar()
    almacen = Almacen(productos)
    estado = config.BRONCE / "_estado.json"
    if estado.exists():
        guardado = json.load(open(estado))["stock"]
        for clave, unidades in guardado.items():
            sku, tienda_id = clave.split("|")
            almacen.stock[(sku, tienda_id)] = unidades
        config.log("inventario recuperado del historico")

    productor = Producer({"bootstrap.servers": config.KAFKA})
    espera = 1 / max(config.EVENTOS_POR_SEGUNDO, 0.01)
    dia, pendientes = None, []

    while True:
        ahora = config.hoy()
        if dia != ahora:
            almacen.recibir(ahora)
            almacen.pedir(ahora)
            pendientes, inventario = plan_del_dia(productos, ahora, factores, almacen)
            pd.DataFrame(inventario).to_csv(config.BRONCE / "inventario_hoy.csv", index=False)
            dia = ahora
            config.log(f"plan del dia {ahora}: {len(pendientes)} ventas")

        if not pendientes:
            # ya se vendio lo que tocaba hoy
            config.log("demanda del dia completada, esperando al siguiente")
            time.sleep(60)
            continue

        venta = pendientes.pop()
        # para poder medir la latencia
        venta["momento"] = datetime.now(timezone.utc).isoformat()
        productor.produce(config.TOPIC, key=venta["tienda_id"].encode(),
                          value=json.dumps(venta).encode())
        productor.poll(0)
        time.sleep(espera)


if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "vivo"
    if modo == "historico":
        historico(int(sys.argv[2]) if len(sys.argv) > 2 else 60)
    else:
        vivo()