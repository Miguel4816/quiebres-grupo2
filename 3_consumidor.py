import json
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer

import config

SEGUNDOS_POR_LOTE = 60

DESTINO = config.BRONCE / "ventas_stream"


def guardar(lote):
    DESTINO.mkdir(parents=True, exist_ok=True)
    nombre = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + ".csv"
    pd.DataFrame(lote).to_csv(DESTINO / nombre, index=False)
    config.log(f"lote guardado: {len(lote)} ventas en {nombre}")


def main():
    consumidor = Consumer({
        "bootstrap.servers": config.KAFKA,
        "group.id": "quiebres",
        "auto.offset.reset": "earliest",
    })
    consumidor.subscribe([config.TOPIC])
    config.log(f"escuchando el topic {config.TOPIC}")

    lote, ultimo = [], time.time()
    while True:

        mensaje = consumidor.poll(1.0)

        #medimos la latencia
        if mensaje is not None and mensaje.error() is None:
            venta = json.loads(mensaje.value())
            venta["particion"] = mensaje.partition()
            venta["offset_kafka"] = mensaje.offset()
            venta["recibido"] = datetime.now(timezone.utc).isoformat()
            lote.append(venta)

#c ada 60 segundos se guarda el lote
        if lote and time.time() - ultimo > SEGUNDOS_POR_LOTE:
            guardar(lote)
            lote, ultimo = [], time.time()


if __name__ == "__main__":
    main()
