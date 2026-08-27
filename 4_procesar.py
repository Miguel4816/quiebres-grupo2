import json
import shutil
import sys
import time
from datetime import timedelta

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession

import config

DIAS_TEST = 7

VARIABLES = ["media_7", "media_28", "desv_14", "quiebres_14",
             "stock", "en_camino", "dias_cobertura", "lead_time"]


def crear_spark():
    spark = (SparkSession.builder
             .appName("quiebres")
             .master("local[*]")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def guardar(spark, vista, carpeta):
    spark.table(vista).write.mode("overwrite").parquet(str(config.ORO / carpeta))


def materializar(spark, vista, ruta):
    #materializa la vista
    ruta.parent.mkdir(parents=True, exist_ok=True)
    spark.table(vista).write.mode("overwrite").parquet(str(ruta))
    spark.read.parquet(str(ruta)).createOrReplaceTempView(vista)


#####################################################################################################
#BRONCE, carga todas las vistas
######################################################################################################

def cargar_bronce(spark):
    productos = pd.DataFrame(json.load(open(config.BRONCE / "catalogo.json", encoding="utf-8"))["products"])
    spark.createDataFrame(productos).createOrReplaceTempView("catalogo")

    spark.createDataFrame(pd.DataFrame(config.TIENDAS)).createOrReplaceTempView("tiendas")

    factores = json.load(open(config.BRONCE / "factores_mes.json", encoding="utf-8"))
    spark.createDataFrame(
        pd.DataFrame([{"mes": int(m), "factor": float(f)} for m, f in factores.items()])
    ).createOrReplaceTempView("factor_mes")

    # spark usa el day of week al revés de lo que tenemos en factor dia de dom a lunes y al revés
    equivalencia = {1: 6, 2: 0, 3: 1, 4: 2, 5: 3, 6: 4, 7: 5}
    spark.createDataFrame(
        pd.DataFrame([{"dia": d, "factor": config.FACTOR_DIA[p]} for d, p in equivalencia.items()])
    ).createOrReplaceTempView("factor_dia")

    (spark.read.csv(str(config.BRONCE / "ventas_historico.csv"), header=True, inferSchema=True)
     .createOrReplaceTempView("ventas_historico"))



    
    ultimo = spark.sql("SELECT MAX(DATE(momento)) FROM ventas_historico").first()[0]
    hueco = (config.hoy() - ultimo).days
    if hueco > 1:
        config.log(f"AVISO: el historico termina el {ultimo}, hace {hueco} dias. "
                   f"Regeneralo con: python 2_simulador.py historico 60")




    stream = config.BRONCE / "ventas_stream"
    if stream.exists() and any(stream.glob("*.csv")):
        (spark.read.csv(str(stream / "*.csv"), header=True, inferSchema=True)
         .createOrReplaceTempView("ventas_stream"))
    else:
        spark.sql("""
            SELECT venta_id, momento, tienda_id, sku, unidades, precio, importe,
                   CAST(NULL AS string) AS recibido,
                   CAST(NULL AS int)    AS particion,
                   CAST(NULL AS bigint) AS offset_kafka
            FROM   ventas_historico
            WHERE  1 = 0
        """).createOrReplaceTempView("ventas_stream")


    (spark.read.csv(str(config.BRONCE / "inventario.csv"), header=True, inferSchema=True)
     .createOrReplaceTempView("inventario_historico"))

    hoy = config.BRONCE / "inventario_hoy.csv"
    if hoy.exists():
        spark.read.csv(str(hoy), header=True, inferSchema=True).createOrReplaceTempView("inventario_vivo")
    else:
        spark.sql("SELECT * FROM inventario_historico WHERE 1 = 0").createOrReplaceTempView("inventario_vivo")

    # hay que elegir entre el en vivo y el inventario historico
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW inventario AS
        SELECT fecha, tienda_id, sku, stock, en_camino, quiebre
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY fecha, tienda_id, sku
                                         ORDER BY prioridad DESC) AS n
            FROM (
                SELECT fecha, tienda_id, sku, stock, en_camino, quiebre, 2 AS prioridad
                FROM   inventario_vivo
                UNION ALL
                SELECT fecha, tienda_id, sku, stock, en_camino, quiebre, 1 AS prioridad
                FROM   inventario_historico
            )
        )
        WHERE n = 1
    """)



######################################################################################################
# PLATA , aqui unificamos y quitamos duplicados, limpieza
######################################################################################################








def plata(spark):
  
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW ventas_crudas AS
        SELECT venta_id, momento, tienda_id, sku, unidades, precio, importe,
               CAST(NULL AS timestamp) AS recibido,
               CAST(NULL AS int)       AS particion,
               CAST(NULL AS bigint)    AS offset_kafka,
               'historico' AS origen
        FROM   ventas_historico
        UNION ALL
        SELECT venta_id, momento, tienda_id, sku, unidades, precio, importe,
               CAST(recibido     AS timestamp) AS recibido,
               CAST(particion    AS int)       AS particion,
               CAST(offset_kafka AS bigint)    AS offset_kafka,
               'streaming' AS origen
        FROM   ventas_stream
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW ventas_revisadas AS
        SELECT v.*,
               CASE
                   WHEN v.venta_id IS NULL                       THEN 'sin identificador'
                   WHEN c.sku      IS NULL                       THEN 'sku no catalogado'
                   WHEN v.unidades IS NULL OR v.unidades <= 0    THEN 'unidades no validas'
                   WHEN v.precio   IS NULL OR v.precio   <= 0    THEN 'precio no valido'
                   ELSE NULL
               END AS motivo_rechazo
        FROM      ventas_crudas v
        LEFT JOIN catalogo      c ON c.sku = v.sku
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW rechazos AS
        SELECT motivo_rechazo, COUNT(*) AS registros
        FROM   ventas_revisadas
        WHERE  motivo_rechazo IS NOT NULL
        GROUP BY motivo_rechazo
    """)

    #
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW ventas AS
        SELECT venta_id,
               CAST(momento AS timestamp) AS momento,
               DATE(momento)              AS fecha,
               tienda_id, sku, unidades, precio, importe,
               recibido, particion, offset_kafka, origen
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY venta_id ORDER BY origen) AS aparicion
            FROM   ventas_revisadas
            WHERE  motivo_rechazo IS NULL
        )
        WHERE aparicion = 1
    """)

    materializar(spark, "ventas", config.PLATA / "ventas")

    entradas = spark.table("ventas_crudas").count()
    validas = spark.table("ventas").count()
    rechazadas = spark.sql("SELECT COALESCE(SUM(registros), 0) FROM rechazos").first()[0]

    spark.createDataFrame(pd.DataFrame([{
        "entradas": entradas,
        "validas": validas,
        "rechazadas": int(rechazadas),
        "duplicadas": entradas - validas - int(rechazadas),
        "calidad_pct": round(validas / entradas * 100, 2) if entradas else 0.0,
    }])).createOrReplaceTempView("calidad")

    guardar(spark, "rechazos", "rechazos")
    guardar(spark, "calidad", "calidad")
    config.log(f"plata: {validas} ventas validas de {entradas}, {rechazadas} rechazadas")




######################################################################################################
    # ORO, modelo estrella
######################################################################################################



def oro(spark):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW dim_producto AS
        SELECT sku,
               title    AS nombre,
               category AS categoria,
               brand    AS marca,
               CAST(price AS double)                        AS precio,
               CAST(COALESCE(minimumOrderQuantity, 1) AS int) AS lote_minimo
        FROM catalogo
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW dim_tienda AS
        SELECT tienda_id, nombre AS tienda, lead_time FROM tiendas
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW dim_tiempo AS
        SELECT c.fecha,
               MONTH(c.fecha)               AS mes,
               DATE_FORMAT(c.fecha, 'EEEE') AS nombre_dia,
               fm.factor                    AS indice_ine,
               fd.factor                    AS factor_dia
        FROM (SELECT EXPLODE(SEQUENCE(MIN(fecha), MAX(fecha), INTERVAL 1 DAY)) AS fecha FROM ventas) c
        JOIN factor_mes fm ON fm.mes = MONTH(c.fecha)
        JOIN factor_dia fd ON fd.dia = DAYOFWEEK(c.fecha)
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW ventas_dia AS
        SELECT fecha, tienda_id, sku,
               SUM(unidades)        AS unidades,
               ROUND(SUM(importe), 2) AS importe
        FROM   ventas
        GROUP BY fecha, tienda_id, sku
    """)

    # 
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW hecho_ventas AS
        SELECT t.fecha, s.tienda_id, p.sku,
               COALESCE(v.unidades,  0)     AS unidades,
               COALESCE(v.importe,   0)     AS importe,
               COALESCE(i.stock,     0)     AS stock,
               COALESCE(i.en_camino, 0)     AS en_camino,
               COALESCE(i.quiebre,   false) AS quiebre
        FROM       dim_tiempo   t
        CROSS JOIN dim_tienda   s
        CROSS JOIN dim_producto p
        LEFT JOIN  ventas_dia   v ON v.fecha = t.fecha AND v.tienda_id = s.tienda_id AND v.sku = p.sku
        LEFT JOIN  inventario   i ON i.fecha = t.fecha AND i.tienda_id = s.tienda_id AND i.sku = p.sku
    """)

    for vista in ("dim_producto", "dim_tienda", "dim_tiempo"):
        materializar(spark, vista, config.ORO / vista)
    materializar(spark, "hecho_ventas", config.ORO / "hecho_ventas")

    filas = spark.table("hecho_ventas").count()
    esperadas = spark.sql("""
        SELECT (SELECT COUNT(*) FROM dim_tiempo)
             * (SELECT COUNT(*) FROM dim_tienda)
             * (SELECT COUNT(*) FROM dim_producto)
    """).first()[0]
    if filas != esperadas:
  
        config.log(f"AVISO: hecho_ventas tiene {filas} filas y deberia tener {esperadas}")
    config.log(f"oro: {filas} filas en hecho_ventas")




    ######################################################################################################
    # PRONOSTICO
    ######################################################################################################




def pronostico(spark):
    # sacando la media sin tener en cuenta la estacionalidad
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW serie AS
        SELECT h.fecha, h.tienda_id, h.sku, h.unidades,
               t.factor_dia, t.indice_ine,
               h.unidades / (t.factor_dia * t.indice_ine) AS sin_estacion
        FROM hecho_ventas h
        JOIN dim_tiempo   t ON t.fecha = h.fecha
    """)

    
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW pronostico AS
        SELECT fecha, tienda_id, sku, unidades,
               ROUND(media_7, 3) AS pred_base,
               ROUND(media_7_limpia * factor_dia * indice_ine, 3) AS pred_modelo
        FROM (
            SELECT fecha, tienda_id, sku, unidades, factor_dia, indice_ine,
                   COALESCE(AVG(unidades)     OVER (PARTITION BY tienda_id, sku ORDER BY fecha
                                                    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING), 0) AS media_7,
                   COALESCE(AVG(sin_estacion) OVER (PARTITION BY tienda_id, sku ORDER BY fecha
                                                    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING), 0) AS media_7_limpia
            FROM serie
        )
    """)

    materializar(spark, "pronostico", config.ORO / "pronostico")

    ultima = spark.sql("SELECT MAX(fecha) FROM hecho_ventas").first()[0]
    corte = ultima - timedelta(days=DIAS_TEST)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW metricas_pronostico AS
        SELECT COUNT(*)                                            AS observaciones,
               ROUND(AVG(ABS(pred_base   - unidades)), 4)          AS mae_base,
               ROUND(AVG(ABS(pred_modelo - unidades)), 4)          AS mae_modelo,
               ROUND(SUM(ABS(pred_modelo - unidades)) / SUM(unidades), 4) AS wape_modelo
        FROM   pronostico
        WHERE  fecha > DATE('{corte}')
    """)
    guardar(spark, "metricas_pronostico", "metricas_pronostico")



    
    futuro = []
    for h in range(1, max(config.HORIZONTES) + 1):
        fecha = ultima + timedelta(days=h)
        futuro.append({"horizonte": h, "fecha": fecha,
                       "factor_dia": config.FACTOR_DIA[fecha.weekday()],
                       "indice_ine": spark.sql(f"SELECT factor FROM factor_mes WHERE mes = {fecha.month}").first()[0]})
    spark.createDataFrame(pd.DataFrame(futuro)).createOrReplaceTempView("dias_futuros")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW nivel_actual AS
        SELECT tienda_id, sku, pred_modelo / (factor_dia * indice_ine) AS nivel
        FROM (
            SELECT p.tienda_id, p.sku, p.pred_modelo, t.factor_dia, t.indice_ine
            FROM pronostico p JOIN dim_tiempo t ON t.fecha = p.fecha
            WHERE p.fecha = DATE('{ultima}')
        )
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW prevision AS
        SELECT n.tienda_id, n.sku, d.horizonte, d.fecha,
               ROUND(GREATEST(n.nivel * d.factor_dia * d.indice_ine, 0), 3) AS demanda,
               ROUND(SUM(GREATEST(n.nivel * d.factor_dia * d.indice_ine, 0))
                     OVER (PARTITION BY n.tienda_id, n.sku ORDER BY d.horizonte), 3) AS demanda_acumulada
        FROM nivel_actual n
        CROSS JOIN dias_futuros d
    """)
    guardar(spark, "prevision", "prevision")

    fila = spark.table("metricas_pronostico").first()
    config.log(f"pronostico: MAE base {fila.mae_base} -> modelo {fila.mae_modelo}")


def procesar(spark):
    cargar_bronce(spark)
    plata(spark)
    oro(spark)
    pronostico(spark)

if __name__ == "__main__":
    config.ORO.mkdir(parents=True, exist_ok=True)
    spark = crear_spark()

    if len(sys.argv) > 1 and sys.argv[1] == "bucle":
        while True:
            inicio = time.time()
            try:
                procesar(spark)
                config.log(f"ciclo terminado en {time.time() - inicio:.0f}s")
            except Exception as e:
                config.log(f"el ciclo ha fallado: {e}")
            time.sleep(max(config.MINUTOS_CICLO * 60 - (time.time() - inicio), 30))
    else:
        procesar(spark)
        spark.stop()
