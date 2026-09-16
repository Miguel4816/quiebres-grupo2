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





# CLASIFICADOR, aqui predecimos, modelo trabaja

def clasificador(spark):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW variables AS
        SELECT fecha, tienda_id, sku, categoria, lead_time,
               CAST(stock     AS double) AS stock,
               CAST(en_camino AS double) AS en_camino,
               COALESCE(media_7,  0) AS media_7,
               COALESCE(media_28, 0) AS media_28,
               COALESCE(desv_14,  0) AS desv_14,
               COALESCE(quiebres_14, 0) AS quiebres_14,
               CASE WHEN COALESCE(media_7, 0) > 0
                    THEN LEAST(stock / media_7, 999) ELSE 999 END AS dias_cobertura,
               etiqueta
        FROM (
            SELECT h.fecha, h.tienda_id, h.sku, h.stock, h.en_camino,
                   p.categoria, CAST(s.lead_time AS double) AS lead_time,
                   AVG(h.unidades)    OVER (PARTITION BY h.tienda_id, h.sku ORDER BY h.fecha
                                            ROWS BETWEEN  7 PRECEDING AND 1 PRECEDING) AS media_7,
                   AVG(h.unidades)    OVER (PARTITION BY h.tienda_id, h.sku ORDER BY h.fecha
                                            ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS media_28,
                   STDDEV(h.unidades) OVER (PARTITION BY h.tienda_id, h.sku ORDER BY h.fecha
                                            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS desv_14,
                   SUM(CASE WHEN h.quiebre THEN 1 ELSE 0 END)
                                      OVER (PARTITION BY h.tienda_id, h.sku ORDER BY h.fecha
                                            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS quiebres_14,
                   MAX(CASE WHEN h.quiebre THEN 1 ELSE 0 END)
                                      OVER (PARTITION BY h.tienda_id, h.sku ORDER BY h.fecha
                                            ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING) AS etiqueta
            FROM hecho_ventas  h
            JOIN dim_producto  p ON p.sku       = h.sku
            JOIN dim_tienda    s ON s.tienda_id = h.tienda_id
        )
    """)

    materializar(spark, "variables", config.PLATA / "variables")

    ultima = spark.sql("SELECT MAX(fecha) FROM variables").first()[0]
    etiquetadas = spark.sql("SELECT * FROM variables WHERE etiqueta IS NOT NULL")
    ultima_etiquetada = etiquetadas.selectExpr("MAX(fecha)").first()[0]
    if ultima_etiquetada is None:
        config.log("sin historico suficiente para entrenar")
        return
    corte = ultima_etiquetada - timedelta(days=DIAS_TEST)

    entrena = etiquetadas.filter(f"fecha <= DATE('{corte}')")
    prueba = etiquetadas.filter(f"fecha >  DATE('{corte}')")

    positivos = entrena.filter("etiqueta = 1").count()
    total = entrena.count()
    if positivos in (0, total):
        config.log("no hay ejemplos de las dos clases")
        return

    #tuvimos que añadir esto porque acertaba los que eran quiebres porque eran minoria, ahora los quiebres van a pesar mas
    peso = (total - positivos) / positivos
    entrena = entrena.selectExpr("*", f"CASE WHEN etiqueta = 1 THEN {peso} ELSE 1.0 END AS peso")
    prueba = prueba.selectExpr("*", "1.0 AS peso")

    modelo = Pipeline(stages=[
        StringIndexer(inputCol="categoria", outputCol="categoria_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=VARIABLES + ["categoria_idx"], outputCol="features", handleInvalid="skip"),
        RandomForestClassifier(labelCol="etiqueta", weightCol="peso", numTrees=50, maxDepth=8, seed=42),
    ]).fit(entrena)

    predicciones = modelo.transform(prueba)
    auc = BinaryClassificationEvaluator(labelCol="etiqueta", metricName="areaUnderROC").evaluate(predicciones)


#verdaderos positivos es que avisó y se quebró, falsos negativos no aviso y se rompio, etc

    predicciones.createOrReplaceTempView("predicciones")
    m = spark.sql("""
        SELECT SUM(CASE WHEN etiqueta = 1 AND prediction = 1 THEN 1 ELSE 0 END) AS vp,
               SUM(CASE WHEN etiqueta = 0 AND prediction = 1 THEN 1 ELSE 0 END) AS fp,
               SUM(CASE WHEN etiqueta = 1 AND prediction = 0 THEN 1 ELSE 0 END) AS fn,
               SUM(CASE WHEN etiqueta = 0 AND prediction = 0 THEN 1 ELSE 0 END) AS vn,
               SUM(CASE WHEN etiqueta = 1 AND dias_cobertura <= lead_time THEN 1 ELSE 0 END) AS vp_regla,
               SUM(CASE WHEN etiqueta = 0 AND dias_cobertura <= lead_time THEN 1 ELSE 0 END) AS fp_regla,
               SUM(CASE WHEN etiqueta = 1 AND dias_cobertura >  lead_time THEN 1 ELSE 0 END) AS fn_regla
        FROM predicciones
    """).first()

    def ratios(vp, fp, fn):
        prec = vp / (vp + fp) if vp + fp else 0.0
        rec = vp / (vp + fn) if vp + fn else 0.0
        return round(prec, 4), round(rec, 4)

    precision, recall = ratios(m.vp, m.fp, m.fn)
    precision_regla, recall_regla = ratios(m.vp_regla, m.fp_regla, m.fn_regla)

    spark.createDataFrame(pd.DataFrame([{
        "auc": round(float(auc), 4),
        "precision": precision, "recall": recall,
        "precision_regla": precision_regla, "recall_regla": recall_regla,
        "vp": int(m.vp), "fp": int(m.fp), "fn": int(m.fn), "vn": int(m.vn),
        "peso_positivos": round(peso, 2),
    }])).createOrReplaceTempView("metricas_modelo")
    guardar(spark, "metricas_modelo", "metricas_modelo")

    bosque = modelo.stages[-1]
    spark.createDataFrame(pd.DataFrame([
        {"variable": n, "importancia": round(float(v), 4)}
        for n, v in zip(VARIABLES + ["categoria"], bosque.featureImportances.toArray())
    ])).createOrReplaceTempView("importancia")
    guardar(spark, "importancia", "importancia")

    if config.MODELO.exists():
        shutil.rmtree(config.MODELO)
    modelo.write().overwrite().save(str(config.MODELO))

    hoy = spark.sql(f"SELECT * FROM variables WHERE fecha = DATE('{ultima}')").selectExpr("*", "1.0 AS peso")
    (modelo.transform(hoy)
     .withColumn("prob", vector_to_array("probability"))
     .selectExpr("tienda_id", "sku", "ROUND(prob[1], 4) AS probabilidad")
     .write.mode("overwrite").parquet(str(config.ORO / "probabilidad")))

    config.log(f"modelo: AUC {auc:.3f}, recall {recall} contra {recall_regla} de la regla")





    # RIESGO, cuanto queda, cuando se agota, etc

def riesgo(spark):
    spark.read.parquet(str(config.ORO / "probabilidad")).createOrReplaceTempView("probabilidad")


#
#nota, nos sugieron la campana de gaus, la tabla dice:

#Nivel de servicio	z
#90 %	1,28
#95 %	1,65
#98 %	2,05
#99 %	2,33

#entonces para industria de retail, deberia ser 95% en productos normales

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW situacion AS
        SELECT h.tienda_id, h.sku, s.tienda, s.lead_time,
               p.nombre AS producto, p.categoria, p.precio, p.lote_minimo,
               CAST(h.stock AS double)     AS stock,
               CAST(h.en_camino AS double) AS en_camino,
               COALESCE(d.desviacion, 0)   AS desviacion,
               ROUND({config.Z_SERVICIO} * COALESCE(d.desviacion, 0) * SQRT(s.lead_time), 2) AS stock_seguridad
        FROM hecho_ventas h
        JOIN dim_tienda   s ON s.tienda_id = h.tienda_id
        JOIN dim_producto p ON p.sku       = h.sku
        JOIN (SELECT tienda_id, sku, STDDEV(unidades) AS desviacion
              FROM hecho_ventas GROUP BY tienda_id, sku) d
             ON d.tienda_id = h.tienda_id AND d.sku = h.sku
        WHERE h.fecha = (SELECT MAX(fecha) FROM hecho_ventas)
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW alertas AS
        SELECT s.tienda_id, s.tienda, s.sku, s.producto, s.categoria,
               f.horizonte, s.stock, s.en_camino, s.lead_time,
               f.demanda_acumulada AS demanda_prevista,
               s.stock_seguridad,
               ROUND(f.demanda_acumulada + s.stock_seguridad, 2) AS punto_pedido,
               ROUND(s.stock + s.en_camino - f.demanda_acumulada, 2) AS proyectado,
               COALESCE(pr.probabilidad, 0) AS probabilidad,
               CASE
                   WHEN s.stock + s.en_camino - f.demanda_acumulada <= 0                    THEN 'CRITICO'
                   WHEN s.stock + s.en_camino - f.demanda_acumulada <  s.stock_seguridad     THEN 'ALTO'
                   WHEN s.stock + s.en_camino - f.demanda_acumulada
                        <  f.demanda_acumulada + s.stock_seguridad                           THEN 'MEDIO'
                   ELSE 'OK'
               END AS nivel
        FROM situacion s
        JOIN prevision f ON f.tienda_id = s.tienda_id AND f.sku = s.sku
        LEFT JOIN probabilidad pr ON pr.tienda_id = s.tienda_id AND pr.sku = s.sku
        WHERE f.horizonte IN (%s)
    """ % ", ".join(str(h) for h in config.HORIZONTES))


    spark.sql("""
        CREATE OR REPLACE TEMP VIEW hecho_riesgo AS
        SELECT a.*,
               CASE WHEN a.nivel = 'OK' THEN 0
                    ELSE GREATEST(CEIL(a.punto_pedido - a.stock - a.en_camino), p.lote_minimo)
               END AS cantidad_sugerida,
               ROUND(CASE WHEN a.nivel = 'OK' THEN 0
                          ELSE GREATEST(CEIL(a.punto_pedido - a.stock - a.en_camino), p.lote_minimo)
                     END * p.precio, 2) AS valor_en_riesgo,
               CASE a.nivel WHEN 'CRITICO' THEN 1000 WHEN 'ALTO' THEN 500
                            WHEN 'MEDIO'   THEN 100  ELSE 0 END
               + a.probabilidad * 90 AS prioridad
        FROM alertas a
        JOIN dim_producto p ON p.sku = a.sku
    """)
    ruta = config.ORO / "hecho_riesgo"
    spark.table("hecho_riesgo").write.mode("overwrite").partitionBy("horizonte").parquet(str(ruta))
    spark.read.parquet(str(ruta)).createOrReplaceTempView("hecho_riesgo")

    resumen = spark.sql("SELECT nivel, COUNT(*) AS n FROM hecho_riesgo WHERE horizonte = 7 GROUP BY nivel").collect()
    config.log("riesgo a 7 dias: " + ", ".join(f"{r.nivel}={r.n}" for r in resumen))



# KPIS, exportacion y salidas

def kpis(spark):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW kpis AS
        SELECT (SELECT MAX(fecha) FROM hecho_ventas)                                    AS fecha,
               (SELECT calidad_pct FROM calidad)                                        AS calidad_pct,
               (SELECT COUNT(*) FROM hecho_riesgo WHERE horizonte = 7 AND nivel = 'CRITICO') AS criticas,
               (SELECT COUNT(*) FROM hecho_riesgo WHERE horizonte = 7 AND nivel = 'ALTO')    AS altas,
               (SELECT COUNT(*) FROM hecho_riesgo WHERE horizonte = 7 AND nivel = 'MEDIO')   AS medias,
               (SELECT ROUND(SUM(valor_en_riesgo), 2) FROM hecho_riesgo
                 WHERE horizonte = 7 AND nivel <> 'OK')                                 AS valor_en_riesgo,
               (SELECT ROUND(AVG(CASE WHEN quiebre THEN 100.0 ELSE 0 END), 2)
                  FROM hecho_ventas
                 WHERE fecha > DATE_SUB((SELECT MAX(fecha) FROM hecho_ventas), 7))      AS tasa_quiebre_pct,
               (SELECT COUNT(*) FROM ventas WHERE origen = 'streaming')                 AS ventas_streaming,
               (SELECT COALESCE(ROUND(AVG(UNIX_TIMESTAMP(recibido) - UNIX_TIMESTAMP(momento)), 2), 0)
                  FROM ventas WHERE origen = 'streaming')                               AS latencia_ingesta_seg,
               (SELECT COALESCE(ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(momento)), 0), 0)
                  FROM ventas WHERE origen = 'streaming')                               AS antiguedad_dato_seg
    """)
    guardar(spark, "kpis", "kpis")
    config.log("kpis: " + str(spark.table("kpis").first().asDict()))



def exportar(spark):
    config.SALIDA.mkdir(parents=True, exist_ok=True)
    alertas = spark.sql("""
        SELECT tienda, producto, sku, nivel, stock, en_camino, demanda_prevista,
               proyectado, cantidad_sugerida, valor_en_riesgo
        FROM   hecho_riesgo
        WHERE  horizonte = 7 AND nivel <> 'OK'
        ORDER BY prioridad DESC
    """).toPandas()
    alertas.to_csv(config.SALIDA / "alertas.csv", index=False)
    config.log(f"exportadas {len(alertas)} alertas a salida/alertas.csv")



















def procesar(spark):
    cargar_bronce(spark)
    plata(spark)
    oro(spark)
    pronostico(spark)
    clasificador(spark)
    riesgo(spark)
    kpis(spark)
    exportar(spark)


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




    

















    def procesar(spark):
        cargar_bronce(spark)
        plata(spark)
        oro(spark)
        pronostico(spark)
        clasificador(spark)
        riesgo(spark)
        kpis(spark)
        exportar(spark)


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