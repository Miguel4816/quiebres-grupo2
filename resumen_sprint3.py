

import json

import pandas as pd

import config

inventario = pd.read_csv(config.BRONCE / "inventario.csv")
ventas = pd.read_csv(config.BRONCE / "ventas_historico.csv")
catalogo = pd.DataFrame(json.load(open(config.BRONCE / "catalogo.json"))["products"])

dias = inventario["fecha"].nunique()
anual = 365 / dias

demandadas = inventario["demanda"].sum()
vendidas = inventario["vendidas"].sum()
no_atendidas = inventario["no_atendidas"].sum()
perdida = inventario["valor_no_atendido"].sum()
realizada = (ventas["unidades"] * ventas["precio"]).sum()

print(f"Periodo: {dias} dias, {len(config.TIENDAS)} tiendas, {len(catalogo)} productos")
print()
print(f"Unidades demandadas:   {round(demandadas)}")
print(f"Unidades vendidas:     {round(vendidas)}")
print(f"Unidades no atendidas: {round(no_atendidas)}")
print(f"Porcentaje no atendido: {round(100 * no_atendidas / demandadas, 2)} %")
print()
print(f"Venta realizada: {round(realizada, 2)} EUR")
print(f"Venta perdida:   {round(perdida, 2)} EUR")
print(f"Perdida sobre la realizada: {round(100 * perdida / realizada, 2)} %")
print()
print(f"Anualizado (x{round(anual, 2)}):")
print(f"  venta realizada: {round(realizada * anual, 2)} EUR")
print(f"  venta perdida:   {round(perdida * anual, 2)} EUR")
print()

print("Valor perdido por tienda:")
por_tienda = inventario.groupby("tienda_id")["valor_no_atendido"].sum()
for tienda in config.TIENDAS:
    valor = por_tienda[tienda["tienda_id"]]
    print(f"  {tienda['tienda_id']} (plazo {tienda['lead_time']} dias): {round(valor, 2)} EUR")
print()

print("Valor perdido por categoria:")
con_categoria = inventario.merge(catalogo[["sku", "category"]], on="sku")
por_categoria = con_categoria.groupby("category")["valor_no_atendido"].sum()
for categoria, valor in por_categoria.sort_values(ascending=False).head(5).items():
    print(f"  {categoria}: {round(valor, 2)} EUR")
print()

en_quiebre = inventario["quiebre"].sum()
print(f"Observaciones en quiebre: {en_quiebre} de {len(inventario)}")
print(f"Tasa de quiebre: {round(100 * en_quiebre / len(inventario), 2)} %")

#hacerle un docker compose build
#y luego:
#docker compose run --rm --no-deps procesar python resumen_sprint3.py