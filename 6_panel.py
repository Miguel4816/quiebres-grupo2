import pandas as pd
import plotly.express as px
import requests
import streamlit as st

import config

st.set_page_config(page_title="Quiebres de stock", layout="wide")

COLORES = {"CRITICO": "#c0392b", "ALTO": "#e67e22", "MEDIO": "#f1c40f"}


@st.cache_data(ttl=30)
def pedir(ruta, parametros=None):
    r = requests.get(config.API + ruta, params=parametros,
                     headers={"X-API-Key": config.API_KEY}, timeout=30)
    return None if r.status_code == 503 else r.json()


st.title(" Prevención de quiebres")
st.caption("Proyecto Final Grupo 2")

vista = st.sidebar.radio("Vista", ["Alertas", "Calidad y modelos"])
if st.sidebar.button("Actualizar"):
    st.cache_data.clear()

datos = pedir("/kpis")
if not datos:
    st.info("El pipeline aun no ha publicado datos. Espera al primer ciclo.")
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Alertas criticas", int(datos["criticas"]))
c2.metric("Alertas altas", int(datos["altas"]))
c3.metric("Tasa de quiebre 7d", f"{datos['tasa_quiebre_pct']:.2f} %")
c4.metric("Valor en riesgo", f"{datos['valor_en_riesgo']:,.0f} EUR")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Calidad del dato", f"{datos['calidad_pct']:.2f} %")
c2.metric("Ventas por streaming", int(datos["ventas_streaming"]))
c3.metric("Latencia del POS al lago", f"{datos['latencia_ingesta_seg']:.2f} s")
c4.metric("Antiguedad del dato", f"{datos['antiguedad_dato_seg'] / 60:.1f} min")


if vista == "Alertas":
    c1, c2 = st.columns(2)
    nivel = c1.selectbox("Nivel", ["Todos", "CRITICO", "ALTO", "MEDIO"])
    horizonte = c2.selectbox("Horizonte (dias)", [7, 14, 3, 1])

    parametros = {"horizonte": horizonte, "limite": 300}
    if nivel != "Todos":
        parametros["nivel"] = nivel

    alertas = pedir("/alertas", parametros)
    if not alertas or not alertas["alertas"]:
        st.success("No hay alertas con esos filtros.")
    else:
        df = pd.DataFrame(alertas["alertas"])
        st.dataframe(df, use_container_width=True, hide_index=True, height=460)
        st.caption(f"{len(df)} alertas ordenadas por prioridad: primero el nivel de la regla, luego la probabilidad del modelo.")
        st.download_button("Descargar en CSV", df.to_csv(index=False),
                           file_name="alertas.csv", mime="text/csv")

        resumen = df.groupby(["tienda", "nivel"]).size().reset_index(name="alertas")
        st.plotly_chart(px.bar(resumen, x="tienda", y="alertas", color="nivel",
                               color_discrete_map=COLORES, title="Alertas por tienda"),
                        use_container_width=True)

else:
    calidad = pedir("/calidad")
    if calidad:
        st.subheader("Calidad del dato")
        r = calidad["resumen"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Ventas recibidas", int(r["entradas"]))
        c2.metric("Validas", int(r["validas"]))
        c3.metric("Rechazadas", int(r["rechazadas"]))
        c4.metric("Duplicadas", int(r["duplicadas"]))
        if calidad["rechazos"]:
            st.plotly_chart(px.bar(pd.DataFrame(calidad["rechazos"]), x="registros", y="motivo_rechazo",
                                   orientation="h", title="Motivos de rechazo"),
                            use_container_width=True)

    modelo = pedir("/modelo")
    if modelo:
        st.subheader("Validacion de los modelos")
        p, m = modelo["pronostico"], modelo["clasificador"]

        c1, c2, c3 = st.columns(3)
        c1.metric("MAE media movil", f"{p['mae_base']:.3f}")
        c2.metric("MAE modelo", f"{p['mae_modelo']:.3f}",
                  f"{(p['mae_base'] - p['mae_modelo']) / p['mae_base'] * 100:.1f} %")
        c3.metric("AUC clasificador", f"{m['auc']:.3f}")

        c1, c2 = st.columns(2)
        c1.metric("Recall del modelo", f"{m['recall']:.3f}", f"{m['recall'] - m['recall_regla']:+.3f} vs regla")
        c2.metric("Precision del modelo", f"{m['precision']:.3f}", f"{m['precision'] - m['precision_regla']:+.3f} vs regla")

        st.plotly_chart(px.bar(pd.DataFrame(modelo["importancia"]).sort_values("importancia"),
                               x="importancia", y="variable", orientation="h",
                               title="Variables que mas pesan en la prediccion"),
                        use_container_width=True)

    linaje = pedir("/linaje", {"limite": 20})
    if linaje and linaje["cargas"]:
        st.subheader("Linaje de las cargas")
        st.caption("De donde vino cada dato y cuando. Se escribe en cada ingesta")
        st.dataframe(pd.DataFrame(linaje["cargas"]), use_container_width=True, hide_index=True)