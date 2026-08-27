
Aun faltan los KPI, en "procesar" falta el riesgo de quiebre calculado con el modelo, la API y un panel para mostrar todo en streamlit o streaming en kafka
docker compose build
docker compose up -d
docker compose run --rm simulador python 2_simulador.py historico 60
docker compose run --rm --no-deps procesar python 4_procesar.py

