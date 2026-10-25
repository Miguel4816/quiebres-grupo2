
INSTRUCCIONES PARA CORRER TODO

docker compose build
docker compose up -d
docker compose run --rm simulador python 2_simulador.py historico 60
docker compose run --rm --no-deps procesar python 4_procesar.py


Para ver los datos del sprint 3, dinero que no se pierde, etc:
hacerle un docker compose build
y luego:
docker compose run --rm --no-deps procesar python resumen_sprint3.py
