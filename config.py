import os
from datetime import datetime, timezone
from pathlib import Path

DATOS = Path(os.getenv("DATOS", "/app/datos"))
BRONCE = DATOS / "bronce"
PLATA = DATOS / "plata"
ORO = DATOS / "oro"
MODELO = DATOS / "modelo"
SALIDA = DATOS / "salida"


KAFKA = os.getenv("KAFKA", "kafka:9092")
TOPIC = "ventas"


API = os.getenv("API", "http://api:8000")
API_KEY = os.getenv("API_KEY", "proyecto-grupo2")

URL_CATALOGO = "https://dummyjson.com/products"
URL_INE = "https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/ICM3465"

SEMILLA = 20262
EVENTOS_POR_SEGUNDO = float(os.getenv("EVENTOS_POR_SEGUNDO", "0.3"))
MINUTOS_CICLO = int(os.getenv("MINUTOS_CICLO", "5"))

TIENDAS = [
    {"tienda_id": "T01", "nombre": "Madrid Centro", "tamano": 1.35, "lead_time": 2},
    {"tienda_id": "T02", "nombre": "Barcelona Diagonal", "tamano": 1.15, "lead_time": 3},
    {"tienda_id": "T03", "nombre": "Valencia Norte", "tamano": 0.85, "lead_time": 4},
    {"tienda_id": "T04", "nombre": "Sevilla Este", "tamano": 0.75, "lead_time": 5},
    {"tienda_id": "T05", "nombre": "Bilbao Abando", "tamano": 0.60, "lead_time": 4},
]


#estimaciones

ROTACION = {
    "groceries": 9.0,
    "mobile-accessories": 4.0,
    "beauty": 3.0,
    "kitchen-accessories": 2.2,
    "skin-care": 2.0,
    "tops": 1.8,
    "sports-accessories": 1.7,
    "mens-shirts": 1.6,
    "home-decoration": 1.5,
    "womens-dresses": 1.4,
    "womens-shoes": 1.2,
    "fragrances": 1.2,
    "mens-shoes": 1.1,
    "sunglasses": 1.0,
    "womens-bags": 0.9,
    "womens-jewellery": 0.8,
    "smartphones": 0.6,
    "furniture": 0.4,
    "mens-watches": 0.4,
    "womens-watches": 0.4,
    "tablets": 0.35,
    "laptops": 0.3,
    "motorcycle": 0.2,
    "vehicle": 0.15,
}

# Lunes = 0 ... Domingo = 6
FACTOR_DIA = {0: 0.88, 1: 0.86, 2: 0.92, 3: 1.00, 4: 1.22, 5: 1.35, 6: 0.62}

Z_SERVICIO = 1.65
HORIZONTES = [1, 3, 7, 14]

TASA_DUPLICADOS = 0.02
TASA_CANTIDAD_MALA = 0.01
TASA_SKU_DESCONOCIDO = 0.005


def hoy():
    return datetime.now(timezone.utc).date()


def log(mensaje):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {mensaje}", flush=True)