import requests

def obtener_api():
    r = requests.get("https://api.exchangerate.host/latest?base=USD")
    print(r.json())

PythonOperator(task_id="cotizacion_usd", python_callable=obtener_api)
