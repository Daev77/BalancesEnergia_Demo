from flask import Flask, Response, stream_with_context
from io import StringIO, BytesIO
import pandas as pd
import requests
import json
import io
import time

app = Flask(__name__)

@app.route('/')
def home():
    # Utilizamos stream_with_context para generar una respuesta que se envíe de manera progresiva.
    def generar_respuesta():
        try:
            # Paso 1: Extracción de datos
            yield "data: Iniciando extracción de datos...\n\n"
            time.sleep(1)  # Simulamos un retraso para que se vea la actualización en tiempo real

            fecha_inicio = "2024-07-09"
            fecha_fin = "2024-07-20"
            file_id = ''  # Acceso para descarga de archivos de las empresas generadoras

            api_key_1 = ""
            api_key_2 = ""

            # Precios Bolsa
            url = f"https://www.simem.co/backend-files/api/PublicData?startDate={fecha_inicio}&endDate={fecha_fin}&datasetId=96D56E"
            response = requests.get(url)

            if response.status_code == 200:
                yield "data: Extracción de Precios de Bolsa: OK\n\n"
            else:
                yield "data: Error en la extracción de Precios de Bolsa\n\n"
                return

            data = response.json()
            records = data.get('result', {}).get('records', [])
            dfPreciosBolsa = pd.DataFrame(records)

            # Plan de despachos (compromisos)
            url = f"https://www.simem.co/backend-files/api/PublicData?startdate={fecha_inicio}&enddate={fecha_fin}&datasetId=ff027b"
            response = requests.get(url)

            if response.status_code == 200:
                yield "data: Extracción de Compromisos: OK\n\n"
            else:
                yield "data: Error en la extracción de Compromisos\n\n"
                return

            data = response.json()
            records = data.get('result', {}).get('records', [])
            dfCompriso = pd.DataFrame(records)

            # Capacidad Generadores
            url = f"https://drive.google.com/uc?id={file_id}"
            response = requests.get(url)
            dfCapacidad = pd.read_excel(BytesIO(response.content), header=5)

            yield "data: Extracción de Capacidad de Generadores: OK\n\n"
            time.sleep(1)  # Simulamos otro retraso

            # Paso 2: Transformación
            yield "data: Iniciando transformación de datos...\n\n"

            dfPreciosBolsa = dfPreciosBolsa.rename(columns={'fecha': 'Fecha'})
            dfCompriso = dfCompriso.rename(columns={'FechaHora': 'Fecha', 'Valor': 'Compromiso (Kwh)'})
            dfCapacidad = dfCapacidad.rename(columns={'CODIGO': 'CodigoPlanta', 'FECHA': 'Fecha', 'CAPACIDAD (Kwh)': 'Capacidad (Kwh)'})
            
            dfPreciosBolsa.dropna()
            dfCompriso.dropna()
            dfCapacidad.dropna()

            dfPreciosBolsa["Fecha"] = pd.to_datetime(dfPreciosBolsa["Fecha"])
            dfCompriso["Fecha"] = pd.to_datetime(dfCompriso["Fecha"])
            dfCapacidad["Fecha"] = pd.to_datetime(dfCapacidad["Fecha"])

            codigos_filtrar = ['ZPA2', 'ZPA3', 'ZPA4', 'ZPA5', 'GVIO', 'QUI1', 'CHVR']
            dfCompromisoFiltrado = dfCompriso[dfCompriso['CodigoPlanta'].isin(codigos_filtrar)]
            dfCapacidadFiltrado = dfCapacidad[dfCapacidad['CodigoPlanta'].isin(codigos_filtrar)]

            dfCapacidadFiltrado.loc[:, 'Fecha'] = dfCapacidadFiltrado['Fecha'].dt.date
            dfCompromisoFiltrado.loc[:, 'Fecha'] = dfCompromisoFiltrado['Fecha'].dt.date

            dfCapacidadAgrupado = dfCapacidadFiltrado.groupby(['CodigoPlanta', 'Fecha'])['Capacidad (Kwh)'].sum().reset_index()
            dfCompromisoAgrupado = dfCompromisoFiltrado.groupby(['CodigoPlanta', 'Fecha'])['Compromiso (Kwh)'].sum().reset_index()

            dfBalance = pd.merge(dfCapacidadAgrupado, dfCompromisoAgrupado, on=['CodigoPlanta', 'Fecha'], how='left')
            dfBalance['Compromiso (Kwh)'] = dfBalance['Compromiso (Kwh)'].dropna()

            dfBalance['Consolidado (Kwh)'] = dfBalance['Capacidad (Kwh)'] - dfBalance['Compromiso (Kwh)']
            dfBalance["Fecha"] = pd.to_datetime(dfBalance["Fecha"])

            dfPreciosFiltrado = dfPreciosBolsa[(dfPreciosBolsa['CodigoVariable'] == 'PPBOGReal') & (dfPreciosBolsa['Version'] == 'TXR')]
            dfBalanceConPrecios = pd.merge(dfBalance, dfPreciosFiltrado[['Fecha', 'Valor']], on='Fecha', how='left')
            dfBalanceConPrecios['Compromisos_MMCOP'] = dfBalanceConPrecios['Consolidado (Kwh)'] * dfBalanceConPrecios['Valor'] / 1000000
            dfBalanceConPrecios['Operación'] = dfBalanceConPrecios['Compromisos_MMCOP'].apply(lambda x: 'Compra' if x < 0 else 'Vender')

            dfBalanceConPrecios['Valor'] = dfBalanceConPrecios['Valor'].round(2)
            dfBalanceConPrecios['Compromisos_MMCOP'] = dfBalanceConPrecios['Compromisos_MMCOP'].round(2)

            yield "data: Transformación: OK\n\n"
            time.sleep(1)  # Simulamos otro retraso

            # Paso 3: Carga
            yield "data: Iniciando carga de datos...\n\n"

            csv_buffer = io.StringIO()
            dfBalanceConPrecios.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            # Autenticación para la API de carga
            file_upload_access_token = ""
            file_upload_account_id = ""
            params = {'key1': api_key_1, 'key2': api_key_2}
            response = requests.get("https://fastupload.io/api/v2/authorize", params)
            json_response = json.loads(response.text)
            try:
                file_upload_access_token = json_response["data"]["access_token"]
                file_upload_account_id = json_response["data"]["account_id"]
            except:
                yield "data: Error en la autenticación y autorización de carga\n\n"
                return

            upload_folder_id = ""  # Si tienes un folder ID
            csv_in_memory = io.BytesIO(csv_data.encode('utf-8'))

            files = {"upload_file": ("dfReporteCompraVentaEnergiaAcme.csv", csv_in_memory, 'text/csv')}
            data = {"access_token": file_upload_access_token, "account_id": file_upload_account_id, "folder_id": upload_folder_id}
            response = requests.post("https://fastupload.io/api/v2/file/upload", files=files, data=data)

            json_response = response.json()

            status = json_response.get('_status')
            datetime = json_response.get('_datetime')

            yield "data: Carga: OK\n\n"
            yield f"data: Estatus: {status}\n\n"
            yield f"data: Fecha y Hora Registro: {datetime}\n\n"

            # Mostrar contenido del CSV
            yield "data: Mostrando contenido del archivo CSV generado:\n\n"
            yield f"data: {csv_data}\n\n"

        except Exception as e:
            yield f"data: Error: {str(e)}\n\n"

    return Response(stream_with_context(generar_respuesta()), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
