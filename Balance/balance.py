# Se desarrolla a continuación el script para correr la aplicación de balances de energía

#Se importan las librerias requeridas
from flask import Flask, jsonify
from io import StringIO, BytesIO
import pandas as pd
import requests
import json
import io

app = Flask(__name__)
@app.route('/')

def home():

    #Se definen los parámetros para correr la aplicación
    # Se definen a continuación las variables para correr la aplicación:

    fecha_inicio="2024-07-09" # define la fecha inicial desde la cual se solicita información de archivos de XM
    fecha_fin="2024-07-20"    # define la fecha final hasta la cual se solicita información de archivos de XM
    file_id = '14WlAlvQj2Xn1SLMvZOLteeqPi7XZX_z0' # acceso para descarga de archivos de las empresas generadoras

    #llaves para conexión por API a XM para cargar balance de energia
    api_key_1="6AjqxTnrfk2IQpm6O8E3xwwkXclCmNYck9ggWo8321OjhQO1BCkGDUlrPkLr9pnt"
    api_key_2="q0Id5omKsRyW90h1p3JoCEMDLTJ4kXowAfeCDnATjvHK2Zp7KciuhaKKH1u3EMiU"


    # PASO 1: EXTRACCIONES

    # Precios Bolsa:

    # Se consumen los datos desde la página de XM para el archivo de precios de bolsa:
    url = f"https://www.simem.co/backend-files/api/PublicData?startDate={fecha_inicio}&endDate={fecha_fin}&datasetId=96D56E"
    response = requests.get(url)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        content_type = response.headers['Content-Type']
    else:
        print("Error al descargar el archivo:", response.status_code)


    #Se crea la variable data para almacenar el archivo json descargado desde XM. Luego se toman los datos "recors" del campo
    #results y se crea, finalmente, el dataframe que contiene la información requeridad de precios de bolsa.
    data = response.json()
    records = data.get('result', {}).get('records', [])
    dfPreciosBolsa = pd.DataFrame(records)        

    # Plan de despachos (compromisos)

    #Se consumen los datos desde la página de XM para determinar los compromisos de despacho:
    url = f"https://www.simem.co/backend-files/api/PublicData?startdate={fecha_inicio}&enddate={fecha_fin}&datasetId=ff027b"
    response = requests.get(url)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        content_type = response.headers['Content-Type']
    else:
       print("Error al descargar el archivo:", response.status_code)

    print("Error al descargar el archivo:", response.status_code)
    
    #Se crea la variable data para almacenar el archivo json descargado desde XM. Luego se toman los datos "records" del campo
    #results y se crea, finalmente, el dataframe que contiene la información requeridad de compromisos de despacho.
    data = response.json()
    records = data.get('result', {}).get('records', [])
    dfCompriso = pd.DataFrame(records)        

    # Capacidad Generadores

    #En este apartado se hace el simil de descarga de la capacidad de despacho desde el generador(es) y se crea el dataframe de capacidad
    url = f"https://drive.google.com/uc?id={file_id}"
    response = requests.get(url)
    dfCapacidad = pd.read_excel(BytesIO(response.content), header=5)



    # PASO 2: TRANSFORMACIONES

    """
    En este apartado se realizará las acciones de limpieza, eliminación de datos nulos, eliminación de datos no relevantes y las adecuaciones necesarias para poder hacer operaciones conjuntas necesarias entre los diferentes datasets (dataframes)

    Se inicia con la homologación de nombres de columnas para los dataframes:

    dfPreciosBolda:
    -fecha: Fecha

    dfDespachoUnidades:
    -FechaHora: Fecha
    -Valor: Compromiso(Kwh)

    dfArchivoCapacidad:
    -CODIGO: CodigoPlanta

    """
    dfPreciosBolsa = dfPreciosBolsa.rename(columns={
        'fecha': 'Fecha'
    })

    dfCompriso = dfCompriso.rename(columns={
        'FechaHora': 'Fecha',
        'Valor': 'Compromiso (Kwh)'
    })

    dfCapacidad = dfCapacidad.rename(columns={
        'CODIGO': 'CodigoPlanta',
        'FECHA': 'Fecha',
        'CAPACIDAD (Kwh)': 'Capacidad (Kwh)'
    })

    #Se elimina columna en dfArchivo capacidad que no tiene utilidad:
    dfCapacidad = dfCapacidad.drop(columns=['Unnamed: 0'])

    # Se eliminan valores nulos:
    dfPreciosBolsa.dropna();
    dfCompriso.dropna();
    dfCapacidad.dropna();

    #Se homologan los fomatos de fecha:
    dfPreciosBolsa["Fecha"] = pd.to_datetime(dfPreciosBolsa["Fecha"])
    dfCompriso["Fecha"] = pd.to_datetime(dfCompriso["Fecha"])
    dfCapacidad["Fecha"] = pd.to_datetime(dfCapacidad["Fecha"])

    #Se seleccionan los generadores correspondientes  al comercializadora ACME SAS
    codigos_filtrar = ['ZPA2', 'ZPA3', 'ZPA4', 'ZPA5', 'GVIO', 'QUI1', 'CHVR']
    dfCompromisoFiltrado = dfCompriso[dfCompriso['CodigoPlanta'].isin(codigos_filtrar)]
    dfCapacidadFiltrado = dfCapacidad[dfCapacidad['CodigoPlanta'].isin(codigos_filtrar)]

    """
    Se continua con la consolidación, en los dataframes, por fecha y unidad generadora (CodigoPlanta) y se totalizan las capacidades y los compromisos:
    """
    dfCapacidadFiltrado.loc[:, 'Fecha'] = dfCapacidadFiltrado['Fecha'].dt.date # se usa .loc para evitar el warning
    dfCompromisoFiltrado.loc[:, 'Fecha'] = dfCompromisoFiltrado['Fecha'].dt.date # se usa .loc para evitar el warning

    dfCapacidadAgrupado = dfCapacidadFiltrado.groupby(['CodigoPlanta', 'Fecha'])['Capacidad (Kwh)'].sum().reset_index()
    dfCompromisoAgrupado = dfCompromisoFiltrado.groupby(['CodigoPlanta', 'Fecha'])['Compromiso (Kwh)'].sum().reset_index()


    #Se realiza a continuación el balance entre capacidad y compromiso:
    dfBalance = pd.merge(dfCapacidadAgrupado, dfCompromisoAgrupado, on=['CodigoPlanta', 'Fecha'], how='left')
    dfBalance['Compromiso (Kwh)'] = dfBalance['Compromiso (Kwh)'].dropna()


    #sE crea una nueva columna: 'Consolidado Planta' restando las capacidades
    dfBalance['Consolidado (Kwh)'] = dfBalance['Capacidad (Kwh)'] - dfBalance['Compromiso (Kwh)']
    dfBalance["Fecha"] = pd.to_datetime(dfBalance["Fecha"]) #se convierte a tipo date para la operación que sigue


    #Ahora se hace el balance valorizando la consolidación:
    dfPreciosFiltrado = dfPreciosBolsa[(dfPreciosBolsa['CodigoVariable'] == 'PPBOGReal') & (dfPreciosBolsa['Version'] == 'TXR')]

    # Se hace el merge de dfBalance con dfPreciosFiltrado en la columna 'Fecha'
    dfBalanceConPrecios = pd.merge(dfBalance, dfPreciosFiltrado[['Fecha', 'Valor']], on='Fecha', how='left')

    #Se crea una nueva columna 'Compromisos_MCOP' multiplicando 'Consolidado Planta' x 'Valor'
    dfBalanceConPrecios['Compromisos_MMCOP'] = dfBalanceConPrecios['Consolidado (Kwh)'] * dfBalanceConPrecios['Valor']/1000000


    # Finalmente, se da la indicación explicita de compra o venta de energía:
    dfBalanceConPrecios['Operación'] = dfBalanceConPrecios['Compromisos_MMCOP'].apply(lambda x: 'Compra' if x < 0 else 'Vender')

    #Se redondea a dos decimales los valores
    dfBalanceConPrecios['Valor'] = dfBalanceConPrecios['Valor'].round(2)
    dfBalanceConPrecios['Compromisos_MMCOP'] = dfBalanceConPrecios['Compromisos_MMCOP'].round(2)


    """
    Ahora, previo a la etapa de carga de la información, se crea el archivo csv que será transferido
    """
    # Este archivo no se descargará. se creará en memoria para la operación de carga
    csv_buffer = io.StringIO()
    dfBalanceConPrecios.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Se visualiza a continuación el contenido
    #print(csv_data)


    # PASO 3: CARGA (TRANSFERENCIA A XM)

    #Se emplean las instrucciones para hacer la carga de la información mediante API

    file_upload_access_token = ""
    file_upload_account_id = ""
    params = {'key1': api_key_1, 'key2': api_key_2}
    response=requests.get("https://fastupload.io/api/v2/authorize", params)
    json_response = json.loads(response.text)
    try:
        file_upload_access_token = json_response["data"]["access_token"]
        file_upload_account_id = json_response["data"]["account_id"]
    except:
        print("Error autenticando y autorizando en el servicio remoto de carga de archivos.")



    upload_folder_id = ""  # Si tienes un folder ID
    # Crear el archivo en memoria a partir de 'csv_data'
    csv_in_memory = io.BytesIO(csv_data.encode('utf-8'))  # Convertir csv_data a bytes

    # Configurar los archivos a subir
    files = {"upload_file": ("dfReporteCompraVentaEnergiaAcme.csv", csv_in_memory, 'text/csv')}  # Nombrar el archivo

    # Datos para la solicitud
    data = {
        "access_token": file_upload_access_token,
        "account_id": file_upload_account_id,
        "folder_id": upload_folder_id  # Agregar folder_id si es necesario
    }

    # Enviar la solicitud POST a la API
    response = requests.post("https://fastupload.io/api/v2/file/upload", files=files, data=data)

    #Se crea variable para almacenar respuesta de la API en formato json:
    json_response = response.json()

    #se extrae solo _status y _datetime
    status = json_response.get('_status')
    datetime = json_response.get('_datetime')

    #se muestra el status de la operación de carga
    #print(f"Status: {status}")
    #print(f"Datetime: {datetime}")
    
    #Se devuelven los resultados como Http
    return jsonify({
        "Estatus": status,
        "Fecha y Hora Registro": datetime,
        "CSV Data": csv_data
    })

if __name__ == '__main__':
    app.run(host= "0.0.0.0", port=8080)






