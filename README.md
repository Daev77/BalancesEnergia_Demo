# Balances de Energía ACME SAS

Para correr la aplicación:

https://demo-app-229092683425.us-central1.run.app/

Tener presente que el servicio puede presentar, por la configuración dada en la plataforma de GCR, un cold start y, por lo tanto, puede tomar un tiempo mayor para desplegarse.

Para recrear el programa

En google drive crear una carpeta y cargar el archivo DespachoGeneradoraX.xlsx y generar id para poder descargarlo por medio de API

En FastUpload crear usuario y generar API keys para poder auntenticarse en la plataforma y subir el reporte final

```sh
git clone
cd Balance
source env/bin/activate
pip3 install -r requirements.txt
python3 balance.py

```

