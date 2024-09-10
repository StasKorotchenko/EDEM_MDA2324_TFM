# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar los archivos de requerimientos y el script de Python al contenedor
COPY requirements.txt requirements.txt
COPY app.py app.py

# Copiar el archivo de credenciales de servicio
COPY tfm-edem-ec8bf5197ad5.json tfm-edem-ec8bf5197ad5.json

# Instalar las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Establecer el punto de entrada para ejecutar el script
ENTRYPOINT ["python", "app.py"]
