# Establecemos la imagen base como Ubuntu
FROM ubuntu:latest

# Actualizamos los repositorios de Ubuntu
RUN apt-get update

# Instalamos python
RUN apt-get install -y python3 python3-pip

# Copiamos el archivo con las librerias necesarias a /code
COPY ./requirements.txt /code/requirements.txt

#Instalamos las librerias incluidas en el archivo 'requirements.txt'
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copiamos nuestro código fuente a /code/app
COPY ./extraccion_app /code/app

# Establecemos el directorio de trabajo del contenedor en /code
WORKDIR /code/app

# Ejecuta el comando que inicia la aplicacion.
ENTRYPOINT python3 Main.py