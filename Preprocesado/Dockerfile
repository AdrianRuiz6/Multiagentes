FROM python:3.8.5-slim-buster



COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT python preprocesado.py