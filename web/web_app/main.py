"""Funcionalidad extra"""
# pylint: disable=W1203
import psycopg2
from flask import Flask, render_template

app = Flask(__name__)




@app.route('/')
def index():
    "Conexión a la bbdd"
    conn = psycopg2.connect (
    host="localhost",
    database="postgres",
    user="postgres",
    password="admin"
)

# Creamos un cursor para realizar operaciones en la base de datos
    cursor = conn.cursor()

# Realizamos una consulta a la base de datos
    cursor.execute("SELECT * FROM tabla_spain_limpio")

# Obtenemos los resultados de la consulta
    resultados = cursor.fetchall()


# Cerramos la conexión con la base de datos
    conn.close()
    return render_template('web.html', resultados=resultados)

if __name__ == '__main__':
    app.run(debug=True)

