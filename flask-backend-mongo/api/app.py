from flask import Flask, request, jsonify
from bson.json_util import dumps
from bson.objectid import ObjectId
import db
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

##Las evidencias de funcionamiento, se encuentran en el blog del curso.

## METODOS GET
@app.route("/")
def index():
    return "Buscador de pacientes y de EPS!"

#Retornar un paciente en especifico
@app.route("/pacientes/<code>", methods=['GET'])
def get_paciente(code):
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.pacientes
        response = app.response_class(
            response=dumps(salud.find_one({'cedula': int(code)})),
            status=200,
            mimetype='application/json'
        )
        return response
    finally:
        con.close()
        print("Connection closed")

#Retornar una eps en especifico
@app.route("/eps/<code>", methods=['GET'])
def get_eps(code):
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.eps
        response = app.response_class(
            response=dumps(salud.find_one({'name': str(code)})),
            status=200,
            mimetype='application/json'
        )
        return response
    finally:
        con.close()
        print("Connection closed")
##Retorna las eps que tengan asociada la ciudad
@app.route("/eps-ciudad/<code>", methods=['GET'])
def get_ciudad(code):
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.eps
        query = {"atencion": {"$elemMatch":{"$eq":str(code)}}}
        print(query)
        response = app.response_class(
            response=dumps(salud.find(query)),
            status=200,
            mimetype='application/json'
        )
        return response
    finally:
        con.close()
        print("Connection closed")

##Retorna los pacientes, relacionandos con una eps y los datos de la eps, relacionando
##Las dos colecciones

@app.route("/pacientes/eps/<code>", methods=['GET'])
def get_eps_del_paciente(code):
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.pacientes
        query =[
            {
                '$lookup': {
                    'from': 'eps',
                    'localField': 'afiliado',
                    'foreignField': 'name',
                    'as': 'Eps'
                }
            },
            {'$match' : {'Eps.name': str(code)}}
        ]
           ## "atencion": {"$elemMatch":{"$eq":str(code)}}}
        print(query)
        response = app.response_class(
            response=dumps(salud.aggregate(query)),
            status=200,
            mimetype='application/json'
        )
        return response
    finally:
        con.close()
        print("Connection closed")

#Retortar todos los pacientes en la base de datos
@app.route("/pacientes", methods=['GET'])
def get_pacientes():
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.pacientes
        response = app.response_class(
            response=dumps(
                salud.find()
            ),
            status=200,
            mimetype='application/json'
        )
        return response
    finally:
        con.close()
        print("Connection closed")

#Retortar todas las EPS en la base de datos
@app.route("/eps", methods=['GET'])
def get_epss():
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.eps
        response = app.response_class(
            response=dumps(
                salud.find()
            ),
            status=200,
            mimetype='application/json'
        )
        return response
    finally:
        con.close()
        print("Connection closed")


##Metodo POST
#Crea un paciente
@app.route("/pacientes", methods=['POST'])
def create_paciente():
    data = request.get_json()
    con = db.get_connection()
    dbSalud = con.dbBuscador  
    try:
        salud = dbSalud.pacientes
        salud.insert_one(data)
        return jsonify({"message":"OK"})
    finally:
        con.close()
        print("Connection closed")

#Crea una eps

@app.route("/eps", methods=['POST'])
def create_eps():
    data = request.get_json()
    con = db.get_connection()
    dbSalud = con.dbBuscador  
    try:
        salud = dbSalud.eps
        salud.insert_one(data)
        return jsonify({"message":"OK"})
    finally:
        con.close()
        print("Connection closed")


##Metodo PUT Para actualizar los datos de la base de datos
#Actualiza un paciente
@app.route("/pacientes/<code>", methods=['PUT'])
def update(code):
    data = request.get_json()
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.pacientes
        salud.replace_one(
            {'cedula': int(code)},
            data, True
        )
        return jsonify({"message":"OK"})
    finally:
        con.close()
        print("Connection closed")

##Metodo DELETE
#Elimina un pacinete
@app.route("/pacientes/<code>", methods=['DELETE'])
def delete_paciente(code):
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.pacientes
        salud.delete_one({'cedula': int(code)})
        return jsonify({"message":"OK"})
    finally:
        con.close()
        print("Connection closed")

#Elimina una eps
@app.route("/eps/<code>", methods=['DELETE'])
def delete_eps(code):
    con = db.get_connection()
    dbSalud = con.dbBuscador
    try:
        salud = dbSalud.eps
        salud.delete_one({'name': str(code)})
        return jsonify({"message":"OK"})
    finally:
        con.close()
        print("Connection closed")
