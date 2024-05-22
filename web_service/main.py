from flask import Flask, jsonify, request
from tables import insert_into_table, create_table, add_column_to_table, find_user, change_first_n_rows
import asyncio

app = Flask(__name__)


@app.route('/user/get/<string:id>', methods = ['GET'])
def get_user(id):
    first_name, second_name = find_user("people", id)

    if first_name != None and second_name != None:
        response = {
            "success": 1,
            "data": {
                "first_name": first_name,
                "last_name": second_name
            }
        }
        return jsonify(response), 200, {"Content-Type": "application/json; charset=utf-8"}
    else:
        return jsonify({"error": "User not found"}), 404



@app.route('/user/create', methods = ['POST'])
def add_user():
    if not request.json or not 'first_name' in request.json or not 'last_name' in request.json or not 'amount' in request.json:
        return jsonify({
            "success": 0,
            "error": {
                "message": "error message"
            }
        }), 400

    first_name = request.json['first_name']
    last_name = request.json['last_name']
    amount = request.json['amount']

    insert_into_table("people", first_name, last_name, amount)

    data = {
            "meta": request.json['meta'],
            "first_name": first_name,
            "last_name": last_name,
            "amount": amount
    }

    return jsonify({"success": 1, "data": data}), 201

@app.route('/amount/calculate', methods = ['GET'])
def amount_calculate():
    result = asyncio.run(change_first_n_rows("people", 20))
    response = {
        "success": 1,
        "data" : {
            "result":  result
        }
    }
    return jsonify(response), 200, {"Content-Type": "application/json; charset=utf-8"}

if __name__ == '__main__':
    app.run(host='127.0.0.1', port='8000')