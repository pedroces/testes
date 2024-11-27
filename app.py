
import util
from flask import Flask, Response, jsonify, request
from werkzeug.datastructures import Headers
from functools import wraps
import datetime
import jwt


app = Flask(__name__)
app.debug = True


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'token' in request.headers:
            token = request.headers['token']

        if not token:
            return jsonify({'Mensagem': 'Token não enviado!'}), 401

        try:
            dados = jwt.decode(token, util.os.environ.get('private_key'), algorithms=['HS256'])

            unix_time_now = int(datetime.datetime.now().timestamp())

            if dados['exp'] < unix_time_now:
                return jsonify({'Mensagem': 'Token expirado!'}), 401
        except Exception as e:
            return jsonify({'Mensagem': 'Token invalido!'}), 401

        return f(*args, **kwargs)

    return decorated


@app.route('/auth', methods=['GET'])
def auth():
    headers_env = request.headers

    headers = Headers(headers_env)

    username = headers.get('Client-Id')

    passwd = headers.get('Client-Secret')

    if username and passwd:

        if username != '' and passwd != '':

            verifica = verifica_usuario_banco(username, passwd)

            if verifica is True:

                payload = {'client_id': username,
                           'client_secret': passwd,
                           'exp': int((datetime.datetime.utcnow() + datetime.timedelta(minutes=30)).timestamp())}

                encode = jwt.encode(payload, util.os.environ.get('private_key'), algorithm='HS256')

                return jsonify({'Mensagem': 'validado com sucesso', 'token': encode})
            else:
                return jsonify({'Mensagem': 'login ou senha invalidos'})
        else:
            return jsonify({'Mensagem': 'client_id e client_secret nao podem ser vazios'})
    else:
        return jsonify({'Mensagem': 'client_id e client_secret sao valores obrigatorios'})


@app.route("/receber_id_hcp", methods=['POST'])
@token_required
def hcp():
    if not request.json:
        return jsonify({'Mensagem': 'O Json nao podem ser vazios'}), 500
    data = request.get_json(force=True)
    if 'dataArmazenanto' not in data:
        return jsonify({'Mensagem': 'O valor "dataArmazenanto" nao podem ser vazios'}), 500
    if 'situacao' not in data:
        return jsonify({'Mensagem': 'O valor "situacao" nao podem ser vazios'}), 500
    if 'id-hcp' not in data:
        return jsonify({'Mensagem': 'O valor "id-hcp" nao podem ser vazios'}), 500
    if 'numscpjud' not in data:
        return jsonify({'Mensagem': 'O valor "numscpjud" nao podem ser vazios'}), 500
    if 'nomarquivo' not in data:
        return jsonify({'Mensagem': 'O valor "nomarquivo" nao podem ser vazios'}), 500
    if 'id_registro' not in data:
        return jsonify({'Mensagem': 'O valor "id_registro" nao podem ser vazios'}), 500

    salvar_id_hcp(data['id_registro'], data['id-hcp'])

    return jsonify({'Mensagem': 'Transação realizada com sucesso.'}), 200


def verifica_usuario_banco(client_id, client_secret):
    sql = "SELECT * FROM client " \
          "WHERE client_id = '{}' AND client_secret ='{}'".format(client_id, client_secret)
    dados_banco = util.exec_sql_return(sql)

    if len(dados_banco) > 0:
        return True
    else:
        return False


def salvar_id_hcp(id_registro, id_hcp):
    sql = f"UPDATE pecas SET id_hcp='{id_hcp}' WHERE id_sistema_peca = {id_registro}"
    util.exec_sql(sql)




if __name__ == '__main__':
    app.run()



