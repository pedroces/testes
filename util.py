from base64 import b64decode

from requests import Session
from zeep import Client, Settings, Transport
import xmltodict
from time import time
from unicodedata import normalize
import re
import pymssql
from dotenv import load_dotenv
import os


load_dotenv('.env')


# ==============================================================================
#   Funções do Banco de Dados
# ==============================================================================

def exec_sql_return_banco_novo(sql):
    conx = pymssql.connect(os.environ.get('db_server'),
                           os.environ.get('db_username'),
                           os.environ.get('db_password'),
                           os.environ.get('database_new')
                           )

    cursor = conx.cursor(as_dict=True)

    cursor.execute(sql)

    dados = cursor.fetchall()

    conx.close()

    return dados


def exec_sql_return(sql):
    conx = pymssql.connect(os.environ.get('db_server'),
                           os.environ.get('db_username'),
                           os.environ.get('db_password'),
                           os.environ.get('database')
                           )

    cursor = conx.cursor(as_dict=True)

    cursor.execute(sql)

    dados = cursor.fetchall()

    conx.close()

    return dados


def exec_sql(sql):
    conx = pymssql.connect(os.environ.get('db_server'),
                           os.environ.get('db_username'),
                           os.environ.get('db_password'),
                           os.environ.get('database')
                           )

    cursor = conx.cursor(as_dict=True)

    cursor.execute(sql)

    conx.commit()

    conx.close()


def exec_sql_integra(sql):
    conx = pymssql.connect(os.environ.get('db_server'),
                           os.environ.get('db_username'),
                           os.environ.get('db_password'),
                           os.environ.get('database_integra')
                           )

    cursor = conx.cursor(as_dict=True)

    cursor.execute(sql)

    dados = cursor.fetchall()

    conx.close()

    return dados


def get_token():
    sql = "SELECT token FROM archer_token WHERE users = 'integra.api'"
    result = exec_sql_integra(sql)
    return result[0]['token']


def exec_sql_integr4(sql, val):
    conx = pymssql.connect(os.environ.get('db_server'),
                           os.environ.get('db_username'),
                           os.environ.get('db_password'),
                           os.environ.get('database_new')
                           )

    cursor = conx.cursor(as_dict=True)

    cursor.execute(sql, val)
    conx.commit()
    conx.close()

    return True

# ==============================================================================
#   Funções do Banco de Dados
# ==============================================================================
# ==============================================================================
#   Funções da API do ARCHER
# ==============================================================================


def extrai_dados(campos, dado):
    lista = {}
    for key in campos.values():
        if key['guid'] == dado['@guid']:
            if dado['@type'] in ['1', '2', '3', '6', '21', '22']:
                if '#text' in dado:
                    lista[key['alias']] = dado['#text']
                else:
                    lista[key['alias']] = ''
            elif dado['@type'] in ['4']:
                if 'ListValues' in dado:
                    lista[key['alias']] = {}
                    if 'ListValue' in dado['ListValues']:
                        if '@id' in dado['ListValues']['ListValue']:
                            lista[key['alias']][0] = {}
                            lista[key['alias']][0]['id'] = dado['ListValues']['ListValue']['@id']
                            lista[key['alias']][0]['displayName'] = dado['ListValues']['ListValue']['@displayName']
                        else:
                            h = 0
                            for m in dado['ListValues']['ListValue']:
                                lista[key['alias']][h] = {}
                                lista[key['alias']][h]['id'] = m['@id']
                                lista[key['alias']][h]['displayName'] = m['@displayName']
                                h += 1
                    else:
                        lista[key['alias']] = "ListValue com mais de um valor selecionando."
                        print("ListValue com mais de um valor selecionando.")
                else:
                    lista[key['alias']] = ''
            elif dado['@type'] in ['9', '23']:
                lista[key["alias"]] = {}
                if 'Reference' in dado:
                    if '@id' in dado['Reference']:
                        lista[key["alias"]][0] = {}
                        lista[key['alias']][0]['id'] = dado['Reference']['@id']
                        lista[key['alias']][0]['texto'] = dado['Reference']['#text']
                    else:
                        cont = 0
                        for ref in dado['Reference']:
                            if '@contentId' in ref:
                                lista[key['alias']][cont] = {}
                                lista[key['alias']][cont]['id'] = ref['@id']
                                lista[key['alias']][cont]['texto'] = ref['#text']
                                cont += 1
            elif dado['@type'] in ['11']:
                lista[key['alias']] = {}
                if 'File' in dado:
                    if '@id' in dado['File']:
                        lista[key["alias"]][0] = {}
                        lista[key['alias']][0]['id'] = dado['File']['@id']
                        if '#text' in dado['File']:
                            lista[key['alias']][0]['nome'] = dado['File']['#text']
                        else:
                            lista[key['alias']][0]['nome'] = ''
                    else:
                        cont = 0
                        for d in dado['File']:
                            if '@id' in d:
                                lista[key['alias']][cont] = {}
                                lista[key['alias']][cont]['id'] = d['@id']
                                lista[key['alias']][cont]['nome'] = d['#text']
                                cont += 1
                            else:
                                lista[key['alias']] = ''
            elif dado['@type'] in ['24']:
                lista[key["alias"]] = {}
                if 'Subform' in dado:
                    if '@contentId' in dado['Subform']:
                        lista[key["alias"]][0] = {}
                        lista[key['alias']][0]['id'] = dado['Subform']['@contentId']
                        if '#text' in dado['Subform']['Field']:
                            lista[key['alias']][0]['texto'] = dado['Subform']['Field']['#text']
                        else:
                            for b in dado['Subform']['Field']:
                                d = extrai_dados(campos, b)
                                for chave, valor in d.items():
                                    lista[key['alias']][0][chave] = valor
                    else:
                        cont = 0
                        for sub in dado['Subform']:
                            if '@contentId' in sub:
                                lista[key['alias']][cont] = {}
                                lista[key['alias']][cont]['id'] = sub['@contentId']
                                if '#text' in sub['Field']:
                                    lista[key['alias']][cont]['texto'] = sub['Field']['#text']
                                else:
                                    for b in sub['Field']:
                                        d = extrai_dados(campos, b)
                                        for chave, valor in d.items():
                                            lista[key['alias']][cont][chave] = valor
                                cont += 1
            elif dado['@type'] in ['8']:
                lista[key["alias"]] = {}
                if 'Groups' in dado:
                    if dado['Groups'] is not None:
                        if 'Group' in dado['Groups']:
                            if '#text' in dado['Groups']['Group']:
                                lista[key["alias"]][0] = {}
                                lista[key['alias']][0]['id'] = dado['Groups']['Group']['@id']
                                lista[key['alias']][0]['name'] = dado['Groups']['Group']['#text']
                            else:
                                for group in dado['Groups']['Group']:
                                    m = 0
                                    if '#text' in group:
                                        lista[key["alias"]][m] = {}
                                        lista[key['alias']][m]['id'] = group['@id']
                                        lista[key['alias']][m]['name'] = group['#text']
                                        m += 1
                if 'Users' in dado:
                    if dado['Users'] is not None:
                        if 'User' in dado['Users']:
                            if '#text' in dado['Users']['User']:
                                lista[key["alias"]][0] = {}
                                lista[key['alias']][0]['id'] = dado['Users']['User']['@id']
                                lista[key['alias']][0]['name'] = dado['Users']['User']['#text']
                            else:
                                print(dado['Users'])
            else:
                print(dado)

            return lista


def tamanho_relatorio(search):
    from zeep.transports import Transport
    s = Session()
    s.verify = False
    transport = Transport(session=s)
    wsdl = 'https://arteria.costaesilvaadv.com.br/RSAarcher/ws/search.asmx?wsdl'
    settings = Settings(strict=False, xml_huge_tree=True)
    client = Client(wsdl=wsdl, settings=settings, transport=transport)

    search = search.replace("<PageSize>0</PageSize>", "<PageSize>1</PageSize>")

    search = client.service.ExecuteSearch(sessionToken=f"{get_token()}", searchOptions=search,
                                          pageNumber=1)
    dict_search = xmltodict.parse(search)
    return dict_search['Records']['@count']


def extrair_relatorio(search, page):
    wsdl = 'https://arteria.costaesilvaadv.com.br/RSAarcher/ws/search.asmx?wsdl'

    # Para Debug
    # settings = Settings(strict=False, xml_huge_tree=True)
    session = Session()
    session.verify = False
    # session.proxies = {'http': 'http://127.0.0.1:8888', 'https': 'http://127.0.0.1:8888'}
    settings = Settings(strict=False, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=Transport(session=session), settings=settings)
    # client = Client(wsdl=wsdl, settings=settings)

    inicio = time()

    search = client.service.ExecuteSearch(sessionToken=f"{get_token()}", searchOptions=search,
                                          pageNumber=page)
    fim = time()

    dict_search = xmltodict.parse(search)

    mensagens(f"{dict_search['Records']['@count']} registros encontrados em {cronometro(fim - inicio)}",
              'ok', bold=True)
    campos = {}
    cont_campo = 0
    for dado in dict_search['Records']['Metadata']['FieldDefinitions']['FieldDefinition']:
        campos[cont_campo] = {
            'guid': dado['@guid'],
            'alias': dado['@name']
        }
        cont_campo += 1

    records = []
    if 'Record' in dict_search['Records']['Record']:
        colunas = {}
        if 'Field' in dict_search['Records']['Record']:
            if '@guid' in dict_search['Records']['Record']['Field']:
                colunas.update(extrai_dados(campos, dict_search['Records']['Record']['Field']))
            else:
                for dado in dict_search['Records']['Record']['Field']:
                    colunas.update(extrai_dados(campos, dado))
        if 'Record' in dict_search['Records']['Record']:
            if type(dict_search['Records']['Record']['Record']) is list:
                for dado in dict_search['Records']['Record']['Record']:
                    if 'Field' in dado:
                        if '@guid' in dado['Field']:
                            colunas.update(extrai_dados(campos, dado['Field']))
                        else:
                            for d in dado['Field']:
                                colunas.update(extrai_dados(campos, d))
            else:
                if 'Field' in dict_search['Records']['Record']['Record']:
                    if '@guid' in dict_search['Records']['Record']['Record']['Field']:
                        colunas.update(extrai_dados(campos, dict_search['Records']['Record']['Record']['Field']))
                    else:
                        for dado in dict_search['Records']['Record']['Record']['Field']:
                            colunas.update(extrai_dados(campos, dado))
        records.append(colunas)
    else:
        if type(dict_search['Records']['Record']) is list:
            for register in dict_search['Records']['Record']:
                colunas = {}
                if 'Field' in register:
                    if '@guid' in register['Field']:
                        colunas.update(extrai_dados(campos, register['Field']))
                    else:
                        for d in register['Field']:
                            colunas.update(extrai_dados(campos, d))
                    # records.append(colunas)
                if 'Record' in register:
                    if 'Field' in register['Record']:
                        if '@guid' in register['Record']['Field']:
                            colunas.update(extrai_dados(campos, register['Record']['Field']))
                        else:
                            for d in register['Record']['Field']:
                                colunas.update(extrai_dados(campos, d))
                        # records.append(colunas)
                    else:
                        for dado in register['Record']:
                            if 'Field' in dado:
                                if '@guid' in dado['Field']:
                                    colunas.update(extrai_dados(campos, dado['Field']))
                                else:
                                    for d in dado['Field']:
                                        colunas.update(extrai_dados(campos, d))
                records.append(colunas)
        else:
            colunas = {}
            if 'Field' in dict_search['Records']['Record']:
                if '@guid' in dict_search['Records']['Record']['Field']:
                    colunas.update(extrai_dados(campos, dict_search['Records']['Record']['Field']))
                else:
                    for dado in dict_search['Records']['Record']['Field']:
                        colunas.update(extrai_dados(campos, dado))
                records.append(colunas)
    return records


def extrair_doc_arteria(fileId, folder):
    wsdl = r'C:\Users\User\PycharmProjects\migracao_peca2\wsdl-arteria.xml'
    settings = Settings(strict=False, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=Transport(timeout=15), settings=settings)

    # Para Debug
    # settings = Settings(strict=False, xml_huge_tree=True)
    # session = Session()
    # session.proxies = {'http': 'http://127.0.0.1:8888', 'https': 'http://127.0.0.1:8888'}
    # session.verify = "C:\\Users\\User\\Documents\\charles.pem"
    # client = Client(wsdl=wsdl, transport=Transport(session=session, timeout=100), settings=settings)

    search = client.service.GetAttachmentFile(sessionToken=f"{get_token()}", fileId=fileId)
    dict_search = xmltodict.parse(search)
    if dict_search['files'] is None:
        raise Exception('no_file')
    bytes = b64decode(dict_search['files']['file']['#text'], validate=True)

    aux = dict_search['files']['file']['@name'].split(".")
    tipo = aux[len(aux) - 1]

    if bytes[0:4] != b'%PDF':
        raise Exception(tipo)

    nome_arquivo = str(fileId) + '_' + tratar_texto(dict_search['files']['file']['@name'])

    f = open(f'{folder}/{nome_arquivo[:54]}.pdf', 'wb')
    f.write(bytes)
    f.close()

    return nome_arquivo


def update_peca_id_hcp(id_registro, id_hcp):
    return True


def get_atach_rest(atach_id, folder):
    endpoint = 'https://arteria.costaesilvaadv.com.br/RSAarcher/platformapi/core/content/attachment/' + atach_id
    s = Session()
    s.proxies = {'http': 'http://127.0.0.1:8888', 'https': 'http://127.0.0.1:8888'}
    s.verify = "C:\\Users\\User\\Documents\\charles.pem"
    s.headers = {'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8',
                 'Authorization': 'Archer session-id=' + get_token(),
                 'Content-Type': 'application/json',
                 'X-Http-Method-Override': 'GET'}
    result = s.post(endpoint).json()['RequestedObject']
    bytes = b64decode(result['AttachmentBytes'], validate=True)

    if bytes[0:4] != b'%PDF':
        raise Exception(f'O Arquivo Não e um PDF extensão : {bytes[0:4]}')

    nome_arquivo = atach_id + '_' + tratar_texto(result['AttachmentName'])

    f = open(f'{folder}/{nome_arquivo}.pdf', 'wb')
    f.write(bytes)
    f.close()

    return nome_arquivo
# ==============================================================================
#   Funções da API do ARCHER
# ==============================================================================

# ==============================================================================
#   Funções Gerais
# ==============================================================================

class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    COMMON = "\033[120m"
    fgGreen = "\033[32m"
    fgBrightGreen = "\033[32;1m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def cronometro(value):
    m, s = divmod(value, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)


def mensagens(mensagem, tipo, bold=False):
    if bold:
        mensagem = bcolors.BOLD + mensagem + bcolors.ENDC
    if tipo == "ok":
        print(bcolors.OKGREEN + mensagem + bcolors.ENDC)
    elif tipo == "ok2":
        print(bcolors.fgGreen + mensagem + bcolors.ENDC)
    elif tipo == "ok3":
        print(bcolors.OKBLUE + mensagem + bcolors.ENDC)
    elif tipo == "warning":
        print(bcolors.WARNING + mensagem + bcolors.ENDC)
    elif tipo == "fail":
        print(bcolors.FAIL + mensagem + bcolors.ENDC)
    elif tipo == "info":
        print(bcolors.COMMON + mensagem + bcolors.ENDC)


def remover_acentos(txt):
    return normalize("NFKD", txt).encode("ASCII", "ignore").decode("ASCII")


def tratar_texto(txt):
    return re.sub(r"[^a-zA-Z0-9]", "", remover_acentos(txt[:-4].upper()))

# ==============================================================================
#   Funções Gerais
# ==============================================================================
