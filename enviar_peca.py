import util
import concurrent.futures
from datetime import datetime
from time import sleep
from zeep.transports import Transport
from leitura_pecas import leitura_diaria_base, xml_migracao_peca, leitura_diaria_base_novo


def criar_metadado_xml(id_arteria, num_scpjud, num_sinistro,
                       ramo, cod_produto, nome_produto,
                       tipo_de_peca, cpf_cnpj, nome_arquivo_semextencao,
        tipo, folder
                       ):
    xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <metadados-documento>
        <unid-org>DIROC</unid-org>
        <num-scpjud>{num_scpjud}</num-scpjud>
        <num-sinistro>{num_sinistro}</num-sinistro>
        <ramo>{ramo}</ramo>
        <cod-produto>{cod_produto}</cod-produto>
        <nome-produto>{nome_produto}</nome-produto>
        <num-proposta></num-proposta>
        <id-origem>{id_arteria}</id-origem>
        <sistema-origem>ARTERIA</sistema-origem>
        <nome-empresa-parc></nome-empresa-parc>
        <cpf-cnpj>{cpf_cnpj}</cpf-cnpj>
        <tipo-documento>DCJ</tipo-documento>
        <subtipo-documento>{tipo_de_peca}</subtipo-documento>
        <num-contrato></num-contrato>
        <num-endosso></num-endosso>
        <documento>
            <nome-arquivo>{nome_arquivo_semextencao[:54]}.{tipo}</nome-arquivo>
        </documento>
    </metadados-documento>'''

    f = open(f'{folder}METADATA/{nome_arquivo_semextencao[:54]}.xml', 'w', encoding='utf-8')
    # f = open('Y:/ENVIAR/ARTERIA/METADATA/{}.xml'.format(nome_arquivo_semextencao[:54]), 'w', encoding='utf-8')
    # f = open('E:/migracao/metadado/{}.xml'.format(nome_arquivo_semextencao[:54]), 'w', encoding='utf-8')
    f.write(xml)
    f.close()
    return 'fim'


# ajuste teria que criar um novo metadado
def enviar_pecas_xml(dado, folder):
    try:
        tipo_peca = tipo_doc_hcp(dado['tipo_peca'])
        if tipo_peca != '':
            extrair_doc_arteria(str(dado['id_arquivo']), folder, dado, tipo_peca)
            #extrair_doc_arteria(str(dado['id_arquivo']), 'E:/migracao/documento/', dado, tipo_peca)
        else:
            raise Exception('tipo_peca_vazio')
    except Exception as e:
        status_migracao_erro(dado['id_arquivo'], str(e))
        pass
        # util.mensagens('Registro id: ' + dado['id_sistema_peca'] + ' deu o seguinte erro: ' + str(e), 'warning')


def extrair_doc_arteria(fileId, folder, dado, tipo_peca):
    wsdl = r'C:\Users\Administrator\Desktop\migracao_peca2\migracao_peca2\wsdl-arteria.xml'
    settings = util.Settings(strict=False, xml_huge_tree=True)
    client = util.Client(wsdl=wsdl, settings=settings)

    # Para Debug
    # settings = Settings(strict=False, xml_huge_tree=True)
    # session = Session()
    # session.proxies = {'http': 'http://127.0.0.1:8888', 'https': 'http://127.0.0.1:8888'}
    # session.verify = "C:\\Users\\User\\Documents\\charles.pem"
    # client = Client(wsdl=wsdl, transport=Transport(session=session, timeout=100), settings=settings)

    search = client.service.GetAttachmentFile(sessionToken=f"{util.get_token()}", fileId=fileId)
    dict_search = util.xmltodict.parse(search)
    if dict_search['files'] is None:
        raise Exception('no_file')

    if '#text' not in dict_search['files']['file']:
        raise Exception('no_file')

    bytes = util.b64decode(dict_search['files']['file']['#text'], validate=True)

    aux = dict_search['files']['file']['@name'].split(".")
    tipo = aux[len(aux) - 1]

    # if bytes[0:4] != b'%PDF':
    #    raise Exception(tipo)

    nome_arquivo = str(fileId) + '_' + util.tratar_texto(dict_search['files']['file']['@name'])

    criar_metadado_xml(
        dado['id_sistema_peca'],
        dado['scpjud'],
        dado['sinistro'],
        int(dado['ramo'].split("-")[0].strip()),
        int(dado['produto'].split("-")[0].strip()),
        dado['produto'].split("-")[1].lstrip(),
        tipo_peca,
        dado['cpf_cnpj'],
        nome_arquivo,
        tipo,
        folder
    )

    f = open(f'{folder}DATA/{nome_arquivo[:54]}.{tipo}', 'wb')
    f.write(bytes)
    f.close()
    status_migracao(dado['id_arquivo'])
    return nome_arquivo


def tipo_doc_hcp(tipo):
    query = f"SELECT TOP 1 cod_subtipo FROM dbo.tipo_de_documento_hcp hcp INNER JOIN dbo.tipo_hcp_arteria thcp ON " \
            f"thcp.id_tipo_de_documento_hcp = hcp.id_hcp INNER JOIN dbo.tipo_de_peca_arteria pa ON pa.id = " \
            f"thcp.id_tipo_de_peca_arteria WHERE pa.nome = '{tipo}' AND hcp.cod_subtipo IS NOT NULL ORDER BY id_hcp ASC "

    pesquisa = util.exec_sql_return_banco_novo(query)
    if len(pesquisa) > 0:
        return pesquisa[0]['cod_subtipo']
    else:
        return ''


def status_migracao(id_arquivo):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))
    cursor = conx.cursor(as_dict=True)

    sql = f"UPDATE documentos_peca SET status = 'MIGRADO' WHERE id_arquivo = {id_arquivo}"
    cursor.execute(sql)
    conx.commit()
    conx.close()
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))
    cursor = conx.cursor(as_dict=True)

    sql2 = f"UPDATE documentos_peca_com_nome SET status_migracao = 'MIGRADO' WHERE id_arquivo = {id_arquivo}"
    cursor.execute(sql2)
    conx.commit()
    conx.close()


def status_migracao_erro(id_arquivo, tipo):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))
    cursor = conx.cursor(as_dict=True)

    sql = f"UPDATE documentos_peca SET status = '{tipo.upper()}' WHERE id_arquivo = {id_arquivo}"
    cursor.execute(sql)
    conx.commit()
    conx.close()


def rotina_envio_csh_arquivos():
    query = "SELECT TOP 4000 pecas.id_sistema_peca, pecas.scpjud, pecas.tipo_peca, " \
            "pecas.diretoria, pecas.ramo, pecas.produto, pecas.cpf_cnpj," \
            "pecas.nome_cliente, pecas.sinistro, " \
            "documentos_peca.id_arquivo , documentos_peca.status " \
            "FROM pecas " \
            "INNER JOIN documentos_peca " \
            "ON documentos_peca.id_sistema_peca = pecas.id_sistema_peca " \
            "WHERE (pecas.nome_cliente = 'CSH' or pecas.nome_cliente = 'CVP') and " \
            "(documentos_peca.status = 'NULL' or " \
            "documentos_peca.status = '' or " \
            "documentos_peca.status = 'INVALID XML CONTENT RECEIVED (NONE)')"

    pesquisa = util.exec_sql_return_banco_novo(query)

    util.mensagens(f"Quantidade de arquivos : {len(pesquisa)}",
                   'ok', bold=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for dados in pesquisa:
            futures.append(executor.submit(enviar_pecas_xml, dado=dados, folder='Y:/ENVIAR/ARTERIA/'))


def rotina_envio_cnp_arquivos():
    query = "SELECT TOP 4000 pecas.id_sistema_peca, pecas.created_at,pecas.scpjud, pecas.tipo_peca, " \
            "pecas.diretoria, pecas.ramo, pecas.produto, pecas.cpf_cnpj, " \
            "pecas.nome_cliente, pecas.sinistro, " \
            "documentos_peca.id_arquivo , documentos_peca.status " \
            "FROM pecas " \
            "INNER JOIN documentos_peca " \
            "ON documentos_peca.id_sistema_peca = pecas.id_sistema_peca " \
            "WHERE pecas.nome_cliente = 'CNP Seguradora' AND " \
            "(documentos_peca.status = 'NULL' or documentos_peca.status IS NULL or " \
            "documentos_peca.status = '' or " \
            "documentos_peca.status = 'INVALID XML CONTENT RECEIVED (NONE)') ORDER BY pecas.created_at DESC"

    pesquisa = util.exec_sql_return_banco_novo(query)

    util.mensagens(f"Quantidade de arquivos : {len(pesquisa)}",
                   'ok', bold=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for dados in pesquisa:
            futures.append(executor.submit(enviar_pecas_xml, dado=dados, folder='X:/ENVIAR/ARTERIA/'))



# TOTAL NO CONNECT 1 ONDA 177945
inicio = util.time()

util.mensagens(f"Inicio da rotina de migração: {datetime.now()}",
               'ok', bold=True)

leitura_diaria_base_novo(xml_migracao_peca)

util.mensagens(f"Fim da Leitura do relatorio : {datetime.now()}",
               'ok', bold=True)
util.mensagens(f"Inicio do envio dos arquivos : {datetime.now()}",
               'ok', bold=True)

rotina_envio_csh_arquivos()

rotina_envio_cnp_arquivos()

fim = util.time()

util.mensagens(f"Fim do envio em : {util.cronometro(fim - inicio)}",
               'ok', bold=True)

util.mensagens(f"Finalizou : {datetime.now()}",
               'ok', bold=True)




