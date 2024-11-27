from concurrent.futures import ThreadPoolExecutor
import datetime
import pandas as pd
import util
from unicodedata import normalize
import re


def verificar_existe(row, index, retorno_banco, df):
    # Realizar o split na string usando "_"
    try:
        valores = row['NOM_DOC_PDF'].split('_')
    except Exception as e:
        print(valores)

    # Verificar se o valor resultante do split existe no retorno do banco de dados
    if valores[0] in retorno_banco:
        # Preencher a coluna "DELETADO" com True
        df.at[index, 'DELETADO'] = True


def rotina_deletados_apartir_da_base_recebida():
    sql3 = "SELECT DISTINCT id_arquivo FROM documentos_peca  " \
           "where id_sistema_peca in (SELECT id_sistema_pecas FROM pecas_excluidas_arteria )"

    deletados = util.exec_sql_return_banco_novo(sql3)

    # Ler o arquivo XLSX
    df = pd.read_excel('seu_arquivo_parte2.xlsx', sheet_name='Sheet1')

    # Simulação do retorno do banco de dados
    retorno_banco = [str(x["id_arquivo"]) for x in deletados]

    # Percorrer as linhas da planilha
    with ThreadPoolExecutor() as executor:
        for index, row in df.iterrows():
            executor.submit(verificar_existe, index, row, retorno_banco, df)

    # Salvar as alterações no arquivo XLSX
    df.to_excel('seu_arquivo_parte2_t.xlsx', index=False)


# Função para preencher o arquivo Excel
def preencher_excel(dados, nome_arquivo):
    # Criar um DataFrame com os dados
    df = pd.DataFrame()
    df['SCPJUD'] = dados['SCPJUD']
    df['Nome_doc'] = dados['NOME_DOC']
    # df = pd.DataFrame.from_dict(dados, orient='index')

    # Transpor o DataFrame
    # df = df.transpose()

    # Salvar o DataFrame no arquivo Excel
    df.to_excel(f'Y:/ENVIAR/GECRE/{nome_arquivo}', index=False)


def remover_acentos(txt):
    return normalize("NFKD", txt).encode("ASCII", "ignore").decode("ASCII")


def tratar_texto(txt):
    return re.sub(r"[^a-zA-Z0-9]", "", remover_acentos(txt[:-4].upper()))


def enviados_no_banco():
    sql3 = "SELECT p.scpjud,doc_nome.id_arquivo,doc_nome.nome_arquivo " \
           "FROM dbo.documentos_peca_com_nome  doc_nome " \
           "INNER JOIN dbo.documentos_peca peca ON peca.id_arquivo = doc_nome.id_arquivo " \
           "INNER JOIN dbo.pecas p ON p.id_sistema_peca = doc_nome.id_sistema_peca " \
           "WHERE peca.status = 'MIGRADO' AND peca.id_sistema_peca IN " \
           f"(SELECT id_sistema_pecas FROM pecas_excluidas_arteria " \
           f"WHERE created_at > '{datetime.datetime.now().strftime('%Y-%m-%d')}')"

    deletados = util.exec_sql_return_banco_novo(sql3)
    # Exemplo de dados retornados de uma consulta (dicionários)
    aux = {'SCPJUD': [], 'NOME_DOC': []}
    for deletado in deletados:
        tipo = deletado['nome_arquivo'].split(".")
        aux['SCPJUD'].append(deletado['scpjud'])
        aux['NOME_DOC_EXCLUIDO'].append(
            str(deletado['id_arquivo']) + '_' + tratar_texto(deletado['nome_arquivo'])[:54] + '.' + tipo[len(tipo) - 1])

    # Nome do arquivo Excel a ser criado
    nome_arquivo = f"dados_deletados_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx"

    # Chamar a função para preencher o arquivo Excel
    preencher_excel(aux, nome_arquivo)

    print(f"Arquivo '{nome_arquivo}' criado com sucesso!")


def enviados_no_banco_deletados():
    dia = datetime.datetime.now() - datetime.timedelta(days=7)
    sql3 = "SELECT p.scpjud, doc_nome.id_arquivo, doc_nome.nome_arquivo " \
           "FROM dbo.documentos_peca_com_nome  doc_nome " \
           "INNER JOIN dbo.documentos_peca peca ON peca.id_arquivo = doc_nome.id_arquivo " \
           "INNER JOIN dbo.pecas p ON p.id_sistema_peca = doc_nome.id_sistema_peca " \
           "WHERE peca.status = 'MIGRADO' AND peca.id_sistema_peca IN " \
           f"(SELECT id_sistema_pecas FROM pecas_excluidas_arteria " \
           f"WHERE created_at > '{dia.strftime('%Y-%m-%d')}')"

    sql3 = "SELECT p.scpjud, doc_nome.id_arquivo, doc_nome.nome_arquivo " \
           "FROM dbo.documentos_peca_com_nome  doc_nome " \
           "INNER JOIN dbo.documentos_peca peca ON peca.id_arquivo = doc_nome.id_arquivo " \
           "INNER JOIN dbo.pecas p ON p.id_sistema_peca = doc_nome.id_sistema_peca " \
           "WHERE peca.status = 'MIGRADO' AND peca.id_sistema_peca IN " \
           "(SELECT id_sistema_pecas FROM pecas_excluidas_arteria ) AND " \
           "(p.nome_cliente = 'CSH' or p.nome_cliente = 'CVP' OR p.nome_cliente = 'CAIXA SEGURADORA')"

    deletados = util.exec_sql_return_banco_novo(sql3)
    # Exemplo de dados retornados de uma consulta (dicionários)
    aux = {'SCPJUD': [], 'NOME_DOC': []}
    for deletado in deletados:
        tipo = deletado['nome_arquivo'].split(".")
        aux['SCPJUD'].append(deletado['scpjud'])
        aux['NOME_DOC'].append(
            str(deletado['id_arquivo']) + '_' + tratar_texto(deletado['nome_arquivo'])[:54] + '.' + tipo[len(tipo) - 1])

    # Nome do arquivo Excel a ser criado
    nome_arquivo = f"dados_deletados_{datetime.datetime.now().strftime('%Y_%m_%d')}.xlsx"

    # Chamar a função para preencher o arquivo Excel
    preencher_excel(aux, nome_arquivo)

    print(f"Arquivo '{nome_arquivo}' criado com sucesso!")


def verificar_existe_excel(row, index, retorno_banco, df):
    # Realizar o split na string usando "_"
    try:
        valores = row['NOM_DOC_PDF'].split('_')
    except Exception as e:
        print(valores)

    # Verificar se o valor resultante do split existe no retorno do banco de dados
    if valores[0] not in retorno_banco:
        # Preencher a coluna "DELETADO" com True
        df.at[index, 'DELETADO'] = True


def rotina_deletados_apartir_da_base_recebida2():
    sql3 = "SELECT DISTINCT(id_arquivo) " \
           "FROM dbo.documentos_peca WHERE status = 'MIGRADO'" \
           "AND id_sistema_peca NOT IN " \
           "(SELECT id_sistema_pecas FROM dbo.pecas_excluidas_arteria)"

    existentes = util.exec_sql_return_banco_novo(sql3)

    existentes = [str(x['id_arquivo']) for x in existentes]

    # Ler o arquivo XLSX
    df = pd.read_excel('seu_arquivo_parte1.xlsx', sheet_name='Sheet1')
    print("Inicio da triagem")
    # Percorrer as linhas da planilha
    with ThreadPoolExecutor(max_workers=20) as executor:
        for index, row in df.iterrows():
            executor.submit(verificar_existe_excel, row, index, existentes, df)

    # Salvar as alterações no arquivo XLSX
    df.to_excel('seu_arquivo_parte1_t.xlsx', index=False)


enviados_no_banco_deletados()
