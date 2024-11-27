import os
import logging
import pymssql
import datetime as dt
from datetime import datetime
from dotenv import load_dotenv
from funcoes_arteria import search
from relatorios import relatorio_subs
from concurrent.futures import ThreadPoolExecutor, as_completed

from relatorios import relatorio_subs

load_dotenv('.env')

def setup_logging():
    now = dt.datetime.now()
    log_directory = f'logs_Pecas_em_Subsidios/{now.strftime("%Y-%m-%d_%H-%M-%S") + " Pecas em Subsidios"}'
    os.makedirs(log_directory, mode=0o777, exist_ok=True)
    log_file_path = os.path.join(log_directory, f'pecas_em_subsidios.log')
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return os.path.join(log_directory, f'pecas_atualizadas_em_subsidios.log')

def executar_sql(conexao, sql):
    try:
        with conexao.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as erro:
        logging.error(f"Erro ao executar SQL: {erro}")
        raise

def executar_sql_update(conexao, sql):
    try:
        with conexao.cursor() as cursor:
            cursor.execute(sql)
        conexao.commit()
    except Exception as erro:
        logging.error(f"Erro ao executar SQL Update: {erro}")
        raise

def salvar_execucao_peca(caminho_tentativas, id_sistema_subs, id_sistema_peca, conexao):
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    sql_check_peca = f"SELECT 1 FROM dbo.info_subsidio_pecas WHERE id_sistema_peca = {id_sistema_peca}"
    sql_update_peca = f"UPDATE dbo.info_subsidio_pecas SET id_sistema_subsidio = {id_sistema_subs}, updated_at = '{agora}' WHERE id_sistema_peca = {id_sistema_peca}"
    sql_insert_peca = f"INSERT INTO dbo.info_subsidio_pecas (id_sistema_peca, id_sistema_subsidio, created_at) VALUES ({id_sistema_peca}, {id_sistema_subs}, '{agora}')"
    
    try:
        if executar_sql(conexao, sql_check_peca):
            executar_sql_update(conexao, sql_update_peca)
            logging.info(f"Registro {id_sistema_peca} atualizado.")
        else:
            executar_sql_update(conexao, sql_insert_peca)
            logging.info(f"Registro {id_sistema_peca} inserido.")
    except Exception as erro:
        logging.error(f"Erro ao processar peça {id_sistema_peca}: {erro}")

def processar_arquivos(caminho_tentativas_pecas, conexao, dados_arteria):
    with ThreadPoolExecutor(max_workers=10) as executor:  # Define quantas threads deseja usar
        futures = []
        for dado in dados_arteria:
            id_sistema_subs = dado['ID do Sistema - Subsídio']
            pecas_processuais = dado.get('Peças Processuais', [])
            
            for peca in pecas_processuais:
                id_sistema_peca = peca.get('ID do Sistema - Peças Processuais')
                if id_sistema_peca:
                    futures.append(
                        executor.submit(salvar_execucao_peca, caminho_tentativas_pecas, id_sistema_subs, id_sistema_peca, conexao)
                    )

        for future in as_completed(futures):
            future.result()  # Lança exceção, se houver

def main():
    caminho_tentativas_pecas = setup_logging()
    conexao = pymssql.connect(
        server=os.environ.get('db_server'),
        user=os.environ.get('db_username'),
        password=os.environ.get('db_password'),
        database=os.environ.get('database_integra')
    )

    try:
        dados_arteria = search(relatorio_subs)
        processar_arquivos(caminho_tentativas_pecas, conexao, dados_arteria)
    finally:
        conexao.close()
    
    logging.info("Processamento concluído.")

if __name__ == "__main__":
    main()
