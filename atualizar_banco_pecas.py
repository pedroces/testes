import os
import logging
import pymssql
import datetime as dt
import threading
from datetime import datetime
from dotenv import load_dotenv
from funcoes_arteria import search
from relatorios import relatorio_subs
from concurrent.futures import ThreadPoolExecutor

load_dotenv('.env')

def setup_logging():
    """
    Configura o logging para registrar mensagens de info e erro.
    """
    now = dt.datetime.now()
    log_directory = f'logs_Pecas_em_Subsidios/{now.strftime("%Y-%m-%d_%H-%M-%S") + " Pecas em Subsidios"}'
    os.makedirs(log_directory, mode=0o777, exist_ok=True)
    
    log_file_path = os.path.join(log_directory, f'pecas_em_subsidios.log')
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    return os.path.join(log_directory, f'pecas_atualizadas_em_subsidios.log')

def log_error(file_path, message):
    """
    Registra erros no arquivo especificado.
    """
    mode = 'a' if os.path.exists(file_path) else 'w'
    with open(file_path, mode) as file:
        if mode == 'w':
            file.write("Erro\n")
        file.write(f"Falha: {message}\n")

def log_update_attempt(file_path, id_sistema_peca, id_sistema_subs, success, message):
    """
    Registra uma tentativa de atualização no arquivo especificado.
    """
    try:
        mode = 'a' if os.path.exists(file_path) else 'w'
        with open(file_path, mode) as file:
            if mode == 'w':
                file.write("ID do Sistema Peça, ID do Sistema Subsídio, Status, Mensagem\n")
            status = 'Sucesso' if success else 'Falha'
            file.write(f"{id_sistema_peca}, {id_sistema_subs}, {status}, {message}\n")
    except Exception as erro:
        logging.error(f"Erro ao escrever no arquivo de tentativas de atualização: {erro}")
        log_error('erro_log.log', str(erro))

def executar_sql(sql):
    """
    Executa um comando SQL no banco de dados.
    """
    try:
        conexao = pymssql.connect(
            server=os.environ.get('db_server'),
            user=os.environ.get('db_username'),
            password=os.environ.get('db_password'),
            database=os.environ.get('database_integra')
        )
        with conexao.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    except Exception as erro:
        logging.error(f"Erro ao executar SQL: {erro}")
        raise
    finally:
        conexao.close()
        return result

def executar_sql_update(sql):
    """
    Executa um comando SQL no banco de dados.
    """
    conx = pymssql.connect(os.environ.get('db_server'),
                           os.environ.get('db_username'),
                           os.environ.get('db_password'),
                           os.environ.get('database_integra')
                           )

    cursor = conx.cursor()

    cursor.execute(sql)
    conx.commit()
    conx.close()

def salvar_execucao_peca(caminho_tentativas, id_sistema_subs, id_sistema_peca):
    """
    Atualiza informações no banco de dados na table de info_subsidio_pecas.
    """
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    sql_check_peca = f"""
        SELECT 1
        FROM dbo.info_subsidio_pecas
        WHERE id_sistema_peca = {id_sistema_peca}
    """
    
    sql_update_peca = f"""
        UPDATE dbo.info_subsidio_pecas
        SET id_sistema_peca = {id_sistema_peca},
            id_sistema_subsidio = {id_sistema_subs}, 
            updated_at = '{agora}'
        WHERE id_sistema_peca = {id_sistema_peca}
    """
    
    sql_insert_peca = f"""
        INSERT INTO dbo.info_subsidio_pecas
        (id_sistema_peca, id_sistema_subsidio, created_at)
        VALUES ({id_sistema_peca}, {id_sistema_subs}, '{agora}')
    """
    
    try:
        resultado = executar_sql(sql_check_peca)
        
        if resultado:
            executar_sql_update(sql_update_peca)
            logging.info(f"Registro com id_sistema_peca {id_sistema_peca} atualizado com sucesso.")
            log_update_attempt(caminho_tentativas, id_sistema_subs, id_sistema_peca, True, "Atualização bem-sucedida.")
        else:
            executar_sql_update(sql_insert_peca)
            logging.info(f"Registro com id_sistema_peca {id_sistema_peca} inserido com sucesso.")
            log_update_attempt(caminho_tentativas, id_sistema_subs, id_sistema_peca, True, "Inserção bem-sucedida.")
            
    except Exception as erro:
        logging.error(f"Erro ao salvar execução para ID {id_sistema_peca}.")
        log_update_attempt(caminho_tentativas, id_sistema_subs, id_sistema_peca, False, str(erro))
        return

def processar_arquivos(caminho_tentativas_pecas):
    """
    Processa os dados obtidos e registra os resultados.
    """
    try:
        dados_arteria = search(relatorio_subs)
        logging.info("Dados do Artéria buscados com sucesso.")
    except Exception as erro:
        logging.error(f"Erro ao buscar dados do Artéria: {erro}")
        log_error('erro_log.log', str(erro))
        return
    
    any_records_processed = False

    for dado in dados_arteria:
                id_sistema_subs = dado['ID do Sistema - Subsídio']
                pecas_processuais = dado.get('Peças Processuais', [])
                
                if pecas_processuais:
                    for peca in pecas_processuais:
                        id_sistema_peca = peca.get('ID do Sistema - Peças Processuais')
                        
                        if id_sistema_peca:
                            salvar_execucao_peca(caminho_tentativas_pecas,id_sistema_subs, id_sistema_peca)
                            any_records_processed = True
                else:
                    print(f"Subsídio {id_sistema_subs} não possui peças processuais.")
                    log_update_attempt(caminho_tentativas_pecas, 'Não possui peça',id_sistema_subs, False, "Subsidio não possui peças processuais" + str(erro))
                    log_error('erro_log.log',str(erro))
    if not any_records_processed:
        log_update_attempt(caminho_tentativas_pecas,'','',False, "Nenhum registro foi processado.")
    
    logging.info("Processamento concluído.")
    
def main():
    """
    Função principal que inicializa o processo.
    """
    
    caminho_tentativas_pecas = setup_logging()
    processar_arquivos(caminho_tentativas_pecas)
    

if __name__ == "__main__":
    thread = threading.Thread(target=main)
    thread.start()