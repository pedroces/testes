import pandas as pd
import datetime as dt
import os
from funcoes_arteria import search
from relatorios import relatorio
import logging
import pymssql
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('.env')

def exec_sql_integra(sql):
    conx = pymssql.connect(os.environ.get('db_server'),
                           os.environ.get('db_username'),
                           os.environ.get('db_password'),
                           os.environ.get('database_integra')
                           )

    cursor = conx.cursor()

    cursor.execute(sql)
    conx.commit()
    conx.close()

    return True

def setup_logging():
    """
    Configura o logging para registrar mensagens de info e erro.
    """
    now = dt.datetime.now()
    log_directory = f'logs_Pecas/{now.strftime("%Y-%m-%d_%H-%M-%S") + " Pecas"}'
    os.makedirs(log_directory, mode=0o777, exist_ok=True)
    
    log_file_path = os.path.join(log_directory, 'processamento_peca.log')
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    return os.path.join(log_directory, 'pecas_atualizadas.log'), os.path.join(log_directory, 'erro_log.log')

def log_error(file_path, message):
    """
    Registra erros no arquivo especificado.
    """
    mode = 'a' if os.path.exists(file_path) else 'w'
    with open(file_path, mode) as file:
        if mode == 'w':
            file.write("Erro\n")
        file.write(f"Falha: {message}\n")

def salvar_execucao(id_sistema_peca,nome_cliente):
    """
    Salva no banco de dados
    """
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        sql = f"UPDATE dbo.pecas SET nome_cliente = '{nome_cliente}', updated_at = '{agora}' WHERE id_sistema_peca = {id_sistema_peca}"

        exec_sql_integra(sql)
        
        print("Nome do Cliente " + nome_cliente + " registrado com sucesso para a peça " + id_sistema_peca + "." )
    except Exception as e:
        print(f'Erro na tentativa de atualizar o id {id_sistema_peca} no banco de dados. Erro: {str(e)}')

def log_update_attempt(file_path, id_sistema_peca, nome_cliente, success, message):
    """
    Registra uma tentativa de atualização no arquivo especificado.
    """
    try:
        mode = 'a' if os.path.exists(file_path) else 'w'
        with open(file_path, mode) as file:
            if mode == 'w':
                file.write("ID do Sistema Peça, Nome do Cliente, Status, Mensagem\n")
            status = 'Sucesso' if success else 'Falha'
            file.write(f"{id_sistema_peca}, {nome_cliente}, {status}, {message}\n")
    except Exception as e:
        logging.error(f"Erro ao escrever no arquivo de tentativas de atualização: {e}")
        log_error('erro_log.log', False, str(e))

def process_files(update_attempts_file_path, error_log_path):
    """
    Processa os arquivos CSV, atualiza o Artéria e registra as tentativas.
    """

    try:
        dados_arteria = search(relatorio)

        logging.info("Dados do Artéria buscados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao buscar dados do Artéria: {e}")
        log_error(error_log_path, str(e))
        return
    
    any_records_processed = False
    
    for dado in dados_arteria:
        try:
            id_sistema_peca = dado['ID do Sistema - Peças Processuais']
            nome_cliente = dado['Processos'][0]['Nome do Cliente'][0]
            
            salvar_execucao(id_sistema_peca, nome_cliente)
            log_update_attempt(update_attempts_file_path, id_sistema_peca, nome_cliente, True, "Atualização bem-sucedida.")
            any_records_processed = True
        except Exception as e:
            logging.error(f"Erro ao atualizar o {id_sistema_peca} no Banco: {e}")
            log_update_attempt(update_attempts_file_path, id_sistema_peca, nome_cliente, False, str(e))
            log_error(error_log_path, str(e))
    if not any_records_processed:
        log_update_attempt(update_attempts_file_path, '', '', False, "Nenhum registro foi processado.")
    
    logging.info("Processamento concluído.")
 
def main():
    update_attempts_file_path, error_log_path = setup_logging()
    process_files(update_attempts_file_path, error_log_path)
    
if __name__ == "__main__":
    main()