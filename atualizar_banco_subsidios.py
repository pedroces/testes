import os
import logging
import pymssql
import datetime as dt
from datetime import datetime
from dotenv import load_dotenv
from funcoes_arteria import search
from relatorios import relatorio_subs

load_dotenv('.env')

def log_error(file_path, message):
    """
    Registra erros no arquivo especificado.
    """
    mode = 'a' if os.path.exists(file_path) else 'w'
    with open(file_path, mode) as file:
        if mode == 'w':
            file.write("Erro\n")
        file.write(f"Falha: {message}\n")

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

def executar_sql_(sql):
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

def configurar_logs_subs():
    """
    Configura o sistema de logs, criando diretórios e arquivos necessários.
    """
    agora = dt.datetime.now()

    diretorio_logs = f'logs/{agora.strftime("%Y-%m-%d_%H-%M-%S")}_Subsidios'
    os.makedirs(diretorio_logs, exist_ok=True)
    
    caminho_arquivo_log = os.path.join(diretorio_logs, 'processamento_subsidio.log')
    logging.basicConfig(
        filename=caminho_arquivo_log, 
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return (
        os.path.join(diretorio_logs, 'subsidios_atualizados.log'),
        os.path.join(diretorio_logs, 'erro_log.log')
    )   

def configurar_logs_pecas():
    """
    Configura o sistema de logs, criando diretórios e arquivos necessários.
    """
    agora = dt.datetime.now()

    diretorio_logs = f'logs/{agora.strftime("%Y-%m-%d_%H-%M-%S")}_Pecas_em_subsidios'
    os.makedirs(diretorio_logs, exist_ok=True)
    
    caminho_arquivo_log = os.path.join(diretorio_logs, 'processamento_pecas_em_subsidios.log')
    logging.basicConfig(
        filename=caminho_arquivo_log, 
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return (
        os.path.join(diretorio_logs, 'pecas_em_subsidios_atualizados.log'),
        os.path.join(diretorio_logs, 'erro_log.log')
    )   

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
            executar_sql_(sql_update_peca)
            logging.info(f"Registro com id_sistema_peca {id_sistema_peca} atualizado com sucesso.")
            registrar_tentativa_pecas(caminho_tentativas, id_sistema_subs, id_sistema_peca, True, "Atualização bem-sucedida.")
        else:
            executar_sql_(sql_insert_peca)
            logging.info(f"Registro com id_sistema_peca {id_sistema_peca} inserido com sucesso.")
            registrar_tentativa_pecas(caminho_tentativas, id_sistema_subs, id_sistema_peca, True, "Atualização bem-sucedida.")
            
    except Exception as erro:
        logging.error(f"Erro ao salvar execução para ID {id_sistema_peca}.")
        registrar_tentativa_pecas(caminho_tentativas, id_sistema_subs, id_sistema_peca, False, "Atualização bem-sucedida.")
        raise
    
def salvar_execucao_subs(caminho_tentativas, id_sistema_subs, id_sistema_processo, seq_integracao, nome_cliente):
    """
    Atualiza informações no banco de dados na table de info_subsidio.
    """
    
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    sql_check = f"""
        SELECT *
        FROM dbo.info_subsidio
        WHERE id_sistema_subsidio = {id_sistema_subs}
    """

    sql_update = f"""
        UPDATE dbo.info_subsidio
        SET id_sistema_processo = {id_sistema_processo}, seq_integracao = {seq_integracao}, nome_cliente = '{nome_cliente}', updated_at = '{agora}'
        WHERE id_sistema_subsidio = {id_sistema_subs}
    """
    
    sql_insert = f"""
        INSERT INTO dbo.info_subsidio
        (id_sistema_subsidio, id_sistema_processo, seq_integracao, nome_cliente, created_at)
        VALUES ({id_sistema_subs}, {id_sistema_processo}, {seq_integracao}, '{nome_cliente}', '{agora}')
    """

    try:
        resultado = executar_sql(sql_check)
        print(resultado)
        if resultado:
            executar_sql_(sql_update)
            logging.info(f"Registro com id_sistema_subs {id_sistema_subs} atualizado com sucesso.")
            #registrar_tentativa_subs(caminho_tentativas, id_sistema_subs, id_sistema_processo, True, "Atualização bem-sucedida.")
        else:
            executar_sql_(sql_insert)
            logging.info(f"Registro com id_sistema_subs {id_sistema_subs} inserido com sucesso.")
            #registrar_tentativa_subs(caminho_tentativas, id_sistema_subs, id_sistema_processo, True, "Atualização bem-sucedida.")
            
    except Exception as erro:
        logging.error(f"Erro ao salvar execução para ID {id_sistema_subs}.")
        #registrar_tentativa_subs(caminho_tentativas, id_sistema_subs, id_sistema_processo, False, "Atualização bem-sucedida.")
        raise
    
def registrar_tentativa_subs(caminho_arquivo, id_sistema_subs, id_sistema_processo, sucesso, mensagem):
    """
    Registra o status das tentativas de atualização.
    """
    status = 'Sucesso' if sucesso else 'Falha'
    with open(caminho_arquivo, 'a') as arquivo:
        arquivo.write(f"{id_sistema_subs}, {id_sistema_processo}, {status}, {mensagem}\n")
        
def registrar_tentativa_pecas(caminho_arquivo, id_sistema_subs, id_sistema_peca, sucesso, mensagem):
    """
    Registra o status das tentativas de atualização.
    """
    status = 'Sucesso' if sucesso else 'Falha'
    with open(caminho_arquivo, 'a') as arquivo:
        arquivo.write(f"{id_sistema_subs}, {id_sistema_peca}, {status}, {mensagem}\n")
        
def atualizar_subs(dados_arteria,dado,caminho_tentativas_pecas,caminho_tentativas_subs):
    try:
        id_sistema_subs = dado['ID do Sistema - Subsídio']
        id_sistema_processo = dado['Processos'][0]['ID do Sistema - Processo']
        seq_integracao = dado['seqBeneficiário'] if dado.get('seqBeneficiário') else ''
        nome_cliente = dado['Processos'][0]['Nome do Cliente'][0]
        
        salvar_execucao_subs(caminho_tentativas_subs,id_sistema_subs, id_sistema_processo, seq_integracao, nome_cliente)
        registrar_tentativa_subs(caminho_tentativas_subs, id_sistema_subs, id_sistema_processo, True, "Atualização bem-sucedida.")
        atualizar_pecas(dados_arteria,caminho_tentativas_pecas,id_sistema_processo)
    except Exception as erro:
        logging.error(f"Erro ao processar ID {id_sistema_subs}: {erro}")
        registrar_tentativa_subs(caminho_tentativas_subs, id_sistema_subs, id_sistema_processo, False, str(erro))

def atualizar_pecas(dados_arteria, caminho_tentativas_pecas,id_sistema_processo):
    
    try:
        for dado in dados_arteria:
                id_sistema_subs = dado['ID do Sistema - Subsídio']
                pecas_processuais = dado.get('Peças Processuais', [])
                
                if pecas_processuais:
                    for peca in pecas_processuais:
                        id_sistema_peca = peca.get('ID do Sistema - Peças Processuais')
                        
                        if id_sistema_peca:
                            salvar_execucao_peca(caminho_tentativas_pecas,id_sistema_subs, id_sistema_peca)
                            registrar_tentativa_pecas(caminho_tentativas_pecas, id_sistema_peca, id_sistema_processo, True, "Atualização bem-sucedida.")
                else:
                    print(f"Subsídio {id_sistema_subs} não possui peças processuais.")
                    registrar_tentativa_pecas(caminho_tentativas_subs, id_sistema_subs, id_sistema_processo, False, str(erro))
                        
    except Exception as erro:
        logging.error(f"Erro ao atualizar tabela info_subsidio_pecas: {erro}")
        registrar_tentativa_pecas(caminho_tentativas_subs, id_sistema_subs, id_sistema_processo, False, str(erro))
        pass

def processar_arquivos(caminho_tentativas_subs,caminho_tentativas_pecas):
    """
    Processa os dados obtidos e registra os resultados.
    """
    try:
        dados_arteria = search(relatorio_subs)
        logging.info("Dados do Artéria buscados com sucesso.")
        try:
            for dado in dados_arteria:
                atualizar_subs(dados_arteria,dado,caminho_tentativas_subs,caminho_tentativas_pecas)
                
        except Exception as erro:
            logging.error(f"Erro ao atualizar tabela info_subsidio: {erro}")
            registrar_tentativa_subs(caminho_tentativas_subs, id_sistema_subs, id_sistema_processo, False, str(erro))
            pass
        
    except Exception as erro:
        logging.error(f"Erro: {erro}")
        pass    

def main():
    """
    Função principal que inicializa o processo.
    """
    
    caminho_tentativas_subs = configurar_logs_subs()
    caminho_tentativas_pecas = configurar_logs_pecas()
    processar_arquivos(caminho_tentativas_subs, caminho_tentativas_pecas)

if __name__ == "__main__":
    main()