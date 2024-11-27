import util
# sql = "SELECT pecas.id_sistema_peca, pecas.scpjud, pecas.tipo_peca, " \
#         "pecas.diretoria, pecas.ramo, pecas.produto, pecas.cpf_cnpj," \
#         "pecas.nome_cliente, pecas.sinistro, " \
#         "documentos_peca.id_arquivo ,documentos_peca_com_nome.nome_arquivo, documentos_peca.status " \
#         "FROM pecas " \
#         "INNER JOIN documentos_peca " \
#         "ON documentos_peca.id_sistema_peca = pecas.id_sistema_peca " \
#         "INNER JOIN documentos_peca_com_nome " \
#         "ON documentos_peca.id_arquivo = documentos_peca_com_nome.id_arquivo " \
#         "WHERE documentos_peca.status = 'MIGRADO'"
#
# migrados = util.exec_sql_return_banco_novo(sql)
#
# sql2 = "SELECT pecas.id_sistema_peca, pecas.scpjud, pecas.tipo_peca, " \
#         "pecas.diretoria, pecas.ramo, pecas.produto, pecas.cpf_cnpj," \
#         "pecas.nome_cliente, pecas.sinistro, " \
#         "documentos_peca.id_arquivo , documentos_peca.status " \
#         "FROM pecas " \
#         "INNER JOIN documentos_peca " \
#         "ON documentos_peca.id_sistema_peca = pecas.id_sistema_peca " \
#         "WHERE documentos_peca.status = 'MIGRADO'"
#
# migrados2 = util.exec_sql_return_banco_novo(sql2)

sql3 = "SELECT DISTINCT id_sistema_peca FROM documentos_peca  " \
      "where status = 'MIGRADO' and id_sistema_peca in (SELECT id_sistema_pecas FROM pecas_excluidas_arteria )"

deletados = util.exec_sql_return_banco_novo(sql3)
print('ff')