from concurrent.futures import ThreadPoolExecutor

import util


xml_base_peca = """<SearchReport id="14151" name="BASE PEÇAS">
  <DisplayFields>
    <DisplayField>17681</DisplayField>
    <DisplayField>20562</DisplayField>
    <DisplayField>17684</DisplayField>
    <DisplayField>17685</DisplayField>
    <DisplayField>21154</DisplayField>
    <DisplayField>18143</DisplayField>
    <DisplayField>18142</DisplayField>
    <DisplayField>18553</DisplayField>
    <DisplayField>22321</DisplayField>
    <DisplayField>17721</DisplayField>
  </DisplayFields>
  <PageSize>5000</PageSize>
  <IsResultLimitPercent>False</IsResultLimitPercent>
  <Criteria>
    <Keywords />
    <Filter>
      <OperatorLogic />
      <Conditions>
        <TextFilterCondition>
          <Field>20562</Field>
          <Operator>DoesNotContain</Operator>
          <Value />
        </TextFilterCondition>
      </Conditions>
    </Filter>
    <ModuleCriteria>
      <Module>459</Module>
      <IsKeywordModule>True</IsKeywordModule>
      <BuildoutRelationship>Union</BuildoutRelationship>
      <SortFields>
        <SortField>
          <Field>17681</Field>
          <SortType>Ascending</SortType>
        </SortField>
      </SortFields>
      <Children>
        <ModuleCriteria>
          <Module>446</Module>
          <IsKeywordModule>False</IsKeywordModule>
          <BuildoutRelationship>Union</BuildoutRelationship>
          <SortFields>
            <SortField>
              <Field>20378</Field>
              <SortType>Ascending</SortType>
            </SortField>
          </SortFields>
        </ModuleCriteria>
      </Children>
    </ModuleCriteria>
  </Criteria>
</SearchReport>"""


xml_migracao_peca = """<SearchReport id="14284" name="MIGRACAO_PECAS">
  <DisplayFields>
    <DisplayField>17681</DisplayField>
    <DisplayField>20562</DisplayField>
    <DisplayField>17684</DisplayField>
    <DisplayField>17685</DisplayField>
    <DisplayField>24340</DisplayField>
    <DisplayField>25443</DisplayField>
    <DisplayField>21154</DisplayField>
    <DisplayField>18143</DisplayField>
    <DisplayField>18142</DisplayField>
    <DisplayField>18553</DisplayField>
    <DisplayField>22321</DisplayField>
    <DisplayField>17721</DisplayField>
  </DisplayFields>
  <PageSize>5000</PageSize>
  <IsResultLimitPercent>False</IsResultLimitPercent>
  <Criteria>
    <Keywords />
    <Filter>
      <OperatorLogic />
      <Conditions>
        <TextFilterCondition>
          <Field>20562</Field>
          <Operator>DoesNotContain</Operator>
          <Value />
        </TextFilterCondition>
        <ValueListFilterCondition>
          <Field>20342</Field>
          <Operator>Contains</Operator>
          <IsNoSelectionIncluded>False</IsNoSelectionIncluded>
          <IncludeChildren>False</IncludeChildren>
          <Values>
            <Value>84850</Value>
          </Values>
        </ValueListFilterCondition>
        <ValueListFilterCondition>
          <Field>24698</Field>
          <Operator>Contains</Operator>
          <IsNoSelectionIncluded>False</IsNoSelectionIncluded>
          <IncludeChildren>False</IncludeChildren>
          <Values>
            <Value>124024</Value>
          </Values>
        </ValueListFilterCondition>
        <DateOffsetFilterCondition>
          <Field>24340</Field>
          <Operator>LastXHours</Operator>
          <Value>24</Value>
        </DateOffsetFilterCondition>
        <ValueListFilterCondition>
          <Field>17685</Field>
          <Operator>DoesNotContain</Operator>
          <IsNoSelectionIncluded>True</IsNoSelectionIncluded>
          <IncludeChildren>False</IncludeChildren>
        </ValueListFilterCondition>
      </Conditions>
    </Filter>
    <ModuleCriteria>
      <Module>459</Module>
      <IsKeywordModule>True</IsKeywordModule>
      <BuildoutRelationship>Union</BuildoutRelationship>
      <SortFields>
        <SortField>
          <Field>17681</Field>
          <SortType>Ascending</SortType>
        </SortField>
      </SortFields>
      <Children>
        <ModuleCriteria>
          <Module>446</Module>
          <IsKeywordModule>False</IsKeywordModule>
          <BuildoutRelationship>Union</BuildoutRelationship>
          <SortFields>
            <SortField>
              <Field>20378</Field>
              <SortType>Ascending</SortType>
            </SortField>
          </SortFields>
        </ModuleCriteria>
      </Children>
    </ModuleCriteria>
  </Criteria>
</SearchReport>"""


def leitura_da_base(xml_peca):
    total = util.tamanho_relatorio(xml_peca.replace("<PageSize>5000</PageSize>", "<PageSize>0</PageSize>"))
    total_pag = (int(total) // 5000) + 1
    i = 189
    while i < total_pag:
        util.mensagens(f"Pagina: {i} de {total_pag}.", 'warning')
        inicio = util.time()

        conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                    util.os.environ.get('db_username'),
                                    util.os.environ.get('db_password'),
                                    util.os.environ.get('database_new'))
        cursor = conx.cursor(as_dict=True)

        dados = util.extrair_relatorio(xml_peca, i)
        for dado in dados:
            sql = f"INSERT INTO pecas (id_sistema_peca, scpjud, tipo_peca, diretoria, ramo, produto, " \
                  f"cpf_cnpj, nome_cliente, sinistro) values " \
                  f"(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            try:
                insert = (
                    dado['ID do Sistema - Peças Processuais'],
                    dado['Numero do Cliente - Robô - Integra'],
                    dado['Tipo de Peça Processual'][0]['displayName'] if dado['Tipo de Peça Processual'] != '' else '',
                    dado['Diretoria'] if 'Diretoria' in dado else '',
                    dado['Ramo Principal'][0]['displayName'] if dado['Ramo Principal'] != '' else '',
                    dado['Produto Principal'][0]['displayName'] if dado['Produto Principal'] != '' else '',
                    dado[
                        'CPF do Autor Principal (Relatório Auto)'] if 'CPF do Autor Principal (Relatório Auto)' in dado else '',
                    dado['Nome do Cliente'][0]['displayName'] if dado['Nome do Cliente'] != '' else '',
                    dado['Número do Sinistro'] if 'Número do Sinistro' in dado else ''
                )
            except Exception as e:
                print(e)

            cursor.execute(sql, insert)

            for documento in dado['Documento'].values():
                sql2 = f"INSERT INTO documentos_peca (id_sistema_peca, id_arquivo) values " \
                       f"(%s,%s)"

                insert2 = (dado['ID do Sistema - Peças Processuais'], documento['id'])

                cursor.execute(sql2, insert2)

        conx.commit()
        conx.close()

        fim = util.time()

        util.mensagens(f"5 mil registros processados em :{util.cronometro(fim - inicio)}",
                       'ok', bold=True)
        i += 1


def leitura_diaria_base(xml_peca):
    try:
        total = util.tamanho_relatorio(xml_peca)
        total_pag = (int(total) // 5000) + 1
        i = 1
        while i <= total_pag:
            util.mensagens(f"Pagina: {i} de {total_pag}.", 'warning')
            inicio = util.time()
            dados = util.extrair_relatorio(xml_peca, i)
            for dado in dados:
                try:
                    if pag_existe(dado['ID do Sistema - Peças Processuais']):
                        atualizar_pag(dado)
                    else:
                        cadastrar_pag(dado)
                    for documento in dado['Documento'].values():
                        if doc_existe(dado['ID do Sistema - Peças Processuais']):
                            atualizar_doc(dado['ID do Sistema - Peças Processuais'], documento['id'])
                        else:
                            cadastrar_doc(dado['ID do Sistema - Peças Processuais'], documento['id'])
                except Exception as e:
                    print(e)
            fim = util.time()

            util.mensagens(f"Registros processados em :{util.cronometro(fim - inicio)}",
                           'ok', bold=True)
            i += 1
    except Exception as e:
        print(e)


def pag_existe(id_peca):
    query = f"SELECT TOP 1 id FROM dbo.pecas WHERE id_sistema_peca = {id_peca}"

    pesquisa = util.exec_sql_return_banco_novo(query)
    if len(pesquisa) > 0:
        return True
    else:
        return False


def doc_existe(id_doc):
    query = f"SELECT TOP 1 id FROM dbo.documentos_peca WHERE id_sistema_peca = {id_doc}"

    pesquisa = util.exec_sql_return_banco_novo(query)
    if len(pesquisa) > 0:
        return True
    else:
        return False


def cadastrar_pag(dado):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql = f"INSERT INTO pecas (id_sistema_peca, scpjud, tipo_peca, diretoria, ramo, produto, " \
          f"cpf_cnpj, nome_cliente, sinistro) values " \
          f"(%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    insert = (
        dado['ID do Sistema - Peças Processuais'],
        dado['Numero do Cliente - Robô - Integra'],
        dado['Tipo de Peça Processual'][0]['displayName'] if dado['Tipo de Peça Processual'] != '' else '',
        dado['Diretoria'] if 'Diretoria' in dado else '',
        dado['Ramo Principal'][0]['displayName'] if dado['Ramo Principal'] != '' else '',
        dado['Produto Principal'][0]['displayName'] if dado['Produto Principal'] != '' else '',
        dado[
            'CPF do Autor Principal (Relatório Auto)'] if 'CPF do Autor Principal (Relatório Auto)' in dado else '',
        dado['Nome do Cliente'][0]['displayName'] if dado['Nome do Cliente'] != '' else '',
        dado['Número do Sinistro'] if 'Número do Sinistro' in dado else ''
    )

    cursor.execute(sql, insert)
    conx.commit()
    conx.close()


def atualizar_pag(dado):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql = f"UPDATE pecas SET scpjud = %s, tipo_peca = %s, diretoria = %s, ramo = %s, produto = %s, " \
          f"cpf_cnpj = %s, nome_cliente = %s, sinistro = %s WHERE id_sistema_peca = %s"

    insert = (
        dado['Numero do Cliente - Robô - Integra'],
        dado['Tipo de Peça Processual'][0]['displayName'] if dado['Tipo de Peça Processual'] != '' else '',
        dado['Diretoria'] if 'Diretoria' in dado else '',
        dado['Ramo Principal'][0]['displayName'] if dado['Ramo Principal'] != '' else '',
        dado['Produto Principal'][0]['displayName'] if dado['Produto Principal'] != '' else '',
        dado[
            'CPF do Autor Principal (Relatório Auto)'] if 'CPF do Autor Principal (Relatório Auto)' in dado else '',
        dado['Nome do Cliente'][0]['displayName'] if dado['Nome do Cliente'] != '' else '',
        dado['Número do Sinistro'] if 'Número do Sinistro' in dado else '',
        dado['ID do Sistema - Peças Processuais']
    )

    cursor.execute(sql, insert)
    conx.commit()
    conx.close()


def cadastrar_doc(id_peca, id_documento):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql2 = f"INSERT INTO documentos_peca (id_sistema_peca, id_arquivo) values " \
           f"(%s,%s)"

    insert2 = (id_peca, id_documento)

    cursor.execute(sql2, insert2)
    conx.commit()
    conx.close()


def atualizar_doc(id_peca, id_documento):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql2 = f"UPDATE documentos_peca SET id_arquivo = %s, status = 'NULL' WHERE id_sistema_peca = %s"

    insert2 = (id_documento, id_peca)

    cursor.execute(sql2, insert2)
    conx.commit()
    conx.close()


def id_tipo_peca(tipo):
    query = f"SELECT TOP 1 id FROM dbo.tipo_de_peca_arteria WHERE nome = '{tipo}'"

    pesquisa = util.exec_sql_return_banco_novo(query)
    if len(pesquisa) > 0:
        return pesquisa[0]['id']
    else:
        return ''


def leitura_diaria_base_novo(xml_peca):
    try:
        total = util.tamanho_relatorio(xml_peca)
        total_pag = (int(total) // 5000) + 1
        i = 1
        while i <= total_pag:
            util.mensagens(f"Pagina: {i} de {total_pag}.", 'warning')
            inicio = util.time()
            dados = util.extrair_relatorio(xml_peca, i)
            to_insert = []
            to_update = []
            base_pecas = pecas_base()
            base_pecas = [str(x["id_sistema_peca"]) for x in base_pecas]
            base_doc = doc_base()
            base_doc = [str(x["id_arquivo"]) for x in base_doc]
            doc_insert = []
            doc_update = []
            for dado in dados:
                if dado['ID do Sistema - Peças Processuais'] in base_pecas:
                    to_update.append(dado)
                else:
                    to_insert.append(dado)
                for documento in dado['Documento'].values():
                    documento['ID do Sistema - Peças Processuais'] = dado['ID do Sistema - Peças Processuais']
                    if documento['id'] in base_doc:
                        doc_update.append(documento)
                    else:
                        doc_insert.append(documento)
            util.mensagens(f'Inserindo {len(to_insert)} dados de peças no banco de dados', 'ok')
            cadastrar_pecas_many(to_insert)
            util.mensagens(f'Atualizando {len(to_update)} dados de peças no banco de dados', 'ok')
            atualizar_pecas_many(to_update)
            util.mensagens(f'Inserindo {len(doc_insert)} dados de documentos no banco de dados', 'ok')
            cadastrar_doc_many(doc_insert)
            util.mensagens(f'Atualizando {len(doc_update)} dados de documentos no banco de dados', 'ok')
            atualizar_doc_many(doc_update)

            fim = util.time()

            util.mensagens(f"Registros processados em :{util.cronometro(fim - inicio)}",
                           'ok', bold=True)
            i += 1
    except Exception as e:
        print(e)


def doc_base():
    query = f"SELECT DISTINCT id_arquivo FROM documentos_peca_com_nome"

    pesquisa = util.exec_sql_return_banco_novo(query)

    return pesquisa


def pecas_base():
    query = f"SELECT DISTINCT id_sistema_peca FROM dbo.pecas"

    pesquisa = util.exec_sql_return_banco_novo(query)

    return pesquisa


def cadastrar_pecas_many(dados):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql = f"INSERT INTO pecas (id_sistema_peca, scpjud, tipo_peca, diretoria, ramo, produto, " \
          f"cpf_cnpj, nome_cliente, sinistro) values " \
          f"(%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    inserts = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        res = []
        for dado in dados:
            res.append(executor.submit(pecas_cadastrar, dado))
        for r in res:
            inserts.append(r.result())

    try:
        cursor.executemany(sql, inserts)
    except Exception as e:
        print(e)
    conx.commit()
    conx.close()


def pecas_cadastrar(dado):
    insert = (
        dado['ID do Sistema - Peças Processuais'],
        dado['Numero do Cliente - Robô - Integra'],
        dado['Tipo de Peça Processual'][0]['displayName'] if dado['Tipo de Peça Processual'] != '' else '',
        dado['Diretoria'] if 'Diretoria' in dado else '',
        dado['Ramo Principal'][0]['displayName'] if dado['Ramo Principal'] != '' else '',
        dado['Produto Principal'][0]['displayName'] if dado['Produto Principal'] != '' else '',
        dado[
            'CPF do Autor Principal (Relatório Auto)'] if 'CPF do Autor Principal (Relatório Auto)' in dado else '',
        dado['Nome do Cliente'][0]['displayName'] if dado['Nome do Cliente'] != '' else '',
        dado['Número do Sinistro'] if 'Número do Sinistro' in dado else ''
    )
    return insert


def atualizar_pecas_many(dados):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql = f"UPDATE pecas SET scpjud = %s, tipo_peca = %s, diretoria = %s, ramo = %s, produto = %s, " \
          f"cpf_cnpj = %s, nome_cliente = %s, sinistro = %s WHERE id_sistema_peca = %s"
    updates = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        res = []
        for dado in dados:
            res.append(executor.submit(pecas_atualizar, dado))
        for r in res:
            updates.append(r.result())
    try:
        cursor.executemany(sql, updates)
    except Exception as e:
        print(e)

    conx.commit()
    conx.close()


def pecas_atualizar(dado):
    update = (
        dado['Numero do Cliente - Robô - Integra'],
        dado['Tipo de Peça Processual'][0]['displayName'] if dado['Tipo de Peça Processual'] != '' else '',
        dado['Diretoria'] if 'Diretoria' in dado else '',
        dado['Ramo Principal'][0]['displayName'] if dado['Ramo Principal'] != '' else '',
        dado['Produto Principal'][0]['displayName'] if dado['Produto Principal'] != '' else '',
        dado[
            'CPF do Autor Principal (Relatório Auto)'] if 'CPF do Autor Principal (Relatório Auto)' in dado else '',
        dado['Nome do Cliente'][0]['displayName'] if dado['Nome do Cliente'] != '' else '',
        dado['Número do Sinistro'] if 'Número do Sinistro' in dado else '',
        dado['ID do Sistema - Peças Processuais']
    )
    return update


def cadastrar_doc_many(dados):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql2 = f"INSERT INTO documentos_peca_com_nome (id_sistema_peca, id_arquivo, nome_arquivo) values " \
           f"(%s,%s,%s)"
    inserts = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        res = []
        for dado in dados:
            res.append(executor.submit(doc_cadastrar, dado))
        for r in res:
            inserts.append(r.result())

    try:
        cursor.executemany(sql2, inserts)
    except Exception as e:
        print(e)
    conx.commit()
    conx.close()


def doc_cadastrar(dado):
    insert = (dado['ID do Sistema - Peças Processuais'], dado['id'], dado['nome'])
    return insert


def atualizar_doc_many(dados):
    conx = util.pymssql.connect(util.os.environ.get('db_server'),
                                util.os.environ.get('db_username'),
                                util.os.environ.get('db_password'),
                                util.os.environ.get('database_new'))

    cursor = conx.cursor(as_dict=True)

    sql2 = f"UPDATE documentos_peca_com_nome SET id_sistema_peca = %s, " \
           f"nome_arquivo = %s WHERE id_arquivo = %s"
    update = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        res = []
        for dado in dados:
            res.append(executor.submit(doc_atualizar, dado))
        for r in res:
            update.append(r.result())

    try:
        cursor.executemany(sql2, update)
    except Exception as e:
        print(e)
    conx.commit()
    conx.close()


def doc_atualizar(dado):
    update = (dado['id'], dado['nome'], dado['ID do Sistema - Peças Processuais'])
    return update



if __name__ == '__main__':
    leitura_diaria_base_novo(xml_migracao_peca)