#  id="19148" name="INTEGRAÇÃO PEÇAS PROCESSUAIS - ROBOT">
relatorio = '''<SearchReport> 
  <DisplayFields>
    <DisplayField name="ID do Sistema - Peças Processuais">17681</DisplayField>
    <DisplayField name="Nome do Cliente">22321</DisplayField>
  </DisplayFields>
  <PageSize>999999</PageSize>
  <IsResultLimitPercent>False</IsResultLimitPercent>
  <Criteria>
    <Keywords />
    <Filter>
      <OperatorLogic />
      <Conditions>
        <TextFilterCondition>
          <Field name="Número do Cliente">17591</Field>
          <Operator>DoesNotContain</Operator>
          <Value />
        </TextFilterCondition>
      </Conditions>
    </Filter>
    <ModuleCriteria>
      <Module name="Peças Processuais">459</Module>
      <IsKeywordModule>True</IsKeywordModule>
      <BuildoutRelationship>Union</BuildoutRelationship>
      <SortFields>
        <SortField>
          <Field name="ID do Sistema - Peças Processuais">17681</Field>
          <SortType>Ascending</SortType>
        </SortField>
      </SortFields>
      <Children>
        <ModuleCriteria>
          <Module name="Processos">446</Module>
          <IsKeywordModule>False</IsKeywordModule>
          <BuildoutRelationship>Intersection</BuildoutRelationship>
          <SortFields>
            <SortField>
              <Field name="ID do Processo">20378</Field>
              <SortType>Ascending</SortType>
            </SortField>
          </SortFields>
        </ModuleCriteria>
      </Children>
    </ModuleCriteria>
  </Criteria>
</SearchReport>'''

relatorio_subs = '''<SearchReport id="19153" name="INTEGRAÇÃO INFO SUBSIDIO - ROBOT">
  <DisplayFields>
    <DisplayField>18156</DisplayField>
    <DisplayField>21712</DisplayField>
    <DisplayField>20305</DisplayField>
    <DisplayField>22321</DisplayField>
    <DisplayField>17681</DisplayField>
  </DisplayFields>
  <PageSize>250</PageSize>
  <IsResultLimitPercent>False</IsResultLimitPercent>
  <Criteria>
    <Keywords />
    <Filter>
      <OperatorLogic />
      <Conditions>
        <ValueListFilterCondition>
          <Field>22321</Field>
          <Operator>Contains</Operator>
          <IsNoSelectionIncluded>False</IsNoSelectionIncluded>
          <IncludeChildren>False</IncludeChildren>
          <Values>
            <Value>114749</Value>
            <Value>129699</Value>
            <Value>127971</Value>
          </Values>
        </ValueListFilterCondition>
      </Conditions>
    </Filter>
    <ModuleCriteria>
      <Module>455</Module>
      <IsKeywordModule>True</IsKeywordModule>
      <BuildoutRelationship>Union</BuildoutRelationship>
      <SortFields>
        <SortField>
          <Field>18156</Field>
          <SortType>Ascending</SortType>
        </SortField>
      </SortFields>
      <Children>
        <ModuleCriteria>
          <Module>446</Module>
          <IsKeywordModule>False</IsKeywordModule>
          <BuildoutRelationship>Intersection</BuildoutRelationship>
          <SortFields>
            <SortField>
              <Field>20378</Field>
              <SortType>Ascending</SortType>
            </SortField>
          </SortFields>
        </ModuleCriteria>
        <ModuleCriteria>
          <Module>459</Module>
          <IsKeywordModule>False</IsKeywordModule>
          <BuildoutRelationship>Intersection</BuildoutRelationship>
          <SortFields>
            <SortField>
              <Field>17681</Field>
              <SortType>Ascending</SortType>
            </SortField>
          </SortFields>
        </ModuleCriteria>
      </Children>
    </ModuleCriteria>
  </Criteria>
</SearchReport>'''