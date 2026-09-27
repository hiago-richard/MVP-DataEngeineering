
# MVP de Engenharia de Dados — Interrupções de Carga do ONS

Projeto de engenharia e análise de dados desenvolvido no Databricks, utilizando dados públicos do Operador Nacional do Sistema Elétrico (ONS). A solução implementa uma arquitetura Medallion (Bronze, Silver e Gold), tratamento e validação de dados, modelagem dimensional em esquema estrela e análises exploratórias orientadas a perguntas de negócio.

O projeto foi desenvolvido como MVP acadêmico no contexto da pós-graduação em Ciência de Dados e Analytics da PUC-Rio.

## 1. Contexto de Negócios e Perguntas (Etapas 2 e 4.1)

Construir um pipeline de dados capaz de transformar registros públicos de interrupção de carga em uma base estruturada, rastreável e adequada à análise.

Os objetivos específicos são:

- Ingerir os dados originais preservando sua estrutura e procedência.
- Padronizar tipos, tratar duplicidades e identificar registros inválidos.
- Organizar os dados em um modelo dimensional.
- Implementar verificações de qualidade e reconciliação entre camadas.
- Responder a seis perguntas de negócio por meio de consultas SQL.
- Documentar as decisões técnicas e as limitações dos resultados.

### Perguntas de negócio planejadas

1. **P1:** Como evoluíram a frequência das interrupções e a energia não suprida ao longo dos anos?
2. **P2:** Quais estados e subsistemas concentram os maiores volumes de carga interrompida e energia não suprida?
3. **P3:** Como varia o tempo médio de recomposição entre estados e subsistemas?
4. **P4:** Como diferem as interrupções que envolveram ou não a Rede Básica?
5. **P5:** Existe comportamento mensal ou sazonal na frequência e magnitude das interrupções?
6. **P6:** Quais perturbações concentraram os maiores volumes de energia não suprida?

As respostas, consultas e limitações são apresentadas na seção 6 e no notebook [`05_analise_final.ipynb`](05_analise_final.ipynb).

### Estrutura dos dados brutos

O conjunto *Interrupção de Carga* é disponibilizado pelo ONS em um arquivo CSV, acompanhado de um dicionário de dados em JSON. A versão utilizada neste MVP contém 8.326 registros e 11 campos de negócio.

| Campo original | Descrição |
|---|---|
| `cod_perturbacao` | Código identificador da perturbação. |
| `din_interrupcaocarga` | Data e hora da interrupção de carga. |
| `id_subsistema` | Identificador do subsistema elétrico. |
| `nom_subsistema` | Nome do subsistema elétrico. |
| `id_estado` | Sigla da unidade federativa. |
| `nom_agente` | Nome do agente afetado. |
| `val_cargainterrompida_mw` | Carga interrompida, em MW. |
| `val_tempomedio_minutos` | Tempo médio de recomposição, em minutos. |
| `val_energianaosuprida_mwh` | Energia não suprida, em MWh. |
| `flg_envolveuredebasica` | Indicador de envolvimento da Rede Básica (S/N). |
| `flg_envolveuredeoperacao` | Indicador de envolvimento da Rede de Operação (S/N). |

A unidade básica da fonte é um registro de interrupção associado a uma perturbação, agente e localidade. O campo `cod_perturbacao` não identifica necessariamente uma linha única, pois uma perturbação pode estar associada a múltiplos registros.

Os dados originais são preservados na camada Bronze. As transformações e estruturas derivadas são descritas nas seções de modelagem e qualidade de dados.

### Contexto de negócios

As interrupções de carga representam eventos relevantes para a operação do Sistema Interligado Nacional, pois podem afetar o fornecimento de energia elétrica e produzir impactos de diferentes magnitudes e durações. O conjunto de dados disponibilizado pelo Operador Nacional do Sistema Elétrico (ONS) permite analisar esses eventos sob perspectivas temporais, geográficas e operacionais.

O problema de negócio abordado neste MVP consiste em transformar registros brutos de interrupção de carga em informações estruturadas que permitam identificar padrões, comparar impactos e reconhecer eventos de maior relevância energética. Para isso, são consideradas medidas como carga interrompida (MW), tempo médio de recomposição (minutos) e energia não suprida (MWh).

O conjunto possui critérios próprios de abrangência e não representa todas as interrupções ocorridas nas redes de distribuição. As análises devem, portanto, ser interpretadas dentro do escopo dos dados publicados pelo ONS.

## 2. Carga dos Dados (Etapa 4.2)

**Fonte:** Operador Nacional do Sistema Elétrico (ONS).

**Fonte oficial e recursos:** [Conjunto Interrupção de Carga — ONS Dados Abertos](https://dados.ons.org.br/dataset/interrupcao_carga), incluindo [recurso CSV](https://dados.ons.org.br/dataset/interrupcao_carga/resource/e87dfb42-d713-41b6-81d3-df21d3caab31) e [dicionário JSON](https://dados.ons.org.br/dataset/interrupcao_carga/resource/cbda0486-fc65-46be-b0b1-d04174cd7eda).

**Coleta e recorte:** os arquivos CSV e dicionário de dados foram obtidos no portal oficial de Dados Abertos do ONS em **03/09/2026**. O CSV foi disponibilizado manualmente no Volume do Databricks. A versão analisada contém **8.326 registros**, com última ocorrência registrada em **01/09/2026**. Como o conjunto é atualizado pelo ONS, extrações posteriores poderão apresentar contagens diferentes. Os arquivos de origem não estão versionados neste repositório.

**Licença e condições de uso:** o conjunto *Interrupção de Carga* é disponibilizado pelo ONS sob a licença Creative Commons Atribuição (CC BY). A licença permite a distribuição, modificação e adaptação dos dados, desde que seja atribuído crédito ao ONS e informadas as alterações realizadas. Neste MVP, os dados foram submetidos a padronização de tipos, deduplicação, validações de qualidade e modelagem dimensional. A fonte original é o [Portal de Dados Abertos do ONS](https://dados.ons.org.br/dataset/interrupcao_carga).

**Procedimento de obtenção:** acessar o conjunto oficial, abrir o recurso CSV e baixar o arquivo; consultar o dicionário no mesmo portal. No Databricks, criar/verificar o Volume com `00_configuracao`, enviar o CSV para `/Volumes/ons_energia/bronze/arquivos_ons/INTERRUPCAO_CARGA.csv` e executar `01_ingestao_bronze`. A ingestão usa CSV UTF-8, separador `;`, cabeçalho e esquema explícito. O nome local é o esperado pelo notebook.



**Conjunto utilizado:** Interrupção de Carga.

**Arquivo de entrada:** `INTERRUPCAO_CARGA.csv`.

**Dicionário:** `DicionarioDados_InterrupcaoDadosl.json`.

A base original contém 8.326 registros e 11 campos, com informações sobre perturbações, datas, subsistemas, estados, agentes, carga interrompida, tempo médio de recomposição, energia não suprida e indicadores de envolvimento das redes.

O período observado vai de 2007 a 2026. Os dados de 2026 são parciais, com última data registrada em 01/09/2026.

Principais medidas:

| Campo | Descrição | Unidade |
|---|---|---|
| `val_cargainterrompida_mw` | Carga interrompida registrada | MW |
| `val_tempomedio_minutos` | Tempo médio para recompor a carga | minutos |
| `val_energianaosuprida_mwh` | Energia não suprida | MWh |

O código `cod_perturbacao` identifica a perturbação na fonte. Uma mesma perturbação pode estar associada a múltiplos registros de agentes e localidades.

> **Escopo:** o conjunto representa registros de interrupção de carga disponibilizados pelo ONS. Não corresponde à totalidade das interrupções nas redes de distribuição e não deve ser confundido com uma base de indicadores DEC/FEC.

### Procedimento de carga no Databricks

A carga inicial foi realizada manualmente a partir do arquivo CSV obtido no portal oficial do ONS em 03/09/2026. O arquivo foi enviado para um Volume do Databricks, no caminho:

`/Volumes/ons_energia/bronze/arquivos_ons/INTERRUPCAO_CARGA.csv`

A execução foi organizada em duas etapas:

1. O notebook [`00_configuracao.ipynb`](00_configuracao.ipynb) prepara o catálogo, os schemas e os caminhos utilizados pelo projeto.
2. O notebook [`01_ingestao_bronze.ipynb`](01_ingestao_bronze.ipynb) realiza a leitura do CSV em UTF-8, com separador `;`, cabeçalho e esquema explícito. Os 11 campos de origem são preservados como texto e recebem três atributos adicionais de rastreabilidade: `_data_ingestao`, `_arquivo_origem` e `_fonte_dados`.

Os dados são persistidos em formato Delta na tabela `ons_energia.bronze.bronze_interrupcao_carga`. A versão analisada resultou em 8.326 registros na Bronze, sem deduplicação nessa etapa.

A estratégia adotada é de carga integral com sobrescrita (`overwrite`), sem processamento incremental ou agendamento automático nesta versão do MVP. O CSV original permanece no Volume do Databricks e não está versionado no GitHub.

## 3. Tecnologias utilizadas

- Databricks Free Edition;
- Apache Spark e PySpark;
- Spark SQL;
- Delta Lake;
- Unity Catalog;
- Git e GitHub;
- Notebooks para implementação, validação e análise.

## 4. Modelagem e Catálogo de Dados (Etapa 4.3)

A solução utiliza a arquitetura Medallion, com três camadas de dados.

![Arquitetura Medallion do MVP](docs/arquitetura_medallion.png)

### Estrutura do modelo dimensional

A camada Gold foi organizada em esquema estrela, com uma tabela fato central e quatro dimensões. A granularidade da fato corresponde a um registro válido de interrupção da camada Silver, e não a uma perturbação agregada. Essa decisão preserva a possibilidade de uma mesma perturbação envolver diferentes agentes e localidades.

| Tabela | Granularidade | Finalidade |
|---|---|---|
| `fato_interrupcao` | Um registro válido de interrupção | Armazena as medidas de carga interrompida, tempo de recomposição e energia não suprida. |
| `dim_tempo` | Uma data distinta | Permite análises por ano, mês, trimestre e dia. |
| `dim_localidade` | Uma combinação de UF e subsistema | Permite análises geográficas. |
| `dim_agente` | Um nome distinto de agente | Permite análises por agente afetado. |
| `dim_rede` | Uma combinação dos indicadores de rede | Permite comparar registros com e sem envolvimento das redes. |

As tabelas são persistidas em formato Delta Lake no schema `ons_energia.gold`. Os relacionamentos são estabelecidos por chaves lógicas, cuja integridade é verificada no pipeline.

O catálogo completo de tabelas, colunas, tipos, chaves e regras de negócio está documentado em [`CATALOGO_DADOS.md`](CATALOGO_DADOS.md).

### Catálogo de dados — tabelas e atributos

As tabelas do projeto estão organizadas no catálogo `ons_energia`, nos schemas `bronze`, `silver` e `gold`, com persistência em Delta Lake. A seguir, apresenta-se o catálogo de atributos das principais tabelas, incluindo tipos de dados e funções no modelo.

#### Camada Bronze — `bronze_interrupcao_carga`

| Coluna | Tipo | Descrição |
|---|---|---|
| `cod_perturbacao` | string | Código da perturbação. |
| `din_interrupcaocarga` | string | Data e hora originais. |
| `id_subsistema` | string | Identificador do subsistema. |
| `nom_subsistema` | string | Nome do subsistema. |
| `id_estado` | string | Unidade federativa. |
| `nom_agente` | string | Agente afetado. |
| `val_cargainterrompida_mw` | string | Carga interrompida. |
| `val_tempomedio_minutos` | string | Tempo de recomposição. |
| `val_energianaosuprida_mwh` | string | Energia não suprida. |
| `flg_envolveuredebasica` | string | Indicador da Rede Básica. |
| `flg_envolveuredeoperacao` | string | Indicador da Rede de Operação. |
| `_data_ingestao` | timestamp | Instante de ingestão. |
| `_arquivo_origem` | string | Caminho do arquivo. |
| `_fonte_dados` | string | Identificação da fonte. |

**Granularidade:** uma linha do CSV original. **Volume:** 8.326 registros.

#### Camada Silver — `silver_interrupcao_carga`

A Silver mantém os campos de negócio da Bronze, convertendo `din_interrupcaocarga` para `timestamp` e as três medidas para `double`. Os demais campos de negócio permanecem como `string`.

Os metadados `_data_ingestao` (`timestamp`), `_arquivo_origem` (`string`) e `_fonte_dados` (`string`) são preservados.

| Atributo adicional | Tipo | Descrição |
|---|---|---|
| `data_interrupcao` | date | Data da interrupção. |
| `ano_interrupcao` | int | Ano. |
| `mes_interrupcao` | int | Mês. |
| `trimestre_interrupcao` | int | Trimestre. |
| `dia_interrupcao` | int | Dia do mês. |
| `hora_interrupcao` | int | Hora do dia. |

**Granularidade:** um registro válido após deduplicação integral. **Volume:** 8.309 registros.

A tabela `silver_registros_invalidos` preserva os registros reprovados nas regras de qualidade, acompanhados de indicadores de erro e do motivo de rejeição.

#### Camada Gold — `fato_interrupcao`

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_interrupcao` | string | Chave lógica única do registro. |
| `sk_tempo` | int | Referência à dimensão tempo. |
| `sk_localidade` | string | Referência à dimensão localidade. |
| `sk_agente` | string | Referência à dimensão agente. |
| `sk_rede` | string | Referência à dimensão rede. |
| `cod_perturbacao` | string | Código original da perturbação. |
| `din_interrupcaocarga` | timestamp | Data e hora da interrupção. |
| `val_cargainterrompida_mw` | double | Carga interrompida (MW). |
| `val_tempomedio_minutos` | double | Tempo médio de recomposição (min). |
| `val_energianaosuprida_mwh` | double | Energia não suprida (MWh). |
| `qtd_registros` | int | Contador de registros, fixado em 1. |

**Granularidade:** um registro válido de interrupção. **Volume:** 8.309 registros.

#### Dimensões da camada Gold

| Tabela | Colunas e tipos |
|---|---|
| `dim_tempo` | `sk_tempo` (int), `data_interrupcao` (date), `ano` (int), `mes` (int), `trimestre` (int), `dia` (int), `dia_semana` (int), `nome_mes` (string). |
| `dim_localidade` | `sk_localidade` (string), `id_estado` (string), `id_subsistema` (string), `nom_subsistema` (string). |
| `dim_agente` | `sk_agente` (string), `nom_agente` (string). |
| `dim_rede` | `sk_rede` (string), `flg_envolveuredebasica` (string), `flg_envolveuredeoperacao` (string), `envolve_rede_basica` (string), `envolve_rede_operacao` (string). |

As chaves substitutas identificam os registros das dimensões e estabelecem relacionamentos lógicos com a tabela fato. A integridade desses relacionamentos é verificada no pipeline.

O detalhamento das regras de negócio, granularidades, tabelas auxiliares e atributos de diagnóstico permanece disponível no [Catálogo de Dados completo](CATALOGO_DADOS.md).

### Evidências do catálogo no Databricks

As capturas a seguir foram obtidas no Unity Catalog e demonstram a organização dos schemas e a estrutura das tabelas persistidas.

**Figura 1 — Organização do catálogo `ons_energia`.**

![Catálogo e schemas Bronze, Silver e Gold](docs/evidencias/01_catalogo_schemas.png)

**Figura 2 — Estrutura da tabela Bronze.**

![Colunas da tabela bronze_interrupcao_carga](docs/evidencias/02_estrutura_bronze.png)

**Figura 3 — Estrutura da tabela Silver.**

![Colunas da tabela silver_interrupcao_carga](docs/evidencias/03_estrutura_silver.png)

**Figura 4 — Estrutura da tabela fato Gold.**

![Colunas da tabela fato_interrupcao](docs/evidencias/04_estrutura_fato_gold.png)

**Figura 5 — Tabelas da camada Gold.**

![Tabela fato e dimensões persistidas na Gold](docs/evidencias/05_tabelas_gold.png)

O catálogo utilizado no Databricks é `ons_energia`, com os schemas `bronze`, `silver` e `gold`.

### 4.1. Bronze — ingestão

A camada Bronze preserva os 11 campos originais como texto e acrescenta metadados de rastreabilidade:

- `_data_ingestao`;
- `_arquivo_origem`;
- `_fonte_dados`.

Tabela principal:

`ons_energia.bronze.bronze_interrupcao_carga`

**Resultado:** 8.326 registros ingeridos.

A preservação dos valores originais permite auditar as transformações aplicadas nas etapas seguintes.

### 4.2. Silver — tratamento e validação

A camada Silver realiza:

- Conversão de datas e medidas numéricas;
- Padronização de tipos;
- Remoção de duplicatas integrais considerando os 11 campos de negócio;
- Verificação de valores ausentes e medidas negativas;
- Separação de registros inválidos em tabela de quarentena;
- Enriquecimento com atributos temporais.

Tabelas principais:

- `ons_energia.silver.silver_interrupcao_carga`;
- `ons_energia.silver.silver_registros_invalidos`.

**Resultado:** 8.309 registros válidos e um registro isolado em quarentena.

O registro inválido apresentou valores negativos de tempo de recomposição e energia não suprida. Ele foi preservado na quarentena para rastreabilidade, sem integrar as análises Gold.

### 4.3. Gold — modelagem dimensional

A camada Gold organiza os dados em um esquema estrela, com uma tabela fato e dimensões de tempo, localidade, agente e rede.

A tabela `ons_energia.gold.fato_interrupcao` contém 8.309 registros, com chaves substitutas e medidas analíticas.

As dimensões permitem consultar as medidas sob diferentes perspectivas sem repetir a lógica de tratamento dos dados.

A solução também mantém tabelas auxiliares com evidências de qualidade, incluindo completude, identificação de valores extremos e avaliação da granularidade.

![Esquema estrela da camada Gold](docs/modelo_dimensional.png)

O dicionário de tabelas, atributos, tipos, chaves, granularidade e regras está em [`CATALOGO_DADOS.md`](CATALOGO_DADOS.md). As chaves documentadas são lógicas e verificadas por testes no código; não se pressupõe a criação de restrições físicas de chave estrangeira.

## 5. Pipeline de Dados (Etapa 4.4)

A implementação foi dividida em etapas sequenciais:

| Notebook | Finalidade |
|---|---|
| [`00_configuracao`](00_configuracao.ipynb) | Configuração inicial do catálogo, schemas e caminhos |
| [`01_ingestao_bronze`](01_ingestao_bronze.ipynb) | Leitura da fonte e ingestão rastreável |
| [`02_refino_silver`](02_refino_silver.ipynb) | Conversão, padronização, deduplicação e quarentena |
| [`03_modelagem_gold`](03_modelagem_gold.ipynb) | Criação do esquema estrela |
| [`04_qualidade_dados`](04_qualidade_dados.ipynb) | Validações e reconciliação das camadas |
| [`05_analise_final`](05_analise_final.ipynb) | Consultas SQL e respostas às seis perguntas |

### Organização do pipeline ETL

O pipeline foi dividido em seis notebooks sequenciais, em vez de concentrar todas as operações em um único arquivo. Essa organização separa a configuração do ambiente, a ingestão, o tratamento, a modelagem dimensional, as verificações de qualidade e a análise final.

O fluxo começa com a configuração do catálogo `ons_energia` e a leitura do CSV armazenado no Volume do Databricks. Os registros são persistidos na Bronze, refinados na Silver e organizados em esquema estrela na Gold. Em seguida, são executadas as verificações de qualidade e as consultas analíticas.

As tabelas são armazenadas em formato Delta Lake no Databricks. A estratégia adotada nesta versão é de carga integral com sobrescrita (`overwrite`), sem agendamento automático ou processamento incremental. Os notebooks estão disponíveis no GitHub e devem ser executados na ordem apresentada acima.

### Evidências de persistência no Databricks

As consultas abaixo demonstram que os dados foram materializados nas tabelas Delta das três camadas. A reconciliação entre as contagens confirma a preservação dos registros originais na Bronze e a aplicação das regras de tratamento na Silver.

**Figura 6 — Contagem de registros nas camadas Bronze, Silver e Gold.**

![Evidência de persistência e contagem das tabelas Delta](docs/evidencias/06_persistencia_camadas.png)

**Figura 7 — Registros inválidos preservados na tabela de quarentena.**

![Evidência da tabela de quarentena na Silver](docs/evidencias/07_quarentena.png)

Para reproduzir o projeto:

1. Obtenha o CSV no [portal oficial do ONS](https://dados.ons.org.br/dataset/interrupcao_carga), consulte o dicionário e carregue o CSV no Volume `/Volumes/ons_energia/bronze/arquivos_ons/` com o nome `INTERRUPCAO_CARGA.csv`. O arquivo não está incluído no GitHub.
2. Revise os caminhos e identificadores definidos em `00_configuracao`.
3. Execute os notebooks na ordem apresentada.
4. Confira os resultados de reconciliação em `04_qualidade_dados`.
5. Execute `05_analise_final` para consultar os indicadores e visualizar as análises.

O ambiente de execução deve permitir o uso de Spark, Delta Lake e Unity Catalog. A ingestão e as tabelas derivadas são gravadas por **carga integral com `overwrite`**; não há atualização incremental nem agendamento automático nesta versão. As contagens esperadas correspondem ao recorte da fonte utilizado no MVP.

**Importante:** o GitHub versiona o código e a documentação. As tabelas Delta e os arquivos armazenados no Volume permanecem no ambiente de dados e não são recriados apenas pela clonagem do repositório.

## 6. Qualidade de Dados (Etapa 4.5)

A reconciliação entre as camadas apresentou os seguintes resultados:

| Etapa | Registros |
|---|---:|
| Bronze — registros originais | 8.326 |
| Duplicatas integrais removidas | 16 |
| Registros inválidos isolados | 1 |
| Silver — registros válidos | 8.309 |
| Gold — registros na tabela fato | 8.309 |

### Problemas identificados e tratamentos aplicados

| Verificação | Problema identificado | Tratamento ou decisão |
|---|---|---|
| Duplicidade | 16 linhas integralmente repetidas nos 11 campos de negócio. | Remoção das duplicatas na Silver, preservando uma ocorrência de cada registro distinto. |
| Medidas inválidas | Um registro apresentou tempo de recomposição e energia não suprida negativos. | Segregação na tabela `silver_registros_invalidos`, com preservação do registro e dos motivos de rejeição. |
| Tipos de dados | Datas e medidas foram recebidas como texto no CSV. | Conversão para `timestamp` e `double` na Silver. |
| Valores extremos | Identificação de medidas positivas elevadas. | Preservação dos valores, pois um extremo estatístico não comprova erro na origem. |
| Divergência temporal | 201 registros apresentaram diferença entre o ano do código da perturbação e o ano da data registrada. | Preservação do código original e utilização de `din_interrupcaocarga` como referência para as análises temporais. |
| Granularidade | Uma perturbação pode estar associada a diferentes agentes e localidades. | Manutenção da granularidade por registro válido, sem deduplicação apenas pelo código da perturbação. |
| Integridade dimensional | Necessidade de garantir a correspondência entre fato e dimensões. | Verificação de unicidade das chaves e correspondência das referências da fato com as dimensões. |

### Evidência da validação de qualidade

As verificações foram executadas no notebook [`04_qualidade_dados.ipynb`](04_qualidade_dados.ipynb), contemplando reconciliação entre camadas, integridade dimensional e avaliação das medidas.

**Figura 8 — Validação da unicidade da chave da tabela fato.**

![Teste de unicidade da chave da fato executado no Databricks](docs/evidencias/08_validacao_qualidade.png)

As regras de validação também contemplam identificadores, datas, subsistemas, unidades federativas e indicadores de envolvimento das redes. Os registros reprovados são separados da base analítica, sem perda da rastreabilidade.

A reconciliação confirma que os 8.326 registros da Bronze correspondem a 16 duplicatas removidas, um registro inválido isolado e 8.309 registros válidos na Silver, posteriormente materializados na tabela fato Gold.

Foram verificados:

- Ausência de valores nulos nos 11 campos de negócio;
- Integridade das chaves da tabela fato;
- Correspondência dos registros da fato com as dimensões;
- Validade das medidas numéricas;
- Duplicidade integral;
- Distribuição e valores extremos;
- Consistência entre o ano da data e o ano incorporado ao código da perturbação.

### Decisões de qualidade

**Duplicatas:** foram removidas somente as linhas integralmente repetidas nos campos de negócio. Não foi aplicada deduplicação por `cod_perturbacao`, pois uma perturbação pode gerar múltiplos registros legítimos.

**Valores extremos:** os valores positivos elevados foram preservados. Um valor estatisticamente extremo não comprova, isoladamente, erro de origem.

**Divergência de ano:** foram identificados 201 registros nos quais o ano do código da perturbação difere do ano da data registrada. As análises temporais utilizam `din_interrupcaocarga`, preservando o código original.

**Granularidade:** a validação realizada na base não encontrou códigos de perturbação associados a mais de uma data de interrupção. Assim, o agrupamento por código foi mantido na análise de concentração de ENS.

## 7. Análise de Dados (Etapa 4.5)

As análises utilizam os registros válidos da camada Gold.

### P1 — Como evoluíram a frequência das interrupções e a energia não suprida?

A quantidade de registros e a ENS não evoluem necessariamente de forma proporcional.

- **2023:** maior frequência anual, com 573 registros e 63.958,83 MWh de ENS.
- **2009:** maior ENS anual, com 107.825,41 MWh em 474 registros.
- **2020:** 98.919,49 MWh em 318 registros.

Os resultados indicam que perturbações de elevada magnitude podem influenciar significativamente os totais anuais mesmo em períodos com menor frequência.

O ano de 2026 é parcial e não foi tratado como equivalente a um ano completo.

**Figura 9 — Evolução anual da energia não suprida (ENS).**

![Evolução anual da energia não suprida, em MWh, de 2007 a 2026](docs/evidencias/09_analise_p1.png)

A visualização evidencia a oscilação da ENS ao longo da série histórica, com picos relevantes em 2009 e 2020. O ano de 2026 apresenta dados parciais e não deve ser comparado diretamente com anos completos.

### P2 — Quais estados e subsistemas concentram os maiores impactos?

São Paulo apresentou a maior ENS acumulada entre os estados, com **117.406,88 MWh**.

O Amapá apresentou **88.038,08 MWh em 108 registros**, com forte influência de uma ocorrência individual de 77.509,63 MWh.

O Rio Grande do Sul apresentou a maior quantidade de registros entre os estados, com 1.020, mas não a maior ENS.

Por subsistema:

| Subsistema | Registros | ENS (MWh) |
|---|---:|---:|
| Sudeste/Centro-Oeste | 3.316 | 315.060,07 |
| Norte | 2.233 | 294.339,02 |
| Nordeste | 1.318 | 203.725,38 |
| Sul | 1.442 | 83.991,71 |

As comparações representam valores absolutos, sem normalização pela carga atendida, quantidade de consumidores ou extensão da rede.

**Figura 10 — Energia não suprida acumulada por unidade federativa.**

![Comparação da energia não suprida acumulada por estado](docs/evidencias/10_analise_p2_estado.png)

A distribuição por estado evidencia diferenças na magnitude acumulada das interrupções, com destaque para São Paulo e Amapá.

**Figura 11 — Distribuição dos indicadores de interrupção por subsistema.**

![Participação dos subsistemas na quantidade de registros, carga interrompida e energia não suprida](docs/evidencias/11_analise_p2_subsistema.png)

Os gráficos apresentam a participação relativa de cada subsistema na quantidade de registros, na soma da carga interrompida e na ENS acumulada. A comparação mostra que a distribuição dos registros não é necessariamente proporcional à distribuição dos impactos energéticos.

### P3 — Como varia o tempo de recomposição?

A distribuição dos tempos é assimétrica e apresenta valores extremos relevantes.

O Amapá registrou média de **378,73 minutos** e mediana de **40,75 minutos**. No Rio Grande do Sul, a média foi de **125,81 minutos** e a mediana de **13,43 minutos**.

Por subsistema, o Sul apresentou a maior média (**103,19 minutos**), enquanto o Nordeste apresentou a maior mediana (**22,95 minutos**).

A análise conjunta de média e mediana evita que ocorrências excepcionais sejam confundidas com o comportamento central dos registros.

**Figura 12 — Indicadores de tempo de médio e mediana de recomposição por unidade federativa.**

![Comparação dos indicadores de tempo médio, mediano e maior tempo de recomposição por estado](docs/evidencias/12_analise_p3.png)

A visualização compara indicadores de tempo de recomposição entre estados e evidencia a influência de ocorrências extremas. A leitura deve considerar as escalas distintas dos eixos, especialmente para o maior tempo registrado.

### P4 — Como diferem as interrupções com e sem envolvimento da Rede Básica?

| Indicador | Com envolvimento | Sem envolvimento |
|---|---:|---:|
| Registros | 6.210 | 2.099 |
| Carga média por registro (MW) | 95,94 | 55,24 |
| ENS acumulada (MWh) | 755.980,96 | 141.135,21 |
| ENS média por registro (MWh) | 121,74 | 67,24 |
| Tempo médio (min) | 62,40 | 79,19 |
| Tempo mediano (min) | 16,00 | 9,38 |

Os registros com envolvimento da Rede Básica apresentaram maiores valores médios de carga interrompida e ENS. A relação entre os grupos e o tempo de recomposição varia conforme a medida estatística utilizada.

Trata-se de comparação descritiva, sem inferência de causalidade.

**Figura 13 — Comparação dos indicadores segundo o envolvimento da Rede Básica.**

![Participação dos registros com e sem envolvimento da Rede Básica nos indicadores de carga, tempo e ENS](docs/evidencias/13_analise_p4.png)

**Nota de interpretação:** os percentuais dos gráficos representam a participação nas somas dos indicadores exibidos pela visualização do Databricks. Para comparar médias e medianas entre os grupos, devem ser utilizados os valores absolutos apresentados na tabela acima, e não os percentuais dos gráficos.

Os gráficos apresentam a participação relativa dos dois grupos nos indicadores analisados. Os valores absolutos, médias e medianas apresentados na tabela desta subseção complementam a interpretação.

### P5 — Existe comportamento mensal ou sazonal?

Na série de 2007 a 2025, outubro apresentou a maior frequência acumulada, com **940 registros**, enquanto julho apresentou a menor, com **425 registros**.

Novembro apresentou a maior ENS acumulada, com **202.462,71 MWh**.

A matriz ano × mês mostrou que parte desse volume está concentrada em períodos excepcionais, como novembro de 2009 e novembro de 2020. Portanto, os picos mensais de ENS não devem ser interpretados automaticamente como um padrão sazonal recorrente.

**Figura 14 — Distribuição mensal dos registros de interrupção.**

![Distribuição dos registros por mês e indicador de média anual](docs/evidencias/14_analise_p5.png)

A visualização permite comparar a frequência de registros entre os meses, destacando outubro como o mês de maior volume acumulado no período analisado. A interpretação da sazonalidade também considera os resultados de ENS e a concentração de eventos excepcionais descritos nesta subseção.

### P6 — Quais perturbações concentraram os maiores impactos energéticos?

| Código | Registros | Estados | ENS (MWh) |
|---|---:|---:|---:|
| `0672/2011` | 29 | 18 | 89.371,48 |
| `6814/2020` | 1 | 1 | 77.509,63 |
| `5799/2018` | 66 | 26 | 51.692,18 |
| `5598/2023` | 116 | 26 | 48.106,22 |
| `5783/2012` | 14 | 12 | 37.518,45 |

As cinco perturbações com maior ENS concentram aproximadamente **33,90%** do total da base; as dez primeiras, **45,17%**; e as vinte primeiras, **53,06%**.

O código `0672/2011` possui data registrada em 2009. Por isso, seu impacto integra o ano de 2009 nas análises temporais.

A concentração ocorre tanto em perturbações de ampla abrangência quanto em registros individuais de longa duração.

**Figura 15 — Perturbações com maior energia não suprida acumulada.**

![Ranking das perturbações por ENS, com quantidade de registros, estados e agentes envolvidos](docs/evidencias/15_analise_p6.png)

A tabela apresenta as perturbações de maior impacto energético, com suas respectivas quantidades de registros, estados e agentes envolvidos. Os resultados evidenciam que tanto eventos de ampla abrangência quanto ocorrências individuais podem concentrar volumes expressivos de ENS.

## 8. Limitações

- A base representa registros públicos de interrupção de carga do ONS, não todas as interrupções da distribuição.
- O ano de 2026 possui cobertura parcial.
- O ano incorporado ao código da perturbação pode divergir do ano da data registrada.
- Valores extremos positivos foram mantidos e podem influenciar médias e totais.
- A soma de cargas interrompidas por registro não representa potência simultaneamente interrompida.
- A frequência de registros não equivale necessariamente à quantidade de perturbações.
- As comparações geográficas não foram normalizadas por características dos sistemas.
- Os resultados são descritivos e não permitem atribuir automaticamente causas aos eventos.

## 9. Autoavaliação do projeto

Considero que os objetivos propostos para este MVP foram alcançados dentro do escopo acadêmico. Foi desenvolvida uma solução de engenharia de dados no Databricks, utilizando a arquitetura Medallion, com ingestão, tratamento, validação e modelagem dimensional dos registros de interrupção de carga do ONS. As seis perguntas de negócio foram respondidas por meio de consultas SQL, permitindo explorar os impactos energéticos, temporais, geográficos e operacionais das interrupções.

O desenvolvimento exigiu atenção especial à granularidade dos registros, ao tratamento de duplicatas, à identificação de dados inválidos e à interpretação de valores extremos. Essas decisões reforçaram a importância de compreender o contexto dos dados antes de transformá-los. Como limitações, destacam-se a carga integral sem automatização do pipeline e a cobertura parcial dos dados de 2026, aspectos que poderão ser aprimorados em evoluções futuras.

O projeto foi elaborado com o auxílio de ferramentas de inteligência artificial generativa, utilizadas como apoio ao aprendizado, à elaboração e revisão de códigos, à documentação e à análise dos resultados. As sugestões foram avaliadas e ajustadas conforme os requisitos do trabalho e os resultados obtidos no Databricks. A execução, as decisões técnicas e a validação final permaneceram sob minha responsabilidade. Essa experiência contribuiu para o desenvolvimento de competências em engenharia de dados e reforçou a importância do uso crítico e responsável da inteligência artificial.

Como trabalhos futuros, pretende-se evoluir a solução para uma ingestão incremental, automatizar a execução e o monitoramento do pipeline, ampliar os testes de qualidade e desenvolver um painel analítico para acompanhamento dos indicadores. Também poderão ser incorporadas novas fontes de dados para contextualizar os impactos das interrupções. Essas evoluções permitirão transformar o MVP acadêmico em um projeto de portfólio mais completo, demonstrando competências adicionais em orquestração, observabilidade e disponibilização de dados para consumo analítico.

## 10. Referência da fonte

Operador Nacional do Sistema Elétrico (ONS) — [Interrupção de Carga](https://dados.ons.org.br/dataset/interrupcao_carga), [recurso CSV](https://dados.ons.org.br/dataset/interrupcao_carga/resource/e87dfb42-d713-41b6-81d3-df21d3caab31) e [dicionário JSON](https://dados.ons.org.br/dataset/interrupcao_carga/resource/cbda0486-fc65-46be-b0b1-d04174cd7eda).

Consulte os canais oficiais de dados abertos do ONS para obter a versão atualizada da fonte.

---

Projeto acadêmico desenvolvido para fins de estudo e demonstração de competências em Engenharia de Dados e Analytics.

