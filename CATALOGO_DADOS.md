# Catálogo de Dados — MVP ONS

Este documento descreve o modelo implementado nos notebooks da branch `main` e complementa o README. **Catálogo:** `ons_energia`; **schemas:** `bronze`, `silver` e `gold`; **formato de persistência:** Delta Lake. Os tipos apresentados correspondem aos esquemas definidos ou derivados pelas transformações implementadas nos notebooks. Os esquemas físicos das tabelas persistidas podem ser consultados no Databricks por meio do comando DESCRIBE TABLE. As chaves são lógicas, com validações no pipeline, sem declaração de constraints físicas de FK.

## 1. Bronze — `ons_energia.bronze.bronze_interrupcao_carga`

**Grão:** uma linha do arquivo CSV original. **Carga:** integral, com sobrescrita. **Volume:** 8.326 linhas na versão analisada. Os onze campos originais são lidos como texto, sem deduplicação.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cod_perturbacao` | `string` | Código da perturbação; não é chave única da linha. |
| `din_interrupcaocarga` | `string` | Data e hora da interrupção registrada na fonte. |
| `id_subsistema` | `string` | Identificador do subsistema elétrico. |
| `nom_subsistema` | `string` | Nome do subsistema elétrico. |
| `id_estado` | `string` | Sigla da unidade federativa. |
| `nom_agente` | `string` | Nome do agente afetado. |
| `val_cargainterrompida_mw` | `string` | Carga interrompida registrada (MW). |
| `val_tempomedio_minutos` | `string` | Tempo médio de recomposição (min). |
| `val_energianaosuprida_mwh` | `string` | Energia não suprida (MWh). |
| `flg_envolveuredebasica` | `string` | Indicador S/N de envolvimento da Rede Básica. |
| `flg_envolveuredeoperacao` | `string` | Indicador S/N de envolvimento da Rede de Operação. |
| `_data_ingestao` | `timestamp` | Instante da ingestão. |
| `_arquivo_origem` | `string` | Caminho do arquivo de origem. |
| `_fonte_dados` | `string` | Identificação da fonte (ONS). |

## 2. Silver — `ons_energia.silver.silver_interrupcao_carga`

**Grão:** um registro válido, após deduplicação integral dos 11 campos de negócio. **Volume:** 8.309 linhas. Os campos de origem são mantidos, com tipos ajustados, e acrescentam-se atributos temporais. As três colunas de rastreabilidade da Bronze permanecem, conforme seleção realizada no notebook.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cod_perturbacao` | `string` | Código da perturbação; não é chave única da linha. |
| `din_interrupcaocarga` | `timestamp` | Data e hora da interrupção registrada na fonte. |
| `id_subsistema` | `string` | Identificador do subsistema elétrico. |
| `nom_subsistema` | `string` | Nome do subsistema elétrico. |
| `id_estado` | `string` | Sigla da unidade federativa. |
| `nom_agente` | `string` | Nome do agente afetado. |
| `val_cargainterrompida_mw` | `double` | Carga interrompida registrada (MW). |
| `val_tempomedio_minutos` | `double` | Tempo médio de recomposição (min). |
| `val_energianaosuprida_mwh` | `double` | Energia não suprida (MWh). |
| `flg_envolveuredebasica` | `string` | Indicador S/N de envolvimento da Rede Básica. |
| `flg_envolveuredeoperacao` | `string` | Indicador S/N de envolvimento da Rede de Operação. |
| `_data_ingestao` | `timestamp` | Metadado de ingestão herdado da Bronze. |
| `_arquivo_origem` | `string` | Arquivo de origem herdado da Bronze. |
| `_fonte_dados` | `string` | Fonte de dados herdada da Bronze. |
| `data_interrupcao` | `date` | Data extraída do timestamp. |
| `ano_interrupcao` | `int` | Ano da data registrada. |
| `mes_interrupcao` | `int` | Mês da data registrada. |
| `trimestre_interrupcao` | `int` | Trimestre da data registrada. |
| `dia_interrupcao` | `int` | Dia do mês. |
| `hora_interrupcao` | `int` | Hora do dia. |

### Quarentena — `ons_energia.silver.silver_registros_invalidos`

**Grão:** registro distinto reprovado em uma ou mais regras. **Volume:** 1 linha na versão analisada. Preserva os atributos tipados, metadados de ingestão e colunas de diagnóstico `erro_identificador`, `erro_data`, `erro_subsistema`, `erro_uf`, `erro_carga`, `erro_tempo`, `erro_energia`, `erro_rede_basica`, `erro_rede_operacao` (boolean), `registro_valido` (boolean) e `motivo_rejeicao` (string). O registro isolado apresentou tempo e ENS negativos. A quarentena não integra a fato.

## 3. Gold — modelo dimensional

**Grão da fato:** um registro válido da Silver, não uma perturbação agregada. Uma perturbação pode envolver diversos agentes e localidades. As dimensões são obtidas de combinações distintas da Silver e persistidas em Delta.

![Modelo dimensional](docs/modelo_dimensional.png)

### `ons_energia.gold.dim_tempo`

**Grão:** uma data distinta presente na Silver. **Chave lógica:** `sk_tempo`, inteiro `yyyyMMdd`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_tempo` | `int` | Chave substituta da data, no formato yyyyMMdd. |
| `data_interrupcao` | `date` | Data de interrupção. |
| `ano` | `int` | Ano. |
| `mes` | `int` | Mês. |
| `trimestre` | `int` | Trimestre. |
| `dia` | `int` | Dia do mês. |
| `dia_semana` | `int` | Dia da semana segundo Spark. |
| `nome_mes` | `string` | Nome do mês. |

### `ons_energia.gold.dim_localidade`

**Grão:** combinação distinta de UF e subsistema. **Chave lógica:** `sk_localidade` = SHA-256 da combinação UF e identificador do subsistema.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_localidade` | `string` | Chave substituta determinística (SHA-256). |
| `id_estado` | `string` | Sigla da unidade federativa. |
| `id_subsistema` | `string` | Identificador do subsistema elétrico. |
| `nom_subsistema` | `string` | Nome do subsistema elétrico. |

### `ons_energia.gold.dim_agente`

**Grão:** nome distinto de agente na fonte. **Chave lógica:** `sk_agente` = SHA-256 do nome; a fonte não disponibiliza um identificador cadastral exclusivo.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_agente` | `string` | Chave substituta determinística (SHA-256). |
| `nom_agente` | `string` | Nome do agente afetado. |

### `ons_energia.gold.dim_rede`

**Grão:** combinação distinta dos dois indicadores S/N de rede. **Chave lógica:** `sk_rede` = SHA-256 da combinação dos indicadores.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_rede` | `string` | Chave substituta determinística (SHA-256). |
| `flg_envolveuredebasica` | `string` | Indicador S/N de envolvimento da Rede Básica. |
| `flg_envolveuredeoperacao` | `string` | Indicador S/N de envolvimento da Rede de Operação. |
| `envolve_rede_basica` | `string` | Rótulo Sim/Não derivado do indicador. |
| `envolve_rede_operacao` | `string` | Rótulo Sim/Não derivado do indicador. |

### `ons_energia.gold.fato_interrupcao`

**Grão:** um registro válido de interrupção. **Chave lógica da linha:** `sk_interrupcao`, hash determinístico dos 11 campos de negócio. **Chaves estrangeiras lógicas:** `sk_tempo`, `sk_localidade`, `sk_agente`, `sk_rede`. **Volume:** 8.309 linhas.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_interrupcao` | `string` | Chave lógica única da linha (SHA-256). |
| `sk_tempo` | `int` | Referência à dimensão tempo. |
| `sk_localidade` | `string` | Referência à dimensão localidade. |
| `sk_agente` | `string` | Referência à dimensão agente. |
| `sk_rede` | `string` | Referência à dimensão rede. |
| `cod_perturbacao` | `string` | Código da perturbação; não é chave única da linha. |
| `din_interrupcaocarga` | `timestamp` | Data e hora da interrupção registrada na fonte. |
| `val_cargainterrompida_mw` | `double` | Carga interrompida registrada (MW). |
| `val_tempomedio_minutos` | `double` | Tempo médio de recomposição (min). |
| `val_energianaosuprida_mwh` | `double` | Energia não suprida (MWh). |
| `qtd_registros` | `int` | Contador aditivo, fixado em 1 por linha. |

### Tabelas auxiliares de qualidade — schema Gold

| Tabela | Grão | Finalidade |
|---|---|---|
| `qualidade_completude` | Atributo avaliado | Contagens de nulos e completude. |
| `qualidade_outliers` | Medida avaliada | Quartis, intervalo interquartil, limites e contagem de extremos. |
| `qualidade_granularidade` | Código de perturbação | Registros, diversidade de agentes/UF/ENS e totais por perturbação. |

Essas tabelas armazenam os resultados das verificações implementadas no notebook 04_qualidade_dados.ipynb. Seus esquemas físicos podem ser consultados diretamente no Unity Catalog do Databricks.

## 4. Regras, relacionamentos e reconciliação

- `fato_interrupcao.sk_tempo` → `dim_tempo.sk_tempo`; `sk_localidade` → `dim_localidade.sk_localidade`; `sk_agente` → `dim_agente.sk_agente`; `sk_rede` → `dim_rede.sk_rede`.
- Duplicatas: apenas linhas iguais nos onze campos de negócio são removidas (16 na versão analisada). Não se deduplica por `cod_perturbacao`.
- Registros com identificador, data, subsistema ou UF inválidos, medidas negativas ou indicadores de rede inválidos são segregados em quarentena.
- Valores positivos extremos são mantidos; 201 registros apresentam divergência entre ano do código e ano da data. As análises temporais usam a data efetiva.
- Reconciliação: 8.326 Bronze = 16 duplicatas + 1 inválido + 8.309 válidos Silver = 8.309 linhas na fato.
- A quantidade de registros é aditiva. A soma de carga interrompida por linha não representa potência simultânea; as medidas de tempo exigem média/mediana conforme a pergunta.

**Fonte de implementação:** notebooks [`01`](01_ingestao_bronze.ipynb), [`02`](02_refino_silver.ipynb), [`03`](03_modelagem_gold.ipynb) e [`04`](04_qualidade_dados.ipynb). Este catálogo descreve o recorte acadêmico, não a versão continuamente atualizada do portal ONS.
