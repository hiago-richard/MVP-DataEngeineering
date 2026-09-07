# Pipeline de Dados do SIN: Monitoramento de Interrupções de Carga no Databricks

Este repositório contém o projeto prático (MVP) desenvolvido para a pós-graduação da **PUC-Rio**, focado na construção de um pipeline de dados *end-to-end* utilizando a **Arquitetura Medalhão (Bronze, Silver e Gold)** no **Databricks** com **Delta Lake** e **Unity Catalog**.

O projeto analisa o histórico de interrupções de carga e ocorrências no **Sistema Interligado Nacional (SIN)**, a partir de dados abertos disponibilizados pelo **Operador Nacional do Sistema Elétrico (ONS)**.

---

## 1. Contexto de Negócios e Objetivos

O Sistema Interligado Nacional (SIN) coordena a produção e a transmissão de energia elétrica no Brasil. Eventos imprevistos e perturbações na rede podem ocasionar cortes temporários no suprimento de energia.

O objetivo deste pipeline é estruturar um ambiente de análise para avaliar a severidade, o tempo de recomposição do sistema e o impacto dessas interrupções em diferentes regiões e agentes ao longo do tempo.

### Perguntas de Negócio

1. **Severidade por Subsistema:** Quais subsistemas do SIN (Norte, Nordeste, Sul, Sudeste/Centro-Oeste) apresentam maior volume acumulado de Carga Interrompida (MW) e Energia Não Suprida (MWh)?
2. **Desempenho de Recomposição:** Qual é o tempo médio de recomposição (`val_tempomedio_minutos`) por agente distribuidor/concessionária?
3. **Impacto da Rede Básica:** Eventos que afetam a *Rede Básica* de transmissão geram impactos significativamente maiores em MWh e duração comparados a eventos puramente de distribuição local?
4. **Evolução Temporal:** Qual é a tendência anual de interrupções e volume de energia cortada ao longo das últimas décadas?

---

## 2. Fonte de Dados

* **Origem:** Portal de Dados Abertos do ONS (Operador Nacional do Sistema Elétrico).
* **Licença:** Licença Aberta para Uso Público / Dados Abertos do Governo Federal.
* **Formato Bruto:** Arquivo CSV delimitado por ponto e vírgula (`;`), contendo 8.326 registros detalhando dados temporais, geográficos, operacionais e de severidade das perturbações.
* **Local de Armazenamento:** `/Volumes/workspace/default/raw_data/INTERRUPCAO_CARGA.csv`

---

## 3. Arquitetura de Dados (Arquitetura Medalhão)

A solução foi implementada utilizando o **Unity Catalog** (Catálogo `workspace`, Schema `default`) e tabelas no formato **Delta Lake**:

```
[ CSV Bruto no Volume ] ──> Camada Bronze (Raw Delta Table)
                                  │
                                  ▼
                            Camada Silver (Cleaned, Typed & Partitioned)
                                  │
                                  ▼
                            Camada Gold (Business Aggregation)
                                  │
                                  ▼
                            Consultas SQL & Dashboards (Databricks SQL)

```

---

## 4. Dicionário de Dados e Modelagem

### Tabela: `workspace.default.silver_interrupcao_carga`

| Coluna | Tipo | Descrição | Origem / Regra |
| --- | --- | --- | --- |
| `cod_perturbacao` | `STRING` | Código de identificação do evento | `bronze.cod_perturbacao` |
| `dh_interrupcao` | `TIMESTAMP` | Data e hora exata da ocorrência | `to_timestamp(din_interrupcaocarga)` |
| `ano` | `INT` | Ano da interrupção (coluna de partição) | `year(dh_interrupcao)` |
| `mes` | `INT` | Mês da interrupção | `month(dh_interrupcao)` |
| `id_subsistema` | `STRING` | Sigla do subsistema (N, NE, S, SE) | `bronze.id_subsistema` |
| `nom_subsistema` | `STRING` | Nome por extenso do subsistema | `bronze.nom_subsistema` |
| `id_estado` | `STRING` | Unidade Federativa afetada | `bronze.id_estado` |
| `nom_agente` | `STRING` | Agente responsável formatado | `trim(upper(nom_agente))` |
| `val_cargainterrompida_mw` | `DOUBLE` | Carga cortada em Megawatts (MW) | `bronze.val_cargainterrompida_mw` |
| `val_tempomedio_minutos` | `DOUBLE` | Duração média da interrupção (min) | `bronze.val_tempomedio_minutos` |
| `val_energianaosuprida_mwh` | `DOUBLE` | Energia Não Suprida total (MWh) | `bronze.val_energianaosuprida_mwh` |
| `flg_envolveuredebasica` | `STRING` | Indica envolvimento da Rede Básica | `'SIM'` se 'S', senão `'NÃO'` |
| `flg_envolveuredeoperacao` | `STRING` | Indica envolvimento da Operação | `'SIM'` se 'S', senão `'NÃO'` |

---

## 5. Estrutura do Pipeline de Código

### Camada Bronze: Ingestão de Dados Brutos

```python
from pyspark.sql import SparkSession

path_raw = "/Volumes/workspace/default/raw_data/INTERRUPCAO_CARGA.csv"

df_bronze = (spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", "true")
    .csv(path_raw))

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_interrupcao_carga")

```

### Camada Silver: Limpeza, Tipagem e Particionamento

```python
from pyspark.sql import functions as F

df_bronze = spark.table("workspace.default.bronze_interrupcao_carga")

df_silver = (df_bronze
    .withColumn("dh_interrupcao", F.to_timestamp("din_interrupcaocarga", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("ano", F.year("dh_interrupcao"))
    .withColumn("mes", F.month("dh_interrupcao"))
    .withColumn("nom_agente", F.trim(F.upper(F.col("nom_agente"))))
    .withColumn("flg_envolveuredebasica", F.when(F.col("flg_envolveuredebasica") == "S", "SIM").otherwise("NÃO"))
    .withColumn("flg_envolveuredeoperacao", F.when(F.col("flg_envolveuredeoperacao") == "S", "SIM").otherwise("NÃO"))
    .drop("din_interrupcaocarga")
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("ano") \
    .saveAsTable("workspace.default.silver_interrupcao_carga")

```

### Camada Gold: Agregações de Negócio

```python
from pyspark.sql import functions as F

df_silver = spark.table("workspace.default.silver_interrupcao_carga")

df_gold = (df_silver
    .groupBy("nom_subsistema", "ano")
    .agg(
        F.countDistinct("cod_perturbacao").alias("total_perturbacoes"),
        F.round(F.sum("val_cargainterrompida_mw"), 2).alias("total_carga_interrompida_mw"),
        F.round(F.sum("val_energianaosuprida_mwh"), 2).alias("total_ens_mwh"),
        F.round(F.avg("val_tempomedio_minutos"), 2).alias("tempo_medio_recomposicao_min")
    )
    .orderBy("nom_subsistema", "ano")
)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_resumo_subsistema")

```

---

## 6. Qualidade dos Dados

Durante o processo de profilaxia dos dados brutos, foram validados os seguintes pontos:

* **Completude:** O arquivo bruto apresenta 8.326 registros sem presença de valores nulos em colunas chave.
* **Integridade de Tipos:** A data/hora original foi convertida com sucesso de texto para `TIMESTAMP`.
* **Consistência:** Valores de colunas categóricas como `nom_agente` foram padronizados para caixa alta e sem espaços residuais.
* **Intervalo Válido:** Todas as colunas numéricas de carga (MW), energia (MWh) e duração (minutos) possuem valores não negativos.

---

## 7. Estrutura do Repositório

```text
├── README.md                          <-- Documentação completa do projeto
├── notebooks/
│   ├── 01_ingestao_bronze.py          <-- Carga inicial e persistência em Delta
│   ├── 02_refino_silver.py            <-- Limpeza, tipagem e particionamento por ano
│   └── 03_agregacoes_gold.py          <-- Visões consolidadas e consultas SQL
└── screenshots/                       <-- Evidências visuais de execução no Databricks
    ├── catalog_explorer.png
    ├── execucao_bronze.png
    ├── consultas_sql.png
    └── graficos_analiticos.png

```

---

## 8. Autoavaliação e Conclusão

* **Objetivos Alcançados:** O pipeline atendeu a todas as etapas exigidas, estruturando um fluxo resiliente no Databricks utilizando Delta Lake e Unity Catalog.
* **Principais Aprendizados:** Implementação prática de governança de dados no Unity Catalog, particionamento na Camada Silver para otimização de leitura e uso de SQL nativo no Databricks para visualização rápida de métricas.
* **Próximos Passos:** Implementar rotinas automatizadas de qualidade usando *Delta Live Tables* (DLT) e integração contínua para atualização dos dados via API do ONS.
