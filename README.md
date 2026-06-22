# Titanic Data Lake - Projeto Didático

Protótipo funcional de um Data Lake moderno usando ferramentas open source do mercado.
Criado para demonstrar na prática os conceitos iniciais de arquitetura de dados que times de engenharia usam em produção.

---

## O que é um Data Lake?

Um Data Lake é um repositório centralizado que armazena dados de múltiplas fontes em seu formato bruto, permitindo processamento e análise em qualquer momento.

Diferente de um banco de dados tradicional onde os dados chegam já estruturados, no Data Lake você armazena primeiro e estrutura depois o que dá flexibilidade pra trabalhar com qualquer tipo de dado.

O padrão mais adotado no mercado para organizar um Data Lake é a **Medallion Architecture**, que divide os dados em três camadas:

| Camada | Nome | O que faz |
|---|---|---|
| 🥉 Bronze | Raw | Cópia fiel da fonte. Nunca alterada. |
| 🥈 Silver | Cleaned | Dados limpos, tipados e tratados. |
| 🥇 Gold | Aggregated | Dados prontos para consumo e análise. |

---

## Ferramentas Utilizadas

**Apache Spark / PySpark**
Engine de processamento distribuído. Processa grandes volumes de dados em paralelo. É o motor que lê, transforma e escreve os dados entre as camadas.

**Delta Lake**
Camada de armazenamento que adiciona confiabilidade em cima de arquivos Parquet. Traz para arquivos o que bancos de dados já tinham: transações ACID, versionamento, histórico de operações e a capacidade de desfazer alterações.

Sem Delta Lake, se um job cair no meio de uma escrita, o arquivo fica corrompido. Com Delta, a operação é atômica ou completa ou não acontece.

**MinIO**
O MinIO simula localmente o comportamento de storages cloud como AWS S3 e Azure ADLS. A vantagem é que o Spark acessa via s3a:// da mesma forma que acessaria um bucket real em produção cloud. O código não muda, só o endereço. Também permite controle de usuarios e acessos a cada bucket e arquivo.

**Jupyter Notebook**
Ambiente de desenvolvimento interativo. Permite executar o pipeline célula por célula com feedback imediato   ideal para aprendizado e prototipagem. Utilizei a imagem docker do jupyter pois ja vem com bibliotecas que utilizei, a pronto uso.

**Docker**
Isola todo o ambiente em containers. O projeto roda igual em qualquer máquina sem configuração manual de dependências.

---

## Arquitetura do Projeto

```
titanic.csv (fonte)
      │
      ▼
┌─────────────┐
│   Bronze    │  Ingesta raw, sem transformação
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Silver    │  Limpeza, tipagem, remoção de nulos
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Gold     │  Agregação: taxa de sobrevivência por classe e sexo
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Análise   │  Lê do Gold e gera gráficos
└─────────────┘

Todas as camadas armazenadas no MinIO via Delta Lake
```

---

## Estrutura de Pastas

```
titanic-datalake/
├── docker-compose.yml          # Jupyter(Bibliotecas) + MinIO + DeltaSpark
├── README.md
├── data/
│   └── raw/
│       └── titanic.csv         # Fonte original
├── minio-data/                 # Volume persistido do MinIO 
└── notebooks/
    ├── 01_bronze.ipynb         # Ingestão CSV → Delta no MinIO
    ├── 02_silver.ipynb         # Limpeza e tipagem → Delta no MinIO
    ├── 03_gold.ipynb           # Agregação → Delta no MinIO
    └── 05_analise.ipynb        # Lê Gold e plota gráficos
```

---

## Como Rodar

**Pré-requisitos:** Docker Desktop instalado e rodando.

```bash
# 1. Clonar ou copiar a pasta do projeto
cd titanic-datalake

# 2. Subir o container
docker compose up

# 3. Acessar o Jupyter
http://localhost:8888

# 4. Acessar o painel do MinIO
http://localhost:9001
# usuário: admin | senha: admin123

# 5. Criar bucker "titanic" no MinIO
- Clique em "Create Bucket"
- Nome: titanic
- Confirme

# 5. Executar os notebooks em ordem no jupyter
01_bronze.ipynb → 02_silver.ipynb → 03_gold.ipynb → 04_analise.ipynb

# 6. Encerrar
docker compose down
```

---

## Conceitos Delta Lake Demonstrados
notebooks/04_delta_concepts.ipynb

**Schema Enforcement**
O Delta bloqueia automaticamente escritas com tipos incompatíveis. Se você tentar sobrescrever uma coluna `boolean` com `integer`, ele rejeita a operação e exige confirmação explícita via `.option("overwriteSchema", "true")`.

**Time Travel**
Toda operação fica registrada no `_delta_log`. Você pode ler qualquer versão anterior dos dados:
```python
spark.read.format("delta").option("versionAsOf", 0).load("s3a://titanic/lakehouse/bronze/titanic")
```

**MERGE (Upsert)**
Atualiza registros existentes e insere novos em uma única operação atômica sem reprocessar a tabela inteira:
```python
dt.alias("silver") \
    .merge(novos_dados.alias("novo"), "silver.PassengerId = novo.PassengerId") \
    .whenMatchedUpdate(set={"Survived": "novo.Survived"}) \
    .whenNotMatchedInsert(values={...}) \
    .execute()
```

**DESCRIBE HISTORY**
Audit log completo de todas as operações realizadas na tabela:
```python
DeltaTable.forPath(spark, "s3a://titanic/lakehouse/silver/titanic").history().show()
```

**VACUUM**
Limpa versões antigas do transaction log, liberando espaço em storage:
```python
dt.vacuum(168)  # mantém últimas 168 horas (7 dias)
```

---

## Pontos de Melhoria - Próximo Nível

Este projeto usa um dataset leve e estático para fins didáticos. Um Data Lake de produção teria:

**Dado Incremental**
Novos dados chegando continuamente e sendo processados via MERGE sem reprocessar tudo.
Ferramenta: O Delta Lake suporta essa operação nativamente, reescrevendo apenas os arquivos que contêm as linhas modificadas ou novas, evitando varreduras desnecessárias no restante do repositório.

**Múltiplas Fontes**
Ingestão de APIs, bancos de dados, arquivos de diferentes sistemas convergindo no Bronze.
Ferramenta: **Apache Kafka** para streaming(processamento em tempo real) ou **Airbyte** para ingestão batch de múltiplas fontes.

**Orquestração**
Pipeline executado automaticamente em schedule ou por evento, sem intervenção manual.
Ferramenta: **Apache Airflow** - orquestrador open source que agenda e monitora pipelines e já é utilizado na empresa em um projeto. Adicionável como novo container no `docker-compose.yml`.

**Governança**
Controle de quem acessa o quê, catálogo de dados e rastreabilidade de linhagem.

O próprio MinIO já suporta isso nativamente, você cria usuários, define permissões por bucket e por operação (leitura, escrita, deleção) direto pelo painel em localhost:9001. Dá pra simular um ambiente com times diferentes acessando camadas diferentes: time de analytics só lê o Gold, pipeline de ingestão escreve no Bronze.

---

## Padrão SparkSession usado no projeto

```python
spark = SparkSession.builder \
    .appName("nome") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://titanic-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```