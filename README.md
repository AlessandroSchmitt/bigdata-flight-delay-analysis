# Big Data Flight Delay Analysis

Progetto 2 del corso di **Big Data** (A.A. 2025/2026), Università degli Studi Roma Tre.

Il progetto confronta tre tecnologie dell'ecosistema Big Data sul dataset **Flight Delay Dataset — 2024**:

- Apache Spark Core (RDD)
- Apache Spark SQL
- Apache Hive con Apache Tez

Sono state implementate le analisi 3.1 e 3.2 della traccia del progetto e valutate sia in ambiente locale sia su un cluster Amazon EMR.

La relazione finale è disponibile in [`docs/final_report_AS.pdf`](docs/final_report_AS.pdf).

## Analisi implementate

### Job 1 — Statistiche delle compagnie aeree

Per ogni coppia `(compagnia, aeroporto di partenza)` vengono calcolati:

- numero totale di voli;
- ritardo minimo, massimo e medio in arrivo;
- tasso di cancellazione;
- mesi in cui la compagnia opera nell'aeroporto.

L'output usa una rappresentazione relazionale appiattita: una riga per ogni coppia `(compagnia, aeroporto)`.

### Job 2 — Report dei ritardi per aeroporto e mese

Per ogni coppia `(aeroporto di partenza, mese)` vengono calcolati:

- numero di voli nelle fasce `LOW`, `MEDIUM`, `HIGH`;
- ritardo medio in partenza e in arrivo per ciascuna fascia;
- tre cause di cancellazione o ritardo più frequenti.

Le cinque categorie di ritardo (`CARRIER`, `WEATHER`, `NAS`, `SECURITY`, `LATE_AIRCRAFT`) sono mantenute separatamente. Un volo non cancellato può quindi contribuire a più cause quando più colonne di causa contengono minuti positivi.

La semantica completa è documentata in:

- [`docs/data_contract.md`](docs/data_contract.md)
- [`docs/job1_design.md`](docs/job1_design.md)
- [`docs/job2_design.md`](docs/job2_design.md)

## Struttura del repository

```text
bigdata-flight-delay-analysis/
├── README.md
├── requirements.txt
├── dataset/
│   ├── download_dataset.py
│   ├── preprocessing.py
│   └── validate_processed.py
├── spark-core/
│   ├── job_1.py
│   └── job_2.py
├── spark-sql/
│   ├── job_1.py
│   └── job_2.py
├── hive/
│   ├── job_1.hql
│   └── job_2.hql
├── scripts/
│   ├── check_job1.py
│   ├── check_job2.py
│   ├── prepare_benchmark_inputs.py
│   ├── run_local_benchmarks.py
│   ├── run_emr_benchmarks.py
│   ├── summarize_local_benchmarks.py
│   ├── summarize_emr_benchmarks.py
│   ├── plot_local_benchmarks.py
│   └── plot_local_vs_emr.py
├── docs/
│   ├── final_report_AS.pdf
│   ├── data_contract.md
│   ├── job1_design.md
│   └── job2_design.md
└── benchmark-results/
    ├── local_runs.csv
    ├── local_summary.csv
    ├── local_environment.txt
    ├── emr_runs.csv
    ├── emr_summary.csv
    ├── emr_environment.json
    └── figures/
```

I dataset originali e preprocessati, gli output dei job, i log e i file temporanei non sono versionati e sono esclusi tramite `.gitignore`.

## Requisiti principali

Ambiente locale utilizzato per il progetto:

- Python 3.12.1
- Java 17
- PySpark / Spark 3.5.5
- Docker
- Apache Hive 4.1.0 con Apache Tez per i benchmark Hive locali
- Matplotlib 3.11.1

Installazione delle dipendenze Python:

```bash
python -m pip install -r requirements.txt
```

Per i benchmark Hive locali, `scripts/run_local_benchmarks.py` si aspetta un container Docker chiamato `hive4`, con HiveServer2 raggiungibile all'interno del container tramite `jdbc:hive2://localhost:10000/default`, la root del repository disponibile come `/workspace` e Hive configurato per usare Tez (`hive.execution.engine=tez`).

## Download del dataset

Il dataset sorgente è **Flight Delay Dataset — 2024** di Kaggle:

<https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024>

Il download può essere eseguito con:

```bash
python dataset/download_dataset.py
```

Il repository non contiene il dataset perché i file sotto `data/` sono volutamente esclusi dal versionamento.

Prima del preprocessing verificare che il CSV sorgente sia disponibile come:

```text
data/raw/flight_data_2024.csv
```

## Preprocessing e validazione

Creazione del dataset canonico:

```bash
spark-submit --master local[2] dataset/preprocessing.py \
  --input data/raw/flight_data_2024.csv \
  --output data/processed/flights_cleaned.csv
```

Validazione del risultato:

```bash
spark-submit --master local[2] dataset/validate_processed.py \
  --input data/processed/flights_cleaned.csv
```

Il dataset canonico usato negli esperimenti contiene **7.061.582 record** e 12 colonne.

Il preprocessing viene eseguito separatamente ed è escluso dai tempi dei job analitici.

## Preparazione degli input di benchmark

Gli input 10%, 50% e 100% vengono generati in modo deterministico a partire dal CSV canonico:

```bash
python scripts/prepare_benchmark_inputs.py
```

Lo script produce:

```text
data/benchmark/10pct/flights_cleaned.csv
data/benchmark/50pct/flights_cleaned.csv
data/benchmark/100pct/flights_cleaned.csv
data/benchmark/manifest.json
```

Le frazioni 10% e 50% sono selezionate tramite soglie deterministiche su hash BLAKE2b; il campione 10% è incluso nel 50%. L'input 100% coincide con il dataset canonico completo.

## Esecuzione dei job Spark

### Spark Core

Job 1:

```bash
spark-submit --master local[2] spark-core/job_1.py \
  --input data/processed/flights_cleaned.csv \
  --output outputs/spark-core/job1
```

Job 2:

```bash
spark-submit --master local[2] spark-core/job_2.py \
  --input data/processed/flights_cleaned.csv \
  --output outputs/spark-core/job2
```

### Spark SQL

Job 1:

```bash
spark-submit --master local[2] spark-sql/job_1.py \
  --input data/processed/flights_cleaned.csv \
  --output outputs/spark-sql/job1
```

Job 2:

```bash
spark-submit --master local[2] spark-sql/job_2.py \
  --input data/processed/flights_cleaned.csv \
  --output outputs/spark-sql/job2
```

## Esecuzione dei job Hive in locale

Con il container `hive4` già predisposto e la repository montata in `/workspace`, verificare prima che Hive utilizzi Tez:

```bash
docker exec hive4 beeline \
  -u jdbc:hive2://localhost:10000/default \
  -e 'SET hive.execution.engine;'
```

Il valore atteso è `hive.execution.engine=tez`.

Esempio Job 1 sul dataset canonico:

```bash
docker exec hive4 beeline \
  -u jdbc:hive2://localhost:10000/default \
  --hiveconf hive.execution.engine=tez \
  --hiveconf INPUT=file:///workspace/data/processed \
  --hiveconf OUTPUT=file:///workspace/outputs/hive/job1 \
  -f /workspace/hive/job_1.hql
```

Per i benchmark ufficiali viene usato direttamente `scripts/run_local_benchmarks.py`, che imposta automaticamente i percorsi degli input e degli output per Hive.

## Verifica dell'equivalenza degli output

I checker non fanno parte dei tempi di benchmark. Ordinano i risultati soltanto per la visualizzazione e confrontano logicamente gli output delle implementazioni.

Job 1:

```bash
spark-submit --master local[2] scripts/check_job1.py \
  --left outputs/spark-sql/job1 \
  --right outputs/spark-core/job1
```

Job 2:

```bash
spark-submit --master local[2] scripts/check_job2.py \
  --left outputs/spark-sql/job2 \
  --right outputs/spark-core/job2
```

Lo stesso controllo può essere ripetuto usando come `--right` l'output Hive.

## Benchmark locale

La campagna ufficiale utilizza:

- 2 job;
- 3 dimensioni: 10%, 50%, 100%;
- 3 tecnologie;
- 3 ripetizioni;
- rotazione dell'ordine delle tecnologie tra le ripetizioni.

Prima di eseguire il benchmark devono essere presenti gli input generati da `scripts/prepare_benchmark_inputs.py`.

Per una nuova campagna, senza modificare i risultati ufficiali già versionati, usare un file di output separato:

```bash
rm -f benchmark-results/local_runs_repro.csv
python scripts/run_local_benchmarks.py \
  --runs 3 \
  --sizes 10pct 50pct 100pct \
  --jobs 1 2 \
  --results benchmark-results/local_runs_repro.csv
```

Sintesi della nuova campagna:

```bash
python scripts/summarize_local_benchmarks.py \
  --input benchmark-results/local_runs_repro.csv \
  --output benchmark-results/local_summary_repro.csv
```

I risultati ufficiali del progetto sono conservati in:

- `benchmark-results/local_runs.csv`
- `benchmark-results/local_summary.csv`
- `benchmark-results/local_environment.txt`

## Benchmark su Amazon EMR

La campagna Cloud è stata eseguita con:

- Amazon EMR 7.13.0;
- 1 Primary Node `m5.xlarge`;
- 2 Core Nodes `m5.xlarge`;
- istanze On-Demand;
- Hadoop 3.4.2;
- Spark 3.5.6;
- Hive 3.1.3;
- Tez 0.10.2;
- YARN come resource manager;
- Amazon S3 come storage diretto per input, codice, output e log.

La dimensione del cluster è stata mantenuta costante durante l'intera campagna.

### Layout S3 richiesto dal runner

Prima dell'esecuzione, il bucket deve contenere:

```text
s3://<BUCKET>/
├── benchmark/
│   ├── 10pct/flights_cleaned.csv
│   ├── 50pct/flights_cleaned.csv
│   └── 100pct/flights_cleaned.csv
└── code/
    ├── spark-core/
    │   ├── job_1.py
    │   └── job_2.py
    ├── spark-sql/
    │   ├── job_1.py
    │   └── job_2.py
    └── hive/
        ├── job_1.hql
        └── job_2.hql
```

Esempio di caricamento, dopo avere creato il bucket e configurato AWS CLI:

```bash
aws s3 cp data/benchmark/10pct/flights_cleaned.csv s3://<BUCKET>/benchmark/10pct/flights_cleaned.csv
aws s3 cp data/benchmark/50pct/flights_cleaned.csv s3://<BUCKET>/benchmark/50pct/flights_cleaned.csv
aws s3 cp data/benchmark/100pct/flights_cleaned.csv s3://<BUCKET>/benchmark/100pct/flights_cleaned.csv
aws s3 cp spark-core/job_1.py s3://<BUCKET>/code/spark-core/job_1.py
aws s3 cp spark-core/job_2.py s3://<BUCKET>/code/spark-core/job_2.py
aws s3 cp spark-sql/job_1.py s3://<BUCKET>/code/spark-sql/job_1.py
aws s3 cp spark-sql/job_2.py s3://<BUCKET>/code/spark-sql/job_2.py
aws s3 cp hive/job_1.hql s3://<BUCKET>/code/hive/job_1.hql
aws s3 cp hive/job_2.hql s3://<BUCKET>/code/hive/job_2.hql
```

Il runner richiede un working tree Git pulito e un cluster EMR in stato `WAITING` o `RUNNING`.

Per una nuova campagna, salvando i risultati in file separati da quelli ufficiali:

```bash
rm -f benchmark-results/emr_runs_repro.csv benchmark-results/emr_environment_repro.json
python scripts/run_emr_benchmarks.py \
  --cluster-id <CLUSTER_ID> \
  --bucket <BUCKET> \
  --runs 3 \
  --sizes 10pct 50pct 100pct \
  --jobs 1 2 \
  --results benchmark-results/emr_runs_repro.csv \
  --environment benchmark-results/emr_environment_repro.json
```

Sintesi:

```bash
python scripts/summarize_emr_benchmarks.py \
  --input benchmark-results/emr_runs_repro.csv \
  --output benchmark-results/emr_summary_repro.csv
```

La metrica principale EMR è `step_seconds`, cioè l'intervallo tra `StartDateTime` ed `EndDateTime` dello step. Il tempo di coda viene registrato separatamente.

I risultati ufficiali del progetto sono conservati in:

- `benchmark-results/emr_runs.csv`
- `benchmark-results/emr_summary.csv`
- `benchmark-results/emr_environment.json`

Gli artefatti ufficiali registrano anche il commit Git utilizzato per ciascuna campagna. La campagna EMR è stata eseguita dopo l'aggiornamento di portabilità I/O necessario a preservare correttamente gli URI `s3://`; la semantica analitica dei job è rimasta invariata.

## Generazione dei grafici

Grafici dei benchmark locali:

```bash
python scripts/plot_local_benchmarks.py
```

Confronto Local vs EMR:

```bash
python scripts/plot_local_vs_emr.py
```

I grafici prodotti sono salvati in `benchmark-results/figures/` in formato PNG e PDF.

## Risultati pubblicati

Gli artefatti versionati includono sia le misure grezze sia le statistiche aggregate:

```text
benchmark-results/local_runs.csv
benchmark-results/local_summary.csv
benchmark-results/emr_runs.csv
benchmark-results/emr_summary.csv
```

Le campagne ufficiali contengono 54 misurazioni locali e 54 misurazioni EMR, tutte completate con successo.

## Riproducibilità

La riproducibilità è supportata da:

- data contract comune alle tre tecnologie;
- sampling deterministico degli input di benchmark;
- script di validazione degli output;
- rotazione dell'ordine delle tecnologie;
- tre ripetizioni per configurazione;
- salvataggio delle misure grezze e delle statistiche aggregate;
- snapshot degli ambienti locale ed EMR;
- registrazione del commit Git utilizzato per le campagne ufficiali.

Per dettagli metodologici, risultati e discussione completa fare riferimento a [`docs/final_report_AS.pdf`](docs/final_report_AS.pdf).
