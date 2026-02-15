# Big Data Framework Project – Data Engineering

Projet réalisé dans le cadre du module **Big Data Framework**.

Ce projet met en place un pipeline Data Engineering complet :

- Data Lake (Raw / Silver)
- Traitements Spark
- Datamarts Gold
- API REST sécurisée JWT
- Dashboard de visualisation
- Architecture Docker

---

# Architecture

## Stack technique

- Hadoop HDFS
- Hive Metastore
- Spark Standalone
- PostgreSQL (Datamarts Gold)
- FastAPI (API sécurisée JWT)
- Dash (Visualisation)
- Docker Compose

---

# Data Lake

Structure :
HDFS
├── raw/
└── silver/



- **Raw** : données sources
- **Silver** : données nettoyées et structurées

---

# Traitements Spark

Les traitements sont exécutés via 

spark-submit

Scripts principaux :

- datamart1.py
- datamart2.py
- datamart3.py
- datamart4.py

---

# Datamarts Gold

Datamarts créés dans PostgreSQL :

| Datamart | Description |
|---|---|
| gold_sales_country_year | ventes par pays et année |
| gold_top_products | top produits |
| gold_monthly_sales | ventes mensuelles |
| gold_customer_lifetime_value | valeur client |

---

# API REST (FastAPI)

API sécurisée avec JWT.

Endpoints :

POST /login
GET /gold/top-products
GET /gold/monthly-sales
GET /gold/customer-lifetime

Pagination implémentée 

Documentation Swagger : 

http://localhost:8000/docs


---

# Dashboard (Dash)

Dashboard basé sur les datamarts Gold.

Graphiques :

- Top Products
- Monthly Sales
- Customer Lifetime Value

URL :



http://localhost:8050


---

# Lancement du projet

```bash
docker compose up -d

Démonstration vidéo

** La vidéo présente :

** spark-submit

** Data Lake raw/silver

** Hive

** Spark UI

** Resource Manager

** Datamarts Gold

** API sécurisée

** Dashboard

--- 
# Choix Techniques

** Pagination API

** Implémentée via paramètres limit et offset.

** Partitionnement

** Partitionnement par date pour optimiser les lectures.

** Configuration Spark

** Spark Standalone

** Hive Metastore activé

--- 

 # Structure du projet 

docker-hadoop-spark-master/
│
├── pipeline/
│   ├── api/
│   ├── dashboard/
│   ├── datamart.py
│   ├── datamart2.py
│   ├── datamart3.py
│   ├── datamart4.py
│   ├── feeder.py
│   └── preprocessor.py
│
├── source/
│   ├── countries.csv
│   ├── customers.csv
│   ├── products.csv
│   └── transactions.csv
│
├── docker-compose.yml
├── README.md
└── .gitignore
