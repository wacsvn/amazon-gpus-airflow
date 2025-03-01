# Amazon GPU Data ETL Pipeline

Blog post: https://adamhoportfolio.github.io/blog-posts/amazon-gpu-price-tracker-etl-pipeline-with-airflow-docker-and-postgresql-oitaz

A data pipeline that automatically fetches GPU product data from Amazon, processes it, and stores it in a PostgreSQL database using Apache Airflow for orchestration and Docker for containerization.

## 📋 Overview

This project creates an automated ETL (Extract, Transform, Load) pipeline that:
1. Scrapes Amazon search results for graphics cards/GPUs
2. Extracts key product information (title, brand, price, rating)
3. Transforms and cleans the data
4. Loads the processed data into a PostgreSQL database
5. Schedules this process to run daily

## 🚀 Features

- **Automated Data Collection**: Daily scheduled scraping of Amazon GPU listings
- **Data Processing**: Handles deduplication and data cleaning
- **Scalable Architecture**: Built with Docker for consistent deployment
- **Monitoring & Reliability**: Leverages Airflow's built-in monitoring and retry capabilities
- **Database Storage**: Structured data storage in PostgreSQL

## 🛠️ Technologies

- **Apache Airflow**: Workflow orchestration
- **Docker & Docker Compose**: Containerization
- **Python**: Core programming language
- **PostgreSQL**: Data storage
- **BeautifulSoup**: Web scraping
- **Pandas**: Data manipulation

## 📊 Data Pipeline Architecture

```
Amazon Website → Web Scraper → Data Cleaning → PostgreSQL Database
      ↑                 ↑              ↑               ↑
      └────────────── Airflow DAG ─────────────────────┘
```

## 🏁 Getting Started

### Prerequisites

- Docker and Docker Compose
- Git

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/amazon-gpu-etl.git
   cd amazon-gpu-etl
   ```

2. Build and start the Docker containers:
   ```bash
   docker-compose up -d
   ```

3. Access the Airflow web interface:
   ```
   http://localhost:8080
   ```

4. The DAG should be automatically loaded and visible in the Airflow UI.

### Configuration

- Database connection settings can be configured through Airflow connections
- The number of GPUs to scrape can be adjusted in the DAG file

## 📝 DAG Description

The main workflow consists of three tasks:

1. **fetch_gpu_data**: Extracts GPU data from Amazon
2. **create_table**: Creates a table in PostgreSQL if it doesn't exist
3. **insert_gpu_data**: Inserts the scraped data into the database

