# IoT Batch Ingestion System  

This project implements a **batch data ingestion pipeline** for environmental IoT sensor data using **Python, Docker, and MongoDB**. It is developed as part of the *Data Engineering* project assignment (**Task 1: Choose a suitable database and store the data in batches**).  

---

## Architecture  

![System Design](Design-perfect.jpg)  

**Workflow:**  
1. **Sensors Data** (from Kaggle or other sources)  
2. **Data Ingestion** → Python script for batch loading  
3. **Batch Processing (ETL)** → Cleaning & transformation  
4. **MongoDB** → Storage of ingested data  
5. **Mongo Express** → Web-based database UI  
6. **Query Service** → Data accessible for applications  
7. **Containerized with Docker** → Portable and cloud-ready  

---

## Features  
- Containerized setup with **Docker & Docker Compose**  
- **MongoDB** as the database (scalable, flexible schema)  
- **Python scripts** for ETL (cleaning + ingestion)  
- **Batch ingestion** with configurable chunk size  
- **Mongo Express** for database visualization  
- Ready for migration to cloud or distributed environments  

---

## Setup Instructions  

### 1. Clone the Repository  
```bash
git clone https://github.com/your-username/iot-batch-ingestion.git
cd iot-batch-ingestion
```

### 2. Prepare Your Data  
Place your sensor data (CSV) inside the `./data` folder.  
Example:  
```
data/cleaned_IoT_data.csv
```

### 3. Build and Start the Containers  
```bash
docker-compose up --build
```

### 4. Access Services  
- **Mongo Express (DB UI):** [http://localhost:8081](http://localhost:8081)  
- **MongoDB:** available at `mongodb://root:example@localhost:27017/?authSource=admin`  

---

## Configuration  

### Environment Variables  
Defined in `docker-compose.yml`:  

- `MONGODB_URI` → Database connection URI  
- `MONGODB_DB` → Database name (`iot`)  
- `MONGODB_COLLECTION` → Collection name (`measurements`)  

### Ingestion Command  
From `docker-compose.yml`:  
```yaml
command: ["python", "-u", "ingest.py", "--file", "/app/data/cleaned_IoT_data.csv", "--chunk-size", "50000"]
```

You can adjust:  
- `--file` → Path to input data file  
- `--chunk-size` → Number of rows per batch  

---

## Example Workflow  

1. Place raw dataset into `data/`  
2. Run `docker-compose up --build`  
3. Data is **cleaned** (ETL step) and **loaded into MongoDB** in chunks  
4. Verify data via **Mongo Express UI**  

---

## Project Structure  

```
project-root/
│── app/
│   ├── ingest.py               # Batch ingestion script
│   ├── clean_csv.py            # Data cleaning script
│   ├── Dockerfile              # App Docker image definition
│   ├── requirements.txt        # Python dependencies
│── data/                       # Input data folder
│   ├── cleaned_IoT_data.csv    # Input csv file
│── init/                       # MongoDB init scripts
│   ├── 01-create-indexes.js
│── docker-compose.yml          # Multi-container setup
│── README.md                   # Documentation
```

---

## Deployment  

- The system runs locally via Docker.  
- Can be migrated to **cloud environments** (AWS, GCP, Azure, Kubernetes) for distributed setups.  

---

## Author  
Developed by Dost (as part of IU Data Engineering assignment).  
 