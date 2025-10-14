# 🌍 IoT Batch Ingestion System  

This project implements a **batch data ingestion pipeline** for environmental IoT sensor data using **Python, Docker, and MongoDB**.  
It was developed as part of the *IU International University – Project: Data Engineering (DLBDSEDE02)*, Task 1: *Choose a suitable database and store the data in batches.*

---

## Overview  

The system ingests IoT sensor data (temperature, humidity, smoke, etc.) in **batches** and stores it in a **MongoDB** database for further analysis.  
Data quality, fault tolerance, and portability are ensured through:  
- **ETL processing** (cleaning + transformation)  
- **Idempotent inserts** (unique `_id` per record)  
- **Logging & metrics** for visibility  
- **Containerization** with Docker Compose  

---

## Architecture  

![System Design](ETL-pipeline.JPG)  
![ETL Diagram](Diagram.PNG)  

**Workflow:**  
1. **Sensor Data Source** → Raw dataset (CSV from Kaggle or simulated data)  
2. **Data Cleaning (ETL Stage)** → Performed using `clean_csv.py`  
3. **Batch Ingestion** → `ingest.py` loads cleaned data into MongoDB in chunks  
4. **MongoDB** → Stores processed data and creates indexes  
5. **Mongo Express** → Web interface for visualizing and validating data  
6. **Logs & Metrics** → Saved to the `/logs` folder for transparency  
7. **Docker Compose** → Automates the environment setup for full reproducibility  

---

## Setup Instructions  

### 1. Clone the Repository  
```bash
git clone https://github.com/your-username/iot-batch-ingestion.git
cd iot-batch-ingestion
```

### 2. Prepare Your Data  
Place the cleaned dataset inside the `./data` folder.  
Example:  
```
data/cleaned_IoT_data.csv
```

### 3. Build and Start the Containers  
```bash
docker-compose up --build
```

### 4. Access Services  
- **Mongo Express (UI):** [http://localhost:8081](http://localhost:8081)  
- **MongoDB:** `mongodb://root:example@localhost:27017/?authSource=admin`  

---

## Example Input Sample  

Data from `cleaned_IoT_data.csv`:  

| ts | device | co | humidity | light | lpg | motion | smoke | temp |
|----|---------|------|-----------|--------|------|---------|--------|--------|
| 1595131334 | 1c:bf:ce:15:ec:4d | 0.004341545 | 55.5 | TRUE | 0.006952255 | FALSE | 0.018426906 | 28.79999924 |
| 1595165297 | b8:27:eb:bf:9d:51 | 0.005693328 | 51.8 | FALSE | 0.00845836 | FALSE | 0.022720576 | 21.8 |
| 1595171312 | 1c:bf:ce:15:ec:4d | 0.004207499 | 65.5 | TRUE | 0.006796302 | FALSE | 0.017985713 | 28 |
| 1594754791 | 00:0f:00:70:91:0a | 0.002840089 | 79.3 | FALSE | 0.005114383 | FALSE | 0.013274837 | 19.5 |
| 1594874372 | b8:27:eb:bf:9d:51 | 0.005382723 | 47.3 | FALSE | 0.008121963 | FALSE | 0.021756672 | 23.1 |

*(Dataset truncated for illustration.)*  

---

## Example MongoDB Output  

![MongoDB Output](MongoDBouput.png)  

```json
{
    "_id": "623e0d0892a1c889967b54256e80954a47efe46e",
    "device": "1c:bf:ce:15:ec:4d",
    "timestamp": "2020-07-19T04:02:13.000Z",
    "temp": 28.799999237060547,
    "humidity": 55.5,
    "co": 0.0043415449971564,
    "smoke": 0.0184269059272954,
    "lpg": 0.006952254607111,
    "light": true,
    "motion": false
}
```

---

## Logs: `logs/ingestion.log`

```
2025-10-13 19:51:44,581 [INFO] TOTAL rows seen:      40518
2025-10-13 19:51:44,582 [INFO] TOTAL inserted:       0
2025-10-13 19:51:44,582 [INFO] TOTAL duplicates:     40518
2025-10-13 19:51:44,583 [INFO] TOTAL duration (sec): 10.83
2025-10-13 19:51:44,583 [INFO] Metrics saved to logs/metrics.json
2025-10-13 19:51:44,608 [INFO] Ingestion process completed successfully.
```

---

## Metrics: `logs/metrics.json`

```json
{
    "rows_seen": 40518,
    "inserted": 0,
    "duplicates": 40518,
    "duration_sec": 10.83,
    "timestamp": "2025-10-13 19:51:44"
}
```

---

## Verification Steps  

1. **Run:**  
   ```bash
   docker-compose up --build
   ```
2. **Open Mongo Express:**  
   [http://localhost:8081](http://localhost:8081)  
3. **Check Logs:**  
   View `logs/ingestion.log` for batch-level progress.  
4. **View Metrics:**  
   Inspect `logs/metrics.json` for ingestion summary.  
5. **Re-run Ingestion:**  
   Confirm duplicates are skipped (idempotency check).  

---

## Project Structure  

```
project-root/
│── app/
│   ├── ingest.py               # Batch ingestion script (with logging & metrics)
│   ├── clean_csv.py            # Data cleaning script
│   ├── requirements.txt        # Python dependencies
│   ├── Dockerfile              # App container build definition
│── logs/
│   ├── ingestion.log           # Detailed logs
│   ├── metrics.json            # Summary statistics 
│── data/
│   ├── cleaned_IoT_data.csv    # Input data file
│── init/
│   ├── 01-create-indexes.js    # MongoDB index initialization                
│── docker-compose.yml          # Multi-container orchestration
│── README.md                   # Documentation
```

---

## Deployment  

- Runs locally through **Docker Compose**.  
- Can easily migrate to **cloud environments** such as AWS, GCP, Azure, or Kubernetes clusters.  

---

## Author  

Developed by **Dost Muhammad**  
📚 *IU International University – Data Engineering Project (DLBDSEDE02)*  
