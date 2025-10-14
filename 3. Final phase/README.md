# IoT Batch Ingestion System  

This project implements a **batch data ingestion pipeline** for environmental IoT sensor data using **Python, Docker, and MongoDB**. It is developed as part of the *Data Engineering* project assignment (**Task 1: Choose a suitable database and store the data in batches**).  

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

![System Design](ETL-pipeline.JPG), (Diagram.PNG) 

**Workflow:**
1. **Sensor Data Source** → Raw dataset (CSV from Kaggle or simulated data)  
2. **ETL Stage** → Cleaning with `clean_csv.py`  
3. **Batch Ingestion** → `ingest.py` loads data into MongoDB in chunks  
4. **MongoDB** → Stores processed data  
5. **Mongo Express** → Simple web-based interface for data visualization  
6. **Logs & Metrics** → Output stored in `/logs` folder  
7. **Docker Compose** → Ensures reproducibility and easy deployment 

---

## Setup Instructions  

### 1. Clone the Repository  
```bash
git clone https://github.com/your-username/iot-batch-ingestion.git
cd iot-batch-ingestion
```

### 2. Prepare Your Data  
Place the cleaned_IoT_data.csv inside the `./data` folder.  
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

### 5. Input samples 
(from cleaned_IoT_data.csv)

ts	        device	            co	            humidity	    light	lpg	            motion  smoke           temp
1595131334	1c:bf:ce:15:ec:4d	0.004341545	    55.5	        TRUE	0.006952255	    FALSE	0.018426906	    28.79999924
1595165297	b8:27:eb:bf:9d:51	0.005693328	    51.8	        FALSE	0.00845836	    FALSE	0.022720576	    21.8
1595171312	1c:bf:ce:15:ec:4d	0.004207499	    65.5	        TRUE	0.006796302	    FALSE	0.017985713	    28
1594754791	00:0f:00:70:91:0a	0.002840089	    79.30000305	    FALSE	0.005114383	    FALSE	0.013274837	    19.5
1594874372	b8:27:eb:bf:9d:51	0.005382723	    47.3	        FALSE	0.008121963	    FALSE	0.021756672	    23.1
1594626571	00:0f:00:70:91:0a	0.004065401	    75.40000153	    FALSE	0.006629473	    FALSE	0.017514512	    19.39999962
1594789030	00:0f:00:70:91:0a	0.003048879	    74.5	        FALSE	0.005383692	    FALSE	0.014022829	    19.5
1594869421	1c:bf:ce:15:ec:4d	0.003898852	    55.5	        TRUE	0.00643187	    FALSE	0.016957439	    27.39999962
1594987236	1c:bf:ce:15:ec:4d	0.003745662	    76.80000305	    TRUE	0.006248045	    FALSE	0.016440253	    23.60000038
1595144975	b8:27:eb:bf:9d:51	0.005760028	    50.8	        FALSE	0.008529929	    FALSE	0.022925989	    22
1594983137	b8:27:eb:bf:9d:51	0.00589169	    53	            FALSE	0.008670535	    FALSE	0.023329887	    22.1
1594850793	00:0f:00:70:91:0a	0.003966903	    72.59999847	    FALSE	0.006512886	    FALSE	0.017185695	    18.60000038
1594660072	00:0f:00:70:91:0a	0.004877737	    75.90000153	    FALSE	0.007563297	    FALSE	0.020161943	    19.10000038
1595197342	b8:27:eb:bf:9d:51	0.005876608	    49.7	        FALSE	0.008654473	    FALSE	0.023283726	    21.4
1594708028	1c:bf:ce:15:ec:4d	0.00450707	    59.20000076	    TRUE	0.007143005	    FALSE	0.018967461	    23.70000076
1595158141	1c:bf:ce:15:ec:4d	0.004523692	    58.20000076	    TRUE	0.007162053	    FALSE	0.019021495	    25.70000076

---

### 6. MongoDB output 

![alt text](<MongoDB ouput.png>)

{
    _id: '623e0d0892a1c889967b54256e80954a47efe46e',
    device: '1c:bf:ce:15:ec:4d',
    timestamp: ISODate('2020-07-19T04:02:13.000Z'),
    temp: 28.799999237060547,
    humidity: 55.5,
    co: 0.0043415449971564,
    smoke: 0.0184269059272954,
    lpg: 0.006952254607111,
    light: true,
    motion: false
}


### logs/ingestion.log output

2025-10-13 19:51:44,581 [INFO] TOTAL rows seen:      40518
2025-10-13 19:51:44,582 [INFO] TOTAL inserted:       0
2025-10-13 19:51:44,582 [INFO] TOTAL duplicates:     40518
2025-10-13 19:51:44,583 [INFO] TOTAL duration (sec): 10.83
2025-10-13 19:51:44,583 [INFO] Metrics saved to logs/metrics.json
2025-10-13 19:51:44,584 [INFO] ============================================================
2025-10-13 19:51:44,608 [INFO] Ingestion process completed successfully.


### logs/metrics.json output

{
    "rows_seen": 40518,
    "inserted": 0,
    "duplicates": 40518,
    "duration_sec": 10.83,
    "timestamp": "2025-10-13 19:51:44"
}

---

### 7. Verification steps 

1. Run docker-compose up --build
2. Open Mongo Express:
    http://localhost:8081
3. Check Logs:
    View logs/ingestion.log for batch-level progress.
4. View Metrics:
    Inspect logs/metrics.json for ingestion summary.
5. Re-run ingestion:
    Verify duplicates are skipped (idempotency check).

---

### 8. Project Structure  

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


---

## Deployment  

- The system runs locally via Docker.  
- Can be migrated to **cloud environments** (AWS, GCP, Azure, Kubernetes) for distributed setups.  

---

## Author  
Developed by Dost (as part of IU Data Engineering assignment).  
 