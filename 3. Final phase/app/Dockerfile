# ===============================================================
# Dockerfile: IoT Batch Ingestion System
# ---------------------------------------------------------------
# This image packages the Python environment for batch ingestion.
# It performs the ETL (cleaning + ingestion) tasks and connects
# to MongoDB as defined in docker-compose.yml.
# The image is lightweight, reproducible, and platform-independent.
# ===============================================================

# ---------------------------------------------------------------
# 1. Base Image
# Use a lightweight Python image to reduce size and build time
# ---------------------------------------------------------------
FROM python:3.11-slim

# ---------------------------------------------------------------
# 2. Set Working Directory
# All subsequent operations occur inside /app
# ---------------------------------------------------------------
WORKDIR /app

# ---------------------------------------------------------------
# 3. Copy Dependencies and Install
# requirements.txt contains Python libraries needed for ETL
# ---------------------------------------------------------------
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------
# 4. Copy Application Source Code
# Copy the entire app folder containing scripts and data models
# ---------------------------------------------------------------
COPY app/ /app/
COPY data/ /app/data/

# ---------------------------------------------------------------
# 5. Environment Variables (Optional Defaults)
# These can be overridden by docker-compose.yml
# ---------------------------------------------------------------
ENV MONGODB_URI="mongodb://root:example@mongo:27017/?authSource=admin" \
    MONGODB_DB="iot" \
    MONGODB_COLLECTION="measurements" \
    CHUNK_SIZE=50000 \
    CSV_ENCODING="utf-8" \
    CSV_SEP="," \
    EPOCH_UNIT="auto"

# ---------------------------------------------------------------
# 6. Default Command
# Runs the batch ingestion process automatically when the container starts
# ---------------------------------------------------------------
CMD ["python", "-u", "ingest.py", "--file", "/app/data/cleaned_IoT_data.csv", "--chunk-size", "50000"]

# ---------------------------------------------------------------
# End of Dockerfile
# ---------------------------------------------------------------
