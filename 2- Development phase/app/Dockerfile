# Use a lightweight Python base image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and ensure logs are flushed immediately
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files into container
COPY . .

# No CMD here → the command will be provided by docker-compose.yml
 