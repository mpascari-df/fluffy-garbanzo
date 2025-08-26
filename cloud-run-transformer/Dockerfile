# Use the official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create empty __init__.py files for proper Python module structure
RUN touch config/__init__.py

# Expose the port the app runs on
EXPOSE 8080

# Use gunicorn to serve the application
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app