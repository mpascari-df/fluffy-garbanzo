# Use the official lightweight Python image as a base image.
# https://hub.docker.com/_/python
FROM python:3.10-slim

# Set the working directory in the container to /app
WORKDIR /app

# Copy the dependency file and install the dependencies
# We copy this first to leverage Docker's layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application source code to the container
COPY . .

# Set the PORT environment variable. Cloud Run will automatically set this.
# The application needs to listen on this port.
ENV PORT 8080

# Run the command that starts the web application.
# This uses Gunicorn, a production-ready WSGI server.
# The `main` refers to our main.py file, and `app` refers to the Flask app instance within it.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
