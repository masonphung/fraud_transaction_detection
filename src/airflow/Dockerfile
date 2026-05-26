from apache/airflow:3.0.2

# Create model directory and set ownership
USER root
RUN mkdir -p /app/models

USER airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages as root (system-side)
# Uninstall conflicting 'dotenv'/'python-env' stub before installing requirements
RUN pip install -r /tmp/requirements.txt

# Switch back to airflow user for safety
USER airflow