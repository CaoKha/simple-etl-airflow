# Use the official Airflow image
FROM apache/airflow:2.10.4-python3.10

# Switch to the airflow user
USER airflow

# Upgrade pip and install uv
RUN pip install --upgrade pip && pip install uv

# Copy the pyproject.toml file into the container
COPY pyproject.toml /opt/airflow/

# Install dependencies using uv
RUN uv pip install -r <(uv pip compile /opt/airflow/pyproject.toml)

# # Copy the dags and data directories into the container
# COPY ./dags/ /opt/airflow/dags/
# COPY ./data/ /opt/airflow/data/

