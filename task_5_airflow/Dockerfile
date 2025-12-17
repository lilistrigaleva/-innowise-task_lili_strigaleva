FROM apache/airflow:3.1.3

# Copy requirements and ensure file is owned by 'airflow' user, then install as 'airflow'.
# Installing pip packages as non-root prevents permission issues and matches Airflow recommendations.
COPY requirements.txt /tmp/requirements.txt

USER root
# Ensure the requirements file is owned by the airflow user so the next RUN as that user works
RUN chown airflow:root /tmp/requirements.txt || true

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Switch back to the airflow user for runtime (base image already sets correct entrypoint)
USER airflow