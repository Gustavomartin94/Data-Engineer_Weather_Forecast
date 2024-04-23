# Use the official Airflow image as a parent image
FROM apache/airflow:2.9.0

# Install Python dependencies
COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT [ "/usr/bin/dumb-init","--","/entrypoint" ]

CMD []