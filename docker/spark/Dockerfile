FROM apache/spark:3.5.9-scala2.12-java11-python3-r-ubuntu

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
USER spark