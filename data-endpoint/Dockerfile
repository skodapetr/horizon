FROM python:3.11.2

# Install libraries.
WORKDIR /opt/data-endpoint
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

# Copy script file.
WORKDIR /opt/data-endpoint/src
COPY ./src/ ./

CMD ["bash"]
