FROM python:3.11.2

# https://github.com/python/cpython/issues/102134
WORKDIR /opt/data-endpoint
ENV VIRTUAL_ENV=/opt/data-endpoint/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install libraries.
WORKDIR /opt/data-endpoint
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy script file.
WORKDIR /opt/data-endpoint/src
COPY ./src/ ./

CMD ["bash"]
