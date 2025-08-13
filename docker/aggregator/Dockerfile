FROM python:3.11-slim

WORKDIR /app

COPY src/aggregator.py ./
COPY src/utils.py ./
COPY src/crypto_utils.py ./

RUN pip install --no-cache-dir pyyaml pycryptodome

COPY docker/aggregator/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]