# POC 3 runner (Python engine): runs inside the cluster, next to StarRocks and the bucket, so no row leaves it.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ruleset.yaml .
COPY sql sql
COPY acmg acmg

USER 1000
ENTRYPOINT ["python", "-m", "acmg.exchange"]
