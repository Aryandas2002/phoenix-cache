FROM python:3.12-slim
WORKDIR /app
COPY cache.py .
USER 65534:65534
ENTRYPOINT ["python", "cache.py"]
