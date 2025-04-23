FROM docker.io/python:3.12-slim

WORKDIR /app

VOLUME /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "src/main.py"]
