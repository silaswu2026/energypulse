FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir flask psycopg2-binary markdown

COPY server.py .

EXPOSE 5000

CMD ["python", "server.py"]
