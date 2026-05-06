FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir flask psycopg2-binary

COPY app.py .

ENV FLASK_APP=app.py
EXPOSE 8050

CMD ["flask", "run", "--host=0.0.0.0", "--port=8050"]
