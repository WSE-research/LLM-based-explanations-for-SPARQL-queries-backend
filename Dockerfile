FROM python:3.8

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY server.py ./

ENTRYPOINT ["exec", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
