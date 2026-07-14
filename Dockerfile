FROM python:3.12-slim

WORKDIR /opt/ugc_api

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/ugc_api

RUN python -m pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

CMD ["gunicorn", "-w 4", "--bind", "0.0.0.0:8000", "app.main:create_app()"]
