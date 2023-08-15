FROM python:3.8.9

ENV PYTHONUNBUFFERED True

COPY requirements.txt ./
RUN pip install -r requirements.txt

ENV APP_HOME /app
COPY base/ /app/base/
COPY services/real_time_subcriptions /app/services/real_time_subcriptions/

WORKDIR $APP_HOME
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 services.real_time_subcriptions.main:app

