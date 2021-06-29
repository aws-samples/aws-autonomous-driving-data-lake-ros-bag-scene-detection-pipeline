FROM python:3.7.8-slim-buster

RUN apt-get -qq update && apt-get -qq install gcc

RUN mkdir /app
COPY ./requirements.txt /requirements.txt
RUN pip install numba
RUN pip install -r requirements.txt

COPY ./app /app
WORKDIR /app

ENTRYPOINT ["python"]

CMD ["main.py"]
