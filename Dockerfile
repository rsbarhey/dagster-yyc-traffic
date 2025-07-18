FROM python:3.12.11-alpine

WORKDIR /app

COPY ./requirements.txt /app

RUN pip install -r requirements.txt

COPY . /app

EXPOSE 3000

CMD [ "dg", "dev", "--host", "0.0.0.0"]