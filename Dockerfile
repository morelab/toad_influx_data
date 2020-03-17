FROM python:3.8

WORKDIR /usr/src/toad_influx_data

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./toad_influx_data ./toad_influx_data
COPY ./run_server.sh ./run_server.sh

EXPOSE 80:8080

CMD [ "./run_server.sh" ]
