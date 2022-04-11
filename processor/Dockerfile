FROM python:latest

LABEL Maintainer="valdo.masuero@gmail.com"

WORKDIR /usr/app/src

RUN pip3 install pandas
RUN pip3 install numpy
RUN pip3 install networkx
RUN pip3 install openpyxl


COPY LOG_MSG.py ./
COPY SCHEDULERS.py ./
COPY SERVERS.py ./
COPY TASKS.py ./
COPY VMs.py ./
COPY processor.py ./


ENTRYPOINT [ "python3", "processor.py","-f"]
