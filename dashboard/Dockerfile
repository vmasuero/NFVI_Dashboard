FROM python:latest

LABEL Maintainer="valdo.masuero@gmail.com"

WORKDIR /usr/app/src

RUN pip3 install pandas
RUN pip3 install numpy
RUN pip install dash
RUN pip install openpyxl
RUN pip3 install dash-bootstrap-components

COPY COLORS.py ./
COPY dashboard.py ./

EXPOSE 8151

ENTRYPOINT [ "python3", "dashboard.py","-f"]

