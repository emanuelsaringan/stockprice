FROM python:2.7
WORKDIR /app
ADD . /app
RUN pip install -r requirements.txt
ENV MYSQL_HOST mysql
CMD python pse_loader.py
