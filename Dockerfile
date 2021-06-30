FROM python:3.7
RUN pip3 install 'apache-airflow==2.1.0'
RUN pip3 install 'virtualenv==20.4.7'
RUN airflow db init
RUN airflow users create \
    --username bluepi \
    --firstname washara \
    --lastname palanund \
    --role Admin \
    --email washara.map@gmail.com \
    --password exam1234
CMD (airflow scheduler &) && airflow webserver 