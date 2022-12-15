FROM apache/airflow:2.4.3-python3.10
#USER root
COPY python_scripts/*  /opt/airflow/python_scripts/
COPY dags/*   /opt/airflow/dags/
COPY requirements.txt /
#USER airflow
#RUN echo $GOOGLE_APPLICATION_CREDENTIALS > /opt/airflow/keys/key.json 
#ENV GOOGLE_APPLICATION_CREDENTIALS /opt/airflow/keys/key.json

#CMD if [ -z $GOOGLE_APPLICATION_CREDENTIALS ] ; then export  GOOGLE_APPLICATION_CREDENTIALS=; else echo $GOOGLE_APPLICATION_CREDENTIALS > /opt/airflow/keys/key.json;fi



RUN pip install --no-cache-dir -r /requirements.txt
#EXPOSE 8080
#CMD["airflow","standalone"]
