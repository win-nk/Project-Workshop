#Upload to Airflow
# 1. Upload from Web UI
# 2. Upload from Cloud Shell โดยใช้gsutil
'''
 # Upload = Copy to GCS   โฟลเดอร์จะมีหรือไม่มีก็ได้ ไม่ต้องสร้างก่อน
 $ gsutil cp [file.txt] gs://[BUCKET]/[folder]/
 # List file  สามารถใส่ option เพิ=มได้ เช่น-lh เพื =อดูขนาดไฟล์
 $ gsutil ls gs://[BUCKET]
 # Cat file   ทดลองอ่านไฟล์ ถ้าไฟล์ใหญ่มากสามารถใช้ |head ได้
 $ gsutil cat gs://[BUCKET]/[FILE] | head
 '''
 
import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'datath',
}

# Exercise1: Simple Pipeline - Hello World Airflow!
# รู้จักกับ Task Flow API ที่มาใหม่ใน Airflow 2.0
# เป็นวิธีการเขียน DAG แบบใหม่ ที่อ่านง่าย และทันสมัยขึ้น เหมาะสำหรับโค้ดที่เป็น PythonOperator ทั้งหมด
# ศึกษา tutorial ฉบับเต็มได้ที่นี่ https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html

@task()
def print_hello():
    print("Hello World!")
    """
    Print Hello World!
    """
    # TODO: เขียนคำสั่ง print
    

@task()
def print_date():
    """
    Print current date
    ref: https://www.w3schools.com/python/python_datetime.asp
    """
    x = datetime.datetime.now()
    print(x)
    # TODO: print เวลาปัจจุบัน


@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=['exercise'])
def exercise1_taskflow_dag():

    t1 = print_hello()
    t2 = print_date()
    t1 >> t2
    # TODO: เขียน dependency ให้ทำ t1 ก่อน t2


exercise1_dag = exercise1_taskflow_dag()
