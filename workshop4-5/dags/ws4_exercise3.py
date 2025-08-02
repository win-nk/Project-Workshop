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
 
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


with DAG(
    "exercise3_fan_in_dag",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["exercise"]
) as dag:

    # Exercise3: Fan-in Pipeline
    # ใน exercise นี้จะได้รู้จักการเขียน task ใน pipeline ขั้นตอนเยอะขึ้น
    # ใช้ DummyOperator เป็น task จำลอง
    
    # for i in range(7):
    #     t = DummyOperator(task_id=f"task_{i}")
    
    t = [DummyOperator(task_id=f"task_{i}") for i in range(7)]

    # t0 = DummyOperator(task_id="task_0")
    # t1 = DummyOperator(task_id="task_1")
    # t2 = DummyOperator(task_id="task_2")
    # t3 = DummyOperator(task_id="task_3")
    # t4 = DummyOperator(task_id="task_4")
    # t5 = DummyOperator(task_id="task_5")
    # t6 = DummyOperator(task_id="task_6")

    [t[0], t[1], t[2]] >> t[4] >> t[6] << [t[3], t[5]]
    # [t0, t1, t2] >> t4 
    # [t3, t4, t5] >> t6
    
    # TODO: สร้าง DummyOperator เพื่อสร้าง dependency ที่ซับซ้อน ตามรูป
