import os
import findspark

os.environ["SPARK_HOME"] = "C:\spark"
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-11" 

findspark.init()

from pyspark.sql import SparkSession
'''
ทำไมเอา from pyspark.sql import SparkSession  
ขึ้นก่อน findspark.init() ถึง เกิด 
TypeError: 'JavaPackage' object is not callable
'''
'''
เพราะ Python ยังไม่รู้จัก Spark ที่อยู่ในเครื่องก่อน findspark.init() ทำงาน
การ import SparkSession จะดึง Java gateway มาทำงานทันที 
ซึ่งยังไม่มีการตั้งค่า SPARK_HOME ไว้ ทำให้ Spark ไม่สามารถเชื่อมต่อกับ Java ได้ 
→ จึงเกิด error 'JavaPackage' object is not callable
'''
spark = SparkSession.builder \
    .appName("Test Spark from VS Code") \
    .getOrCreate()

print("✅ Spark version:", spark.version)
