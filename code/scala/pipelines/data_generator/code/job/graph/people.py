from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def people(spark: SparkSession, in0: DataFrame) -> DataFrame:
    person_json_1 = '''{"id":1,"name":"James Jones","email":"james_earl_93@gmail.com","updated_at":"2022-05-05T12:00:00.000-05:00","addresses":[{"postal_code":"A24011","address_line1":"4638 White Pine Lane","address_line2":"Roanoke, Virginia","type":"PRIMARY"},{"postal_code":"CD36582","address_line1":"1268 George Avenue","address_line2":"Theodore, Alabama","type":"ALTERNATE"},{"postal_code":"AX85003","address_line1":"3881 Coplin Avenue","address_line2":"Phoenix, Arizona","type":"ALTERNATE"}]}'''
    person_json_2 = '''{"id":2,"name":"Jonas Smith","updated_at":"2022-05-05T16:12:44.252-05:00","email":"jjsmith89@yahoo.com","addresses":[{"postal_code":"AB33614","address_line1":"22897 Maryland Avenue, Tampa, Florida","type":"PRIMARY"},{"postal_code":"CD21321","address_line1":"559 Lake Floyd Circle, New Castle, Delaware","type":"ALTERNATE"}]}'''
    person_json_3 = '''{"id":3,"name":"Adam Boorman","email":"adamb213@gmail.com","updated_at":"2022-05-05T05:32:55.864-05:00","addresses":[{"postal_code":"XR96814","address_line1":"4558 Indiana Circle","address_line2":"Honolulu, Hawai","type":"PRIMARY"},{"postal_code":"CC47906","address_line1":"26 Glenwood Drive","address_line2":"West Lafayette, IN","type":"ALTERNATE"}]}'''
    people_df = spark.read.json(sc.parallelize([person_json_1, person_json_2, person_json_3]))
    # people_df.coalesce(1).write.json(path="dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/people/people_2022-05-05.json",mode='overwrite')
    out0 = people_df.coalesce(1)

    return out0
