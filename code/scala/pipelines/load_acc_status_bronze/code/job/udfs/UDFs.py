from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.column import Column
pyUDFInstance = None

def registerUDFs(spark: SparkSession):
    pass

def initialize(spark: SparkSession):
    pyUDFInstance = spark.sparkContext._jvm.io.prophecy.libs.python.UDFUtils

def createScalaList(l, spark):
    res = spark.sparkContext._jvm.scala.collection.immutable.Nil

    for x in reversed(l):
        res = getattr(res, "$colon$colon")(x)

    return res

def lookup(lookupName: str, *cols):
    _cols = createScalaList([item._jc for item in list(cols)], spark)
    lookupResult = pyUDFInstance.lookup(lookupName, _cols)

    return Column(lookupResult)

def lookup_last(lookupName: str, *cols):
    _cols = createScalaList([item._jc for item in list(cols)], spark)
    lookupResult = pyUDFInstance.lookup_last(lookupName, _cols)

    return Column(lookupResult)

def lookup_match(lookupName: str, *cols):
    _cols = createScalaList([item._jc for item in list(cols)], spark)
    lookupResult = pyUDFInstance.lookup_match(lookupName, _cols)

    return Column(lookupResult)

def lookup_count(lookupName: str, *cols):
    _cols = createScalaList([item._jc for item in list(cols)], spark)
    lookupResult = pyUDFInstance.lookup_count(lookupName, _cols)

    return Column(lookupResult)

def lookup_row(lookupName: str, *cols):
    _cols = createScalaList([item._jc for item in list(cols)], spark)
    lookupResult = pyUDFInstance.lookup_row(lookupName, _cols)

    return Column(lookupResult)

def lookup_row_reverse(lookupName: str, *cols):
    _cols = createScalaList([item._jc for item in list(cols)], spark)
    lookupResult = pyUDFInstance.lookup_row_reverse(lookupName, _cols)

    return Column(lookupResult)

def lookup_nth(lookupName: str, *cols):
    _cols = createScalaList([item._jc for item in list(cols)], spark)
    lookupResult = pyUDFInstance.lookup_nth(lookupName, _cols)

    return Column(lookupResult)
