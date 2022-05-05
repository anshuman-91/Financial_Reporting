from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, DataType, StructField, StringType, IntegerType, ByteType, FloatType, \
    ShortType, LongType, DoubleType, BinaryType, BooleanType, DateType, TimestampType, DecimalType
from datetime import datetime
import json


def readResource(path: str) -> str:
    f = open(path, "r")
    return f.read()


def defaultForDatatype(type: DataType):
    if isinstance(type, StringType):
        return ""
    elif isinstance(type, IntegerType) or isinstance(type, ShortType) or isinstance(type, LongType):
        return 0
    elif isinstance(type, ByteType):
        return 0
    elif isinstance(type, FloatType) or isinstance(type, DoubleType):
        return 0.0
    elif isinstance(type, BinaryType):
        return bytearray(0)
    elif isinstance(type, BooleanType):
        return False
    elif isinstance(type, DateType):
        return datetime.now()
    elif isinstance(type, TimestampType):
        return datetime.now()
    elif isinstance(type, DecimalType):
        return DecimalType(0)
    else:
        raise Exception(f"default value for datatype: {type} not found")


def defaultsForSchema(schema: StructType):
    pairs = list(map(lambda f: (f.name, defaultForDatatype(f.dataType)), schema.fields))
    return dict(pairs)


def createDF(spark, columns, values, schema, typeMap, port) -> DataFrame:
    defaults = defaultsForSchema(schema)
    missingColumns = list(set(defaults.keys()) - set(columns))
    allColumns = columns + missingColumns
    missingValues = list(map(lambda c: defaults[c.lower()], missingColumns))
    allValues = list(map(lambda row: row + missingValues, values))

    reorderedSchema = StructType(list(map(lambda column: StructField(column, typeMap[column]), allColumns)))
    df = spark.createDataFrame(allValues, reorderedSchema)
    return df


def createDfFromResourceFiles(spark: SparkSession, schemaDefinitionPath: str, dataPath: str, port: str) -> DataFrame:
    schemaJson = json.loads(readResource(schemaDefinitionPath))
    schema = StructType.fromJson(schemaJson)

    colDataMap = {}
    for col in schema:
        colDataMap[col.name.lower()] = col.dataType

    data = json.loads(readResource(dataPath))
    cols = []
    for col in data["columns"]:
        cols.append(col.lower())

    values = []
    for row in data["values"]:
        newRow = []
        for (value, colName) in zip(row, cols):
            type = colDataMap[colName]
            newValue = ""
            if isinstance(type, StringType):
                newValue = value
            elif isinstance(type, IntegerType) or isinstance(type, ShortType) or isinstance(type, LongType):
                newValue = 0 if value == "" else int(value)
            elif isinstance(type, ByteType):
                newValue = 0 if value == "" else str.encode(value)
            elif isinstance(type, FloatType) or isinstance(type, DoubleType):
                newValue = 0.0 if value == "" else float(value)
            elif isinstance(type, BinaryType):
                newValue = bytearray(0) if value == "" else bytearray(value)
            elif isinstance(type, BooleanType):
                newValue = False if value == "" else value == "true"
            elif isinstance(type, DateType):
                newValue = datetime.now() if value == "" else datetime.strptime(value, '%y-%m-%d')
            elif isinstance(type, TimestampType):
                newValue = datetime.now() if value == "" else datetime.strptime(value, '%y-%m-%d %H:%M:%S')
            elif isinstance(type, DecimalType):
                newValue = DecimalType(0) if value == "" else value
            else:
                raise Exception("unexpected type", type)
            newRow.append(newValue)
        values.append(newRow)

    # dataSchema = StructType(list(filter(lambda c: c.name.lower() in cols, schema.fields)))
    return createDF(spark, cols, values, schema, colDataMap, port)


def rowEquals(r1, r2):
    if len(r1) != len(r2):
        return False

    for i in range(len(r1)):
        if r1[i] != r2[i]:
            return False

    return True


def postProcess(origDf: DataFrame) -> DataFrame:
    df = origDf.na.fill("null")
    return df.sql_ctx.createDataFrame(df.rdd, df.schema)


def assertDFEquals(expectedUnsorted: DataFrame, resultUnsorted: DataFrame, maxUnequalRowsToShow: int) -> str:
    def _sort(df: DataFrame):
        return df.sort(*[col(x) for x in df.columns])

    def fetchTypes(df: DataFrame):
        return [(f.name, f.dataType) for f in df.schema.fields]

    def _assertEqualsTypes(dfExpected: DataFrame, dfActual: DataFrame):
        expectedTypes = fetchTypes(dfExpected)
        actualTypes = fetchTypes(dfActual)
        if expectedTypes != actualTypes:
            raise Exception("Types NOT equal! " + str(expectedTypes) + " != " + str(actualTypes))

    def _assertEqualsCount(dfExpected: DataFrame, dfActual: DataFrame):
        dfExpectedCount = dfExpected.rdd.count()
        dfActualCount = dfActual.rdd.count()
        if dfExpectedCount != dfActualCount:
            raise Exception(f"Length not Equal.\n{str(dfExpectedCount)} vs {dfActualCount}")

    def _assertEqualsValues(dfExpected: DataFrame, dfActual: DataFrame):
        expectedVal = dfExpected.rdd.zipWithIndex().map(lambda x: (x[1], x[0]))
        resultVal = dfActual.rdd.zipWithIndex().map(lambda x: (x[1], x[0]))

        joined = expectedVal.join(resultVal)
        unequalRDD = joined.filter(lambda x: (not rowEquals(x[1][0], x[1][1])))

        if len(unequalRDD.take(maxUnequalRowsToShow)) != 0:
            raise Exception("Expected != Actual\nMismatch: " + str(unequalRDD.take(maxUnequalRowsToShow)))

    expected = _sort(postProcess(expectedUnsorted))
    result = _sort(postProcess(resultUnsorted))

    try:
        expected.rdd.cache()
        result.rdd.cache()

        # assert equality by means of count, types and data
        _assertEqualsCount(expected, result)
        _assertEqualsTypes(expected, result)
        _assertEqualsValues(expected, result)
    except:
        raise Exception("Dataframe match error")
    finally:
        expected.rdd.unpersist()
        result.rdd.unpersist()


def assertPredicates(port: str, df: DataFrame, predicates):
    for (pred, name) in predicates:
        assert df.filter(pred).count() == df.count(), f"Predicate {name} [[`{pred}`]] not universally true for port {port}"
    return
