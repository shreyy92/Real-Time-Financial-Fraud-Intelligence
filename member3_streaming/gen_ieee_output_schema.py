import pyarrow.parquet as pq

table = pq.read_table("data/features/ieee_features.parquet")
names = table.schema.names

py_lines = [
    '"""PySpark StructType schema for IEEE-CIS Tier A streaming features."""',
    'from pyspark.sql.types import (',
    '    StructType, StructField, StringType, IntegerType, DoubleType, ByteType, LongType, BooleanType, ShortType',
    ')',
    '',
    'IEEE_OUTPUT_SCHEMA = StructType(['
]

type_map = {
    'int32': 'IntegerType()',
    'int64': 'LongType()',
    'int8': 'ByteType()',
    'int16': 'ShortType()',
    'double': 'DoubleType()',
    'float': 'DoubleType()',
    'string': 'StringType()',
    'bool': 'BooleanType()',
}

for field in table.schema:
    fname = field.name
    ftype = str(field.type)
    spark_type = type_map.get(ftype, 'StringType()')
    py_lines.append(f'    StructField("{fname}", {spark_type}, True),')

py_lines.append('])\n')

with open("member3_streaming/ieee_output_schema.py", "w") as f:
    f.write("\n".join(py_lines))

print(f"Generated member3_streaming/ieee_output_schema.py with {len(names)} fields.")
