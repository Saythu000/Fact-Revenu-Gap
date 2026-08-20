# Databricks notebook source
# DBTITLE 1,BRONZE FILES TO PROCESS INGESTION PIPELINE
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

dbutils.widgets.text("ProcessedJSON", "")
ProcessedJSON = dbutils.widgets.get("ProcessedJSON")

def get_sql_type(data_type):
    type_mapping = {
        "StringType": "STRING",
        "IntegerType": "INT",
        "LongType": "BIGINT",
        "DoubleType": "DOUBLE",
        "FloatType": "FLOAT",
        "BooleanType": "BOOLEAN",
        "DateType": "DATE",
        "TimestampType": "TIMESTAMP",
        "DecimalType": "DECIMAL(38,10)"
    }
    return type_mapping.get(data_type, "STRING")

def get_data_type(data_type):
    type_mapping = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "DoubleType": DoubleType(),
        "FloatType": FloatType(),
        "BooleanType": BooleanType(),   
        "DateType": DateType(),
        "TimestampType": TimestampType(),
        "DecimalType": DecimalType(38, 10)
    }
    return type_mapping.get(data_type, StringType())

def get_struct(schema_file_df):
    fields = []
    for row in schema_file_df.orderBy("Ordinal").collect():
        field_name = row.ColumnName
        data_type = get_data_type(row.DataType)
        fields.append(StructField(field_name, data_type, nullable=True))
    return StructType(fields)

def get_select_expr(schema_file_df):
    sql_commands = []
    for row in schema_file_df.orderBy("Ordinal").collect():
        column_name = row.ColumnName
        data_type = row.DataType
        sql_format = row.Format
        sql_commands.append(
            f"NULLIF(CAST({column_name} AS {get_sql_type(data_type)}),'') AS {column_name}"
            if data_type not in ["DateType", "TimestampType"] or sql_format in [None, ""]
            else f"to_date({column_name},'{sql_format}') AS {column_name}"
            if data_type == "DateType"
            else f"to_timestamp({column_name},'{sql_format}') AS {column_name}"
        )
    return sql_commands

def process_files(json_payload_str):
    result_list = []
    double_quote = '"'
    
    try:
        data = json.loads(json_payload_str)
        payload_files = data.get("FileIds", [])
        
        try:
            ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            current_job_id = ctx.tags().get("jobId").getOrElse(lambda: "Undefined")
        except Exception:
            current_job_id = "Undefined"

        for row in payload_files:
            row_result = {
                "CurrentJobId": current_job_id,
                "FileID": str(row.get("FileID", "")),
                "FileName": str(row.get("FileName", "")),
                "FullFilePath": str(row.get("ClientContainer", "")),
                "Status": "FAILED",
                "RecordCount": "0",
                "ErrorMessage": ""
            }
            
            try:
                ClientContainer = row.get("ClientContainer", "")
                CurrentFolderPath = row.get("CurrentFolderPath", "")
                FileName = row.get("FileName", "")
                ProcessedFolderPath = row.get("ProcessedFolderPath", "")
                SchemaFileName = row.get("SchemaFileName", "")
                SchemaFilePath = row.get("SchemaFilePath", "")
                
                FullFileName = f"{ClientContainer}{CurrentFolderPath}/{FileName}"
                FullProcessedPath = ProcessedFolderPath
                SchemaFile = f"{SchemaFilePath}/{SchemaFileName}"
                
                check_path = FullFileName if FullFileName.startswith('/Volumes/') or FullFileName.startswith('dbfs:') else f"file:{FullFileName}"
                
                is_valid = False
                try:
                    if len(dbutils.fs.ls(check_path)) > 0:
                        is_valid = True
                except Exception:
                    pass

                if not is_valid:
                    row_result["ErrorMessage"] = "Data File Not Found"
                    result_list.append(row_result)
                    continue

                schema_json = spark.read.format("json").option("multiline", "true").load(SchemaFile)
                schema_df = schema_json.select(explode(col("Fields"))).select(
                    col("col.ColumnName").alias("ColumnName"),
                    col("col.DataType").alias("DataType"),
                    col("col.Format").alias("Format"),
                    col("col.Ordinal").alias("Ordinal")
                )

                dest_schema = get_struct(schema_df)
                select_expr = get_select_expr(schema_df)
                
                df_raw_csv = spark.read.format("csv") \
                    .option("header", row.get("HasHeader", "true")) \
                    .option("delimiter", row.get("ColumnDelimiter", ",")) \
                    .option("quote", row.get("TextQualifier", '"')) \
                    .load(check_path)

                df_transformed = df_raw_csv.selectExpr(*select_expr) \
                    .withColumn("FILE_ID", lit(row.get("FileID")).cast(LongType())) \
                    .withColumn("FILE_LAYOUT_ID", lit(row.get("FileLayoutID")).cast(IntegerType())) \
                    .withColumn("FILE_LAYOUT_DESCRIPTION", lit(row.get("FileLayoutDescription"))) \
                    .withColumn("CLIENT_ID", lit(row.get("ClientID"))) \
                    .withColumn("LOAD_DATETIME", current_timestamp())

                rec_count = df_transformed.count()
                
                if rec_count > 0:
                    df_transformed.write.format("parquet").mode("append").save(FullProcessedPath)
                
                row_result["Status"] = "SUCCESS"
                row_result["RecordCount"] = str(rec_count)
                row_result["ErrorMessage"] = ""
                
            except Exception as e:
                err = str(e).replace(double_quote, "").replace("\n", " ").replace("\r", " ").strip()
                row_result["ErrorMessage"] = err
                row_result["Status"] = "FAILED"
                
            result_list.append(row_result)
            
    except Exception as e:
        err = str(e).replace(double_quote, "").replace("\n", " ").replace("\r", " ").strip()
        result_list.append({
            "CurrentJobId": "Undefined",
            "FileID": "",
            "FileName": "",
            "FullFilePath": "",
            "Status": "FAILED",
            "RecordCount": "0",
            "ErrorMessage": err
        })
        
    return json.dumps(result_list)

if ProcessedJSON:
    res = process_files(ProcessedJSON)
    dbutils.notebook.exit(res)
