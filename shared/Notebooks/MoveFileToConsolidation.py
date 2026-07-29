# Databricks notebook source
# DBTITLE 1,BRONZE CONSOLIDATION MOVER PIPELINE
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame, Row
from delta.tables import *

dbutils.widgets.text("FileId", "")
dbutils.widgets.text("CurrentContainer", "")
dbutils.widgets.text("CurrentFolderPath", "")
dbutils.widgets.text("ConsolidatedLayerDataModel", "")
dbutils.widgets.text("ConsolidatedLayerDataModelFilePath", "")
dbutils.widgets.text("ConsolidatedMappingFileName", "")
dbutils.widgets.text("ConsolidatedMappingFilePath", "")
dbutils.widgets.text("ConsolidatedFolderPath", "")

FileId = dbutils.widgets.get("FileId")
CurrentContainer = dbutils.widgets.get("CurrentContainer")
CurrentFolderPath = dbutils.widgets.get("CurrentFolderPath")
ConsolidatedLayerDataModel = dbutils.widgets.get("ConsolidatedLayerDataModel")
ConsolidatedLayerDataModelFilePath = dbutils.widgets.get("ConsolidatedLayerDataModelFilePath")
ConsolidatedMappingFileName = dbutils.widgets.get("ConsolidatedMappingFileName")
ConsolidatedMappingFilePath = dbutils.widgets.get("ConsolidatedMappingFilePath")
ConsolidatedFolderPath = dbutils.widgets.get("ConsolidatedFolderPath")

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

def get_struct(data_model_df):
    fields = []
    for row in data_model_df.orderBy("Ordinal").collect():
        field_name = row.FieldName
        data_type = get_data_type(row.DataType)
        fields.append(StructField(field_name, data_type, nullable=True))
    return StructType(fields)

def get_select_expr(cols_df):
    sql_commands = []
    for row in cols_df.collect():
        field_name = row.FieldName
        data_type = row.DataType
        source_column = row.SourceColumn.strip().strip("'").strip('"') if row.SourceColumn else ""
        destination_column = row.DestinationColumn
        source_column_format = row.SourceColumnFormat if row.SourceColumnFormat else ""
        column_query_raw = row.ColumnQuery if row.ColumnQuery else ""
        column_query = column_query_raw.strip().strip("'").strip('"') if column_query_raw else ""
        
        if column_query and column_query.strip():
            expr = f"NULLIF(CAST({column_query} AS {get_sql_type(data_type)}),'') AS {destination_column}"
        elif data_type == "DateType" and source_column_format:
            expr = f"to_date({source_column},'{source_column_format}') AS {destination_column}"
        elif data_type == "TimestampType" and source_column_format:
            expr = f"to_timestamp({source_column},'{source_column_format}') AS {destination_column}"
        else:
            expr = f"CAST({source_column} AS {get_sql_type(data_type)}) AS {destination_column}"
        
        sql_commands.append(expr)
    return sql_commands

def custom_select(available_cols, required_cols):
    return [
        col(column) if column in available_cols else lit(None).alias(column)
        for column in required_cols
    ]

def process_consolidation(FileId, CurrentContainer, CurrentFolderPath,
                         ConsolidatedLayerDataModel, ConsolidatedLayerDataModelFilePath,
                         ConsolidatedMappingFileName, ConsolidatedMappingFilePath,
                         ConsolidatedFolderPath):
    rJSON = {}
    double_quote = '"'
    
    try:
        try:
            ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            current_job_id = ctx.tags().get("jobId").getOrElse(lambda: "Undefined")
        except Exception:
            current_job_id = "Undefined"
        
        rJSON["CurrentJobId"] = current_job_id
        
        FullProcessed = f"{CurrentContainer}/{CurrentFolderPath}/"
        FullConsolidatedFolderPath = ConsolidatedFolderPath
        DataModelFile = f"{ConsolidatedLayerDataModelFilePath}/{ConsolidatedLayerDataModel}"
        ConsolidationMapping = f"{ConsolidatedMappingFilePath}/{ConsolidatedMappingFileName}"
        
        temp_data_model = spark.read.format("json").option("multiline", "true").load(DataModelFile)
        data_model = temp_data_model.select(explode(col("Fields"))).select(
            col("col.FieldName").alias("FieldName"),
            col("col.DataType").alias("DataType"),
            col("col.Ordinal").alias("Ordinal")
        )
        
        dest_schema = get_struct(data_model)
        df_data_model = spark.createDataFrame([], dest_schema)
        
        consolidated_mappings = spark.read.format("json").option("multiline", "true").load(ConsolidationMapping)
        temp_mappings = consolidated_mappings.select(explode(col("columnMapping"))).select(
            col("col.recordType").alias("recordType"),
            col("col.selectColumns").alias("selectColumns")
        )
        
        s_record_type = temp_mappings.select(explode(col("recordType"))).select(
            col("col.Field").alias("Field"),
            col("col.Value").alias("Value")
        )
        s_columns = temp_mappings.select(explode(col("selectColumns"))).select(
            col("col.SourceColumn").alias("SourceColumn"),
            col("col.DestinationColumn").alias("DestinationColumn"),
            col("col.SourceColumnFormat").alias("SourceColumnFormat"),
            col("col.ColumnQuery").alias("ColumnQuery")
        )
        
        seq_columns = data_model.join(
            s_columns,
            data_model["FieldName"] == s_columns["DestinationColumn"],
            "inner"
        ).select(
            data_model["FieldName"],
            data_model["DataType"],
            data_model["Ordinal"],
            s_columns["SourceColumn"],
            s_columns["DestinationColumn"],
            s_columns["SourceColumnFormat"],
            s_columns["ColumnQuery"]
        )
        
        sel_cols = get_select_expr(seq_columns)
        
        record_type_row = s_record_type.select("Field", "Value").collect()[-1]
        filter_field = "FileId" if not record_type_row.Field or record_type_row.Field == "" else record_type_row.Field
        filter_value = FileId if not record_type_row.Value or record_type_row.Value == "" else record_type_row.Value
        
        df_file_reformatted = spark.read.format("parquet").load(FullProcessed) \
            .selectExpr(*sel_cols) \
            .filter(col("FileID").cast("string") == str(FileId)) \
            .filter(col(filter_field).cast("string") == str(filter_value))
        
        df_file = df_data_model.union(
            df_file_reformatted.select(
                *custom_select(
                    df_file_reformatted.columns,
                    df_data_model.columns
                )
            )
        )
        
        record_count = df_file.count()
        
        if record_count > 0:
            df_file.write.format("delta").option("mergeSchema", "true").mode("append").save(FullConsolidatedFolderPath)
        
        rJSON["ConsolidatedCount"] = str(record_count)
        rJSON["Status"] = "SUCCESS"
        rJSON["ErrorMessage"] = ""
        
    except Exception as e:
        error_msg = str(e).replace(double_quote, "").replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()
        error_msg = ''.join(char for char in error_msg if ord(char) >= 32)
        
        rJSON["ConsolidatedCount"] = "0"
        rJSON["Status"] = "FAILED"
        rJSON["ErrorMessage"] = error_msg
    
    return json.dumps(rJSON)

if __name__ == "__main__":
    res = process_consolidation(FileId, CurrentContainer, CurrentFolderPath,
                                ConsolidatedLayerDataModel, ConsolidatedLayerDataModelFilePath,
                                ConsolidatedMappingFileName, ConsolidatedMappingFilePath,
                                ConsolidatedFolderPath)
    print(res)
    dbutils.notebook.exit(res)

