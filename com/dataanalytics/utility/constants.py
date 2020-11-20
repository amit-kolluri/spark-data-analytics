from pyspark.sql.types import StructType, StructField, StringType

job_meta_data_file = r"C:\Users\amitk\Documents\Projects\Python\spark-data-analytics\logs\last_job_run_id.txt"
try:
    file_data = open(job_meta_data_file, 'r')
    current_run_id = int(file_data.read(1))
    print("curr job run id: ", current_run_id)
    file_data.close()

except:
    current_run_id = 0

log_file_name = r"C:\Users\amitk\Documents\Projects\Python\spark-data-analytics\logs\logs_job_run_" + str(
    current_run_id)
processedFile = r"C:\Users\amitk\Documents\Projects\Python\spark-data-analytics\processed_files\\"

csv_file_project_1 = r"C:\Users\amitk\Documents\Projects\Python\spark-data-analytics\resources\FL_insurance_sample.csv"
create_table_query_file = r"C:\Users\amitk\Documents\projects\python\spark-data-analytics\resources\create_table_query.txt"


def getProjectName(file_name):
    corrected_file_name = file_name.replace("\\", "/")
    *middle, project_name = corrected_file_name.split("/")
    return project_name


def updateJobRunId():
    updated_value = str(current_run_id)
    file_write = open(job_meta_data_file, 'w')
    file_write.write(updated_value)
    file_write.close()


def remove_all_folders(fp):
    msg = ""
    try:
        import shutil,os
        shutil.rmtree(fp)
        os.mkdir(fp)
        msg = "folder successfully deleted"
    except Exception as e:
        msg = "folder not found"

def createEmptyDF(spark):
    schema = StructType([
        StructField('empty', StringType(), True)
    ])
    df = spark.createDataFrame([], schema)
    return df

#
# print(getProjectName(__file__))
# print((current_run_id))
# updateJobRunId()
