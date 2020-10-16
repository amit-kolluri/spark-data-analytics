current_run_id = int(
    open(r"C:\Users\Amit\Documents\Projects\Pyspark\DataAnalytics\resources\last_job_run_id.txt").read(1))
job_meta_data_file = open(r"C:\Users\Amit\Documents\Projects\Pyspark\DataAnalytics\resources\last_job_run_id.txt", "w+")
log_file_name = r"C:\Users\Amit\Documents\Projects\Pyspark\DataAnalytics\logs\logs_job_run_" + str(current_run_id)
csv_file_project_1 = r"C:/Users/Amit/Documents/Projects/Pyspark/DataAnalytics/resources"


def getProjectName(file_name):
    *middle, project_name = file_name.split("/")
    return project_name


def updateJobRunId():
    updated_value = str(current_run_id + 1)
    job_meta_data_file.write(updated_value)

# getProjectName(__file__)
# updateJobRunId()
