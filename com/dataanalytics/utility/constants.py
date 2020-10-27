job_meta_data_file = r"C:/Users/amitk/PycharmProjects/spark-data-analytics/logs/last_job_run_id.txt"
try:
    file_data = open(job_meta_data_file, 'r')
    current_run_id = int(file_data.read(1))
    print("curr: " , current_run_id)
    file_data.close()

except:
    current_run_id = 0

log_file_name = r"C:/Users/amitk/PycharmProjects/spark-data-analytics/logs/logs_job_run_" + str(current_run_id)

csv_file_project_1 = r"C:/Users/amitk/PycharmProjects/spark-data-analytics/resources"


def getProjectName(file_name):
    corrected_file_name = file_name.replace("\\", "/")
    *middle, project_name = corrected_file_name.split("/")
    return project_name


def updateJobRunId():
    updated_value = str(current_run_id + 1)
    file_write = open(job_meta_data_file, 'w')
    file_write.write(updated_value)
    file_write.close()


#
# print(getProjectName(__file__))
# print((current_run_id))
# updateJobRunId()
