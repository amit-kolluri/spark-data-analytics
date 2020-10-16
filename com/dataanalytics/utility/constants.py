current_run_id = int(
    open(r"C:\Users\Amit\Documents\Projects\Pyspark\DataAnalytics\logs\last_job_run_id.txt").read(1)) + 1

log_file_name = r"C:\Users\Amit\Documents\Projects\Pyspark\DataAnalytics\logs\logs_job_run_" + str(current_run_id)
csv_file_project_1 = r"C:/Users/Amit/Documents/Projects/Pyspark/DataAnalytics/resources"
