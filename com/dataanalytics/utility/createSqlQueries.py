import com.dataanalytics.utility.processFileOps as fileOps

import com.dataanalytics.utility.constants as const


class SqlQueries:
    def __init__(self, spark, columns_list, table_name):
        self.spark = spark
        self.columns_list = columns_list
        self.table_name = table_name

    def createTableInDB(self):
        create_table_stmt = 'create table if not exists ' + self.table_name + '('
        no_of_columns = len(self.columns_list)
        count = 1
        for i in self.columns_list:
            if count == no_of_columns:
                create_table_stmt = create_table_stmt + '\n' + i + ' varchar'
            else:
                create_table_stmt = create_table_stmt + '\n' + i + ' varchar,'
            count = count + 1

        create_table_stmt = create_table_stmt + '\n)'
        return create_table_stmt

    def getCreateTableQueryFromDF(self):

        write_to_file = fileOps.ProcessFileOps(self.spark)
        create_table_stmt = self.createTableInDB()
        write_to_file.writeTextFile(const.create_table_query_file, create_table_stmt)
