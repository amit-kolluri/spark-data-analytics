from datetime import datetime, tzinfo
import logging
import com.dataanalytics.utility.constants as constants


class MessageLogger:

    def __init__(self, project_name, msg):
        self.project_name = project_name
        self.msg = msg
        logging.basicConfig(filename=constants.log_file_name, level=logging.INFO)
        # logging.warning('This will get logged to a file')
        # print(constants.log_file_name)

    def logInfo(self, msg):
        logging.info("'%s'  ::  '%s'    ::  '%s'" % (datetime.now(), self.project_name, msg))

    def logError(self, msg):
        logging.error("'%s'  ::  '%s'    ::  '%s'" % (datetime.now(), self.project_name, msg))

    def updateJobRunId(self):
        print("Aaaa")

#
# def main():
#     ml = MessageLogger("aa", "hyiuaysd")
#     ml.logInfo()
#
#
# if __name__ == "__main__":
#     main()
