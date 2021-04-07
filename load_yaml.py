import yaml
from application_logging import logger

class yaml_ops():

    def __init__(self):
        self.logger_object = logger.App_Logger()
        self.file_object = "yaml"

    def load_file(self,file_name):
        try:
            with open(file_name,"r") as file:
                config = yaml.safe_load(file)
                self.logger_object.log(self.file_object, "{} load successfully".format(file_name))
                return config

        except Exception as e:
            self.logger_object.log(self.file_object,"Error:: {} in loading {} file".format(e,file_name))
            raise e
