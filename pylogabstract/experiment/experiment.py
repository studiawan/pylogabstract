import os
import errno
from configparser import ConfigParser


class Experiment(object):
    def __init__(self, method, dataset, config_file):
        self.method = method
        self.dataset = dataset
        self.config_file = config_file
        self.configuration = {}
        self.files = {}

    @staticmethod
    def __check_path(path):
        # check a path is exist or not. if not exist, then create it
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    def __read_config(self):
        # read configuration file to run an experiment based on a specific method and a dataset
        parser = ConfigParser()
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_path, self.config_file)
        parser.read(config_path)

        for section_name in parser.sections():
            options = {}
            for name, value in parser.items(section_name):
                options[name] = value
            self.configuration[section_name] = options

    def __get_dataset(self):
        # get all log files under dataset directory and their properties
        # get full path of each filename
        dataset_path = os.path.join(self.configuration['datasets']['dataset_path'], self.dataset, 'logs')
        matches = []
        for root, dirnames, filenames in os.walk(dataset_path):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                matches.append((full_path, filename))

        # set experiment result path for a single log file path
        result_path = os.path.join(self.configuration['experiments']['result_path'], self.method)
        for full_path, filename in matches:
            self.files[filename] = {
                'log_path': full_path,
            }

        for full_path, filename in matches:
            # get all files for abstraction
            properties = {}
            for key, value in self.configuration['abstraction_result_path'].items():
                directory = os.path.join(result_path, self.dataset, value)
                self.__check_path(directory)
                properties[key] = os.path.join(directory, filename)

            # update files dictionary
            self.files[filename].update(properties)

            # get ground truth for each file
            properties = {}
            for key, value in self.configuration['abstraction_ground_truth'].items():
                properties[key] = os.path.join(dataset_path, value, filename)

            # update files dictionary
            self.files[filename].update(properties)

        self.files['evaluation_file'] = os.path.join(result_path,
                                                     self.configuration['experiments']['evaluation_file'])

    def run_abstraction_serial(self):
        self.__read_config()
        self.__get_dataset()


if __name__ == '__main__':
    abstraction_method = 'pylogabstract'
    dataset_name = 'casper-rw'
    conf_file = 'abstraction.conf'

    experiment = Experiment(abstraction_method, dataset_name, conf_file)
    experiment.run_abstraction_serial()
