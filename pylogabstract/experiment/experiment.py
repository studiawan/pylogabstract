import os
import errno
import csv
from configparser import ConfigParser
from pylogabstract.abstraction.abstraction import LogAbstraction
from pylogabstract.abstraction.abstraction_utility import AbstractionUtility
from pylogabstract.output.output import Output


class Experiment(object):
    def __init__(self, method, dataset, config_file):
        self.method = method
        self.dataset = dataset
        self.config_file = config_file
        self.configuration = {}
        self.files = {}

        # initiate abstraction
        self.log_abstraction = LogAbstraction()

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
        if self.config_file:
            config_path = self.config_file
        else:
            current_path = os.path.dirname(os.path.realpath(__file__))
            config_path = os.path.join(current_path, self.config_file)

        # read configuration and save to dictionary
        parser = ConfigParser()
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
        self.__check_path(result_path)
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

        # evaluation file and directory. to save metrics results such as precision and recall
        self.files['evaluation_file'] = os.path.join(result_path,
                                                     self.configuration['experiments']['evaluation_file'])

    @staticmethod
    def __convert_to_lineid_abstractionid(abstractions):
        lineid_abstractionid = {}
        for abstraction_id, abstraction in abstractions.items():
            for lineid in abstraction['log_id']:
                lineid_abstractionid[lineid] = abstraction_id

        return lineid_abstractionid

    @staticmethod
    def __get_evaluation_metrics(groundtruth_file, prediction):
        # groundtruth and prediction has the same format= line_id: abstraction_id
        groundtruth = AbstractionUtility.read_json(groundtruth_file)
        print(groundtruth, prediction)
        precision, recall, f1, accuracy = 0., 0., 0., 0.

        metrics = {'precision': precision,
                   'recall': recall,
                   'f1': f1,
                   'accuracy': accuracy}
        return metrics

    def __run_pylogabstract(self, log_path):
        abstractions, raw_logs = self.log_abstraction.get_abstraction(log_path)
        return abstractions, raw_logs

    def __get_abstraction(self, filename, properties):
        # run experiment: get abstraction
        abstractions = {}
        raw_logs = {}
        if filename != 'evaluation_file':
            if self.method == 'pylogabstract':
                abstractions, raw_logs = self.__run_pylogabstract(properties['log_path'])

        # write result to file
        Output.write_perline(abstractions, raw_logs, properties['perline_path'])
        Output.write_perabstraction(abstractions, raw_logs, properties['perabstraction_path'])

        # update abstraction id based on ground truth and convert the format to line_id: abstraction_id
        abstractions = AbstractionUtility.get_abstractionid_from_groundtruth(properties['abstraction_withid_path'],
                                                                             abstractions)
        lineid_abstractionid_prediction = self.__convert_to_lineid_abstractionid(abstractions)

        # get evaluation metrics
        metrics = self.__get_evaluation_metrics(properties['lineid_abstractionid_path'],
                                                lineid_abstractionid_prediction)
        evaluation_metrics = (filename, metrics['precision'], metrics['recall'], metrics['f1'], metrics['accuracy'])

        return evaluation_metrics

    def run_abstraction_serial(self):
        # initialization
        self.__read_config()
        self.__get_dataset()

        # open evaluation file
        f = open(self.files['evaluation_file'], 'wt')
        writer = csv.writer(f)

        # set header for evaluation file
        header = self.configuration['experiments']['evaluation_file_header'].split('\n')
        writer.writerow(tuple(header))

        # run the experiment
        for filename, properties in self.files.items():
            metrics = self.__get_abstraction(filename, properties)
            writer.writerow(metrics)

        # close evaluation file
        f.close()


if __name__ == '__main__':
    abstraction_method = 'pylogabstract'
    dataset_name = 'casper-rw'
    conf_file = ''

    experiment = Experiment(abstraction_method, dataset_name, conf_file)
    experiment.run_abstraction_serial()
