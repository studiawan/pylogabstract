import os
import errno
import csv
import sys
import statistics
from configparser import ConfigParser
from sklearn.metrics import accuracy_score  # precision_score, recall_score, f1_score,
from pylogabstract.abstraction.abstraction import LogAbstraction
from pylogabstract.abstraction.abstraction_utility import AbstractionUtility
from pylogabstract.output.output import Output
from pylogabstract.misc.iplom import IPLoM, ParaIPLoM
from pylogabstract.misc.logsig import LogSig, ParaLogSig
from pylogabstract.misc.drainv1 import Drain, ParaDrain
from pylogabstract.misc.misc_utility import MiscUtility


class Experiment(object):
    def __init__(self, method, dataset, config_file):
        self.method = method
        self.dataset = dataset
        self.config_file = config_file
        self.configuration = {}
        self.files = {}

        # initiate abstraction
        if self.method == 'pylogabstract':
            self.log_abstraction = LogAbstraction()

        elif self.method in ['iplom', 'logsig', 'drain', 'logmine', 'spell']:
            self.misc_utility = MiscUtility()

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
            config_path = os.path.join(current_path, 'abstraction.conf')

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
                properties[key] = os.path.join(self.configuration['datasets']['dataset_path'], self.dataset,
                                               value, filename)

            # update files dictionary
            self.files[filename].update(properties)

        # evaluation file and directory. to save metrics results such as precision and recall
        self.files['evaluation_file'] = os.path.join(result_path, self.dataset,
                                                     self.configuration['experiments']['evaluation_file'])

    @staticmethod
    def __get_evaluation_metrics(groundtruth_file, prediction):
        # groundtruth and prediction has the same format= line_id: abstraction_id
        groundtruth = AbstractionUtility.read_json(groundtruth_file)
        groundtruth_list = list(groundtruth.values())
        prediction_list = list(prediction.values())

        # precision = precision_score(groundtruth_list, prediction_list, average='micro')
        # recall = recall_score(groundtruth_list, prediction_list, average='micro')
        # f1 = f1_score(groundtruth_list, prediction_list, average='micro')
        precision, recall, f1 = 0, 0, 0
        accuracy = accuracy_score(groundtruth_list, prediction_list)

        metrics = {'precision': round(precision, 3),
                   'recall': round(recall, 3),
                   'f1': round(f1, 3),
                   'accuracy': round(accuracy, 3)}

        return metrics

    def __run_pylogabstract(self, log_path):
        abstractions, raw_logs = self.log_abstraction.get_abstraction(log_path)
        return abstractions, raw_logs

    def __run_iplom(self, log_path, output_path):
        # get file and directory
        log_path_split = log_path.split('/')
        output_path_split = output_path.split('/')
        directory = '/'.join(output_path_split[:-1]) + '/'
        filename = log_path_split[-1]

        # write log file containing message only
        parsed_logs, _ = self.misc_utility.write_parsed_message(log_path, output_path)

        # run IPLoM method
        para = ParaIPLoM(path=directory, logname=filename, parsed_logs=parsed_logs)
        iplom = IPLoM(para)
        iplom.main_process()
        abstractions, raw_logs = iplom.get_abstraction()

        return abstractions, raw_logs

    def __run_logsig(self, log_path, output_path, groundtruth_file):
        # get input
        log_path_split = log_path.split('/')
        directory = '/'.join(output_path[:-1]) + '/'
        filename = log_path_split[-1]

        # write log file containing message only
        parsed_logs, _ = self.misc_utility.write_parsed_message(log_path, output_path)

        # get number of cluster (misc_utility)
        cluster_number = self.misc_utility.get_cluster_number(groundtruth_file)

        # run LogSig with k from ground truth
        para = ParaLogSig(path=directory, logname=filename, groupNum=cluster_number, parsed_logs=parsed_logs)
        logsig = LogSig(para)
        logsig.mainProcess()
        abstractions, raw_logs = logsig.get_abstractions()

        return abstractions, raw_logs

    def __run_drain(self, log_path, output_path):
        # get input
        log_path_split = log_path.split('/')
        directory = '/'.join(output_path[:-1]) + '/'
        filename = log_path_split[-1]

        # write log file containing message only
        parsed_logs, _ = self.misc_utility.write_parsed_message(log_path, output_path)

        # run Drain method
        para = ParaDrain(path=directory, logName=filename, st=0.5, depth=4, parsed_logs=parsed_logs)
        drain = Drain(para)
        drain.mainProcess()
        abstractions, raw_logs = drain.get_abstractions()

        return abstractions, raw_logs

    def __get_abstraction(self, filename, properties):
        # run experiment: get abstraction
        abstractions = {}
        raw_logs = {}
        if self.method == 'pylogabstract':
            abstractions, raw_logs = self.__run_pylogabstract(properties['log_path'])

        elif self.method == 'iplom':
            abstractions, raw_logs = self.__run_iplom(properties['log_path'],
                                                      properties['message_file_path'])

        elif self.method == 'logsig':
            abstractions, raw_logs = self.__run_logsig(properties['log_path'],
                                                       properties['message_file_path'],
                                                       properties['abstraction_withid_path'])

        elif self.method == 'drain':
            abstractions, raw_logs = self.__run_drain(properties['log_path'],
                                                      properties['message_file_path'])

        # write result to file
        Output.write_perline(abstractions, raw_logs, properties['perline_path'])
        Output.write_perabstraction(abstractions, raw_logs, properties['perabstraction_path'])
        Output.write_comparison(properties['abstraction_withid_path'], properties['lineid_abstractionid_path'],
                                abstractions, raw_logs, properties['comparison_path'])

        # update abstraction id based on ground truth and convert the format to line_id: abstraction_id
        lineid_abstractionid_prediction = \
            AbstractionUtility.get_abstractionid_from_groundtruth(properties['abstraction_withid_path'], abstractions)

        # get evaluation metrics
        metrics = self.__get_evaluation_metrics(properties['lineid_abstractionid_path'],
                                                lineid_abstractionid_prediction)
        evaluation_metrics = (filename, metrics['precision'], metrics['recall'], metrics['f1'], metrics['accuracy'])
        print('Accuracy :', metrics['accuracy'], '\n')

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
        accuracy = []
        for filename, properties in self.files.items():
            if filename != 'evaluation_file':
                print('Processing', filename, '...')
                metrics = self.__get_abstraction(filename, properties)
                writer.writerow(metrics)
                accuracy.append(metrics[4])

        print('Mean accuracy:', statistics.mean(accuracy))

        # close evaluation file
        f.close()


if __name__ == '__main__':
    abstraction_list = ['pylogabstract', 'iplom', 'logsig']
    dataset_list = ['casper-rw', 'dfrws-2009-jhuisi', 'dfrws-2009-nssal',
                    'dfrws-2016', 'honeynet-challenge7']

    if len(sys.argv) < 3:
        print('Please input abstraction method and dataset name.')
        print('experiment.py method_name dataset_name')
        print('Supported methods :', abstraction_list)
        print('Supported datasets:', dataset_list)
        sys.exit(1)

    else:
        abstraction_method = sys.argv[1]
        dataset_name = sys.argv[2]
        conf_file = ''

        experiment = Experiment(abstraction_method, dataset_name, conf_file)
        experiment.run_abstraction_serial()
