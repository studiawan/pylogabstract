import os
import json
import errno
from configparser import ConfigParser
from pylogabstract.parser.parser import Parser


class GroundTruth(object):
    def __init__(self, dataset, datasets_config_file, wordlist_dir):
        self.dataset = dataset
        self.datasets_config_file = datasets_config_file
        self.wordlist_dir = wordlist_dir
        self.configurations = {}

    @staticmethod
    def __check_path(path):
        # check a path is exist or not. if not exist, then create it
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    @staticmethod
    def __write_to_json(file_name, dictionary):
        # write a dictionary to json file
        with open(file_name, 'w') as f:
            json.dump(dictionary, f)

    def __read_configuration(self):
        # get configuration path
        if self.datasets_config_file:
            config_path = self.datasets_config_file
        else:
            current_path = os.path.dirname(os.path.realpath(__file__))
            config_path = os.path.join(current_path, 'datasets.conf')

        # parse config file
        parser = ConfigParser()
        parser.read(config_path)

        # get all configurations
        for section_name in parser.sections():
            options = {}
            for name, value in parser.items(section_name):
                if '\n' in value:
                    value = value.split('\n')
                options[name] = value
            self.configurations[section_name] = options

        # check path for labeled directory
        self.__check_path(self.configurations[self.dataset]['labeled_dir'])

    def __read_wordlist(self, log_type):
        # read word list of particular log type for initial abstraction
        if self.wordlist_dir:
            current_path = self.wordlist_dir
        else:
            current_path = os.path.dirname(os.path.realpath(__file__))
            current_path = os.path.join(current_path, 'wordlist')

        # open word list files in the specified directory
        wordlist_path = os.path.join(current_path, log_type + '.txt')
        with open(wordlist_path, 'r') as f:
            wordlist_temp = f.readlines()

        # get word list
        wordlist = []
        for wl in wordlist_temp:
            wordlist.append(wl.strip())

        return wordlist

    def __set_label(self, log_file, labeled_file, wordlist):
        pass

    @staticmethod
    def __get_preprocessed_logs(logfile):
        parser = Parser(logfile)
        parsed_logs = parser.parse_logs()

        return parsed_logs

    def __get_perabstraction(self):
        pass

    def get_ground_truth(self):
        # initialization
        self.__read_configuration()
        logtypes = self.configurations[self.dataset + '-logtype']

        # if logtypes is string, then convert it to list
        if isinstance(logtypes, str):
            logtypes = [logtypes]

        # get abstraction ground truth per log type
        for logtype in logtypes:
            # get list of log types
            if isinstance(self.configurations[self.dataset][logtype], str):
                file_list = [self.configurations[self.dataset][logtype]]
            else:
                file_list = self.configurations[self.dataset][logtype]

            # set group/cluster label for each line in a log file
            for filename in file_list:
                # note that file extension is removed
                log_file = os.path.join(self.configurations[self.dataset]['base_dir'], filename)
                labeled_file = os.path.join(self.configurations[self.dataset]['labeled_dir'], filename)
                wordlist = self.__read_wordlist(logtype)
                self.__set_label(log_file, labeled_file, wordlist)

            # get abstraction for each group/cluster
            self.__get_perabstraction()


if __name__ == '__main__':
    datasets_config = ''
    wordlist_directory = ''
    dataset_list = ['casper-rw', 'dfrws-2009-jhuisi', 'dfrws-2009-nssal',
                    'dfrws-2016', 'honeynet-challenge5', 'honeynet-challenge7']
    dataset_name = ''

    gt = GroundTruth(dataset_name, datasets_config, wordlist_directory)
    gt.get_ground_truth()
