import os
import json
import errno
from configparser import ConfigParser
from collections import defaultdict
from pylogabstract.preprocess.preprocess import Preprocess
from pylogabstract.output.output import Output


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

    @staticmethod
    def __get_preprocessed_logs(log_file):
        preprocess = Preprocess(log_file)
        raw_logs = preprocess.raw_logs
        message_length_group = preprocess.message_length_group
        event_attributes = preprocess.event_attributes

        return raw_logs, message_length_group, event_attributes

    def __set_abstraction_label(self, log_file, wordlist):
        # preprocessing
        raw_logs, message_length_group, event_attributes = self.__get_preprocessed_logs(log_file)

        # label each log line
        # note that write to labeled file is not ordered by line id anymore
        # as the process is based on message length group
        groups = defaultdict(list)
        for message_length, unique_event_id in message_length_group.items():
            for event_id in unique_event_id:
                for line_ids in event_attributes[event_id]['member']:
                    for line_id in line_ids:
                        log_lower = raw_logs[line_id].lower().strip()
                        flag = True
                        for index, label in enumerate(wordlist):
                            if label in log_lower:
                                groups[index].append(line_id)
                                flag = False
                                break

                        if flag:
                            print(log_lower)
                            groups[-1].append(line_id)

        return groups, raw_logs

    @staticmethod
    def __get_asterisk(candidate):
        # candidate: list of list
        abstraction = ''

        # transpose row to column
        candidate_transpose = list(zip(*candidate))
        candidate_length = len(candidate)

        if candidate_length > 1:
            # get abstraction
            abstraction_list = []
            for index, message in enumerate(candidate_transpose):
                message_length = len(set(message))
                if message_length == 1:
                    abstraction_list.append(message[0])
                else:
                    abstraction_list.append('*')

            abstraction = ' '.join(abstraction_list)

        elif candidate_length == 1:
            abstraction = ' '.join(candidate[0])

        return abstraction

    def __get_perabstraction(self, groups, raw_logs):
        abstractions = {}
        abstraction_id = 0
        for group_id, line_ids in groups.items():
            candidate = []
            for line_id in line_ids:
                candidate.append(raw_logs[line_id].split())

            abstractions[abstraction_id] = {
                'abstraction': self.__get_asterisk(candidate),
                'log_id': line_ids
            }
            abstraction_id += 1

        return abstractions

    def __save_groundtruth(self, abstractions, log_file):
        # save abstraction ground truth
        # 1. every log line with abstraction id
        # 2. list of abstraction id and its corresponding abstraction (with asterisk)
        lineid_abstractionid = {}
        abstraction_withid = {}
        for abstraction_id, abstraction in abstractions.items():
            abstraction_withid[abstraction_id] = abstraction['abstraction']
            for line_id in abstraction['original_id']:
                lineid_abstractionid[line_id] = abstraction_id

        # get directory
        lineid_abstractionid_dir = self.configurations[self.dataset]['lineid_abstractionid_dir']
        abstraction_withid_dir = self.configurations[self.dataset]['abstraction_withid_dir']

        # check path
        self.__check_path(lineid_abstractionid_dir)
        self.__check_path(abstraction_withid_dir)

        # write ground truth to json file
        self.__write_to_json(os.path.join(lineid_abstractionid_dir, log_file), lineid_abstractionid)
        self.__write_to_json(os.path.join(abstraction_withid_dir, log_file), abstraction_withid)

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
                wordlist = self.__read_wordlist(logtype)
                groups, raw_logs = self.__set_abstraction_label(log_file, wordlist)

                # get abstraction for each group/cluster
                abstractions = self.__get_perabstraction(groups, raw_logs)

                # write per abstraction
                perabstraction_file = os.path.join(self.configurations[self.dataset]['perabstraction_dir'], filename)
                Output.write_perabstraction(abstractions, raw_logs, perabstraction_file)

                # save ground truth
                self.__save_groundtruth(abstractions, filename)


if __name__ == '__main__':
    datasets_config = ''
    wordlist_directory = ''
    dataset_list = ['casper-rw', 'dfrws-2009-jhuisi', 'dfrws-2009-nssal',
                    'dfrws-2016', 'honeynet-challenge5', 'honeynet-challenge7']
    dataset_name = dataset_list[0]

    gt = GroundTruth(dataset_name, datasets_config, wordlist_directory)
    gt.get_ground_truth()