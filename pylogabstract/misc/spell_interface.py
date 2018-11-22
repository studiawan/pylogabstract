import subprocess
from collections import defaultdict
from pylogabstract.abstraction.abstraction_utility import AbstractionUtility
from pylogabstract.misc.misc_utility import MiscUtility


class SpellInterface(object):
    def __init__(self, parsed_logs, input_directory, log_file, abstraction_json_file):
        self.parsed_logs = parsed_logs
        self.input_directory = input_directory
        self.log_file = log_file
        self.abstraction_json_file = abstraction_json_file

    def get_abstractions(self):
        # run Spell method
        subprocess.call(['python2', '/home/hudan/Git/spell/spell.py', self.input_directory, self.log_file,
                         self.abstraction_json_file])

        # get abstractions
        abstractions = AbstractionUtility.read_json(self.abstraction_json_file)
        abstractions = self.__get_final_abstraction(abstractions)

        return abstractions

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

    def __get_final_abstraction(self, abstractions):
        # restart abstraction id from 0, get abstraction and its log ids
        # in this method, we include other fields such as timestamp, hostname, ip address, etc in abstraction
        final_abstractions = {}
        abstraction_id = 0

        parsed_logs = self.parsed_logs
        for abs_id, abstraction in abstractions.items():
            # get entities from raw logs per cluster (except the main message)
            candidates = defaultdict(list)
            candidates_log_ids = defaultdict(list)
            candidates_messages = defaultdict(list)
            log_ids = abstraction['log_id']

            for log_id in log_ids:
                parsed = parsed_logs[log_id]
                values = []
                values_length = 0
                message = []
                for label, value in parsed.items():
                    if label != 'message':
                        value_split = value.split()
                        values.extend(value_split)
                        values_length += len(value_split)

                    # get the message here
                    else:
                        message_split = value.split()
                        message.extend(message_split)

                # get abstraction candidates and their respective log ids
                candidates[values_length].append(values)
                candidates_log_ids[values_length].append(log_id)
                candidates_messages[values_length].append(message)

            # get asterisk for entity and message and then set final abstraction
            for label_length, candidate in candidates.items():
                entity_abstraction = self.__get_asterisk(candidate)
                message_abstraction = self.__get_asterisk(candidates_messages[label_length])
                final_abstractions[abstraction_id] = {
                    'abstraction': entity_abstraction + ' ' + message_abstraction,
                    'log_id': candidates_log_ids[label_length]
                }
                abstraction_id += 1

        return final_abstractions


if __name__ == '__main__':
    # set input path
    dataset_path = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/'
    analyzed_file = 'auth.log'
    outputfile = '/home/hudan/Git/pylogabstract/results/' + analyzed_file
    abstraction_jsonfile = outputfile + '.json'

    # parse logs
    utility = MiscUtility()
    parsedlogs, _ = utility.write_parsed_message(dataset_path + analyzed_file, outputfile)

    msg_dir = '/home/hudan/Git/pylogabstract/results/'
    spell = SpellInterface(parsedlogs, msg_dir, analyzed_file, abstraction_jsonfile)
    abstraction_results = spell.get_abstractions()

    for k, v in abstraction_results.items():
        print(k, v)
