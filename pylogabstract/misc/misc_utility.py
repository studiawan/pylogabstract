from pylogabstract.parser.parser import Parser
from pylogabstract.abstraction.abstraction_utility import AbstractionUtility


class MiscUtility(object):
    def __init__(self):
        self.parser = Parser()

    def __parser(self, log_file):
        # parse log files, only message field given to the method
        parsed_results, raw_results = self.parser.parse_logs(log_file)
        return parsed_results

    def write_parsed_message(self, log_file, output_file):
        # write message fields from all log entries to file
        # this file will be read by a particular method
        parsed_logs = self.__parser(log_file)
        f = open(output_file, 'w')
        for log_id, value in parsed_logs.items():
            if value['message'] not in ['', '\n', '\r\n']:
                f.write(value['message'] + '\n')
            else:
                f.write('pylogabstract: no message found' + '\n')
        f.close()

        return parsed_logs

    @staticmethod
    def get_cluster_number(groundtruth_file):
        abstractionid_abstractionstr = AbstractionUtility.read_json(groundtruth_file)
        cluster_number = len(abstractionid_abstractionstr.keys())

        return cluster_number


if __name__ == '__main__':
    logfile = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/daemon.log'
    outputfile = 'test.log'
    utility = MiscUtility()
    utility.write_parsed_message(logfile, outputfile)
