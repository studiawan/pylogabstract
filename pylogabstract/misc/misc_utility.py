from pylogabstract.parser.parser import Parser


class MiscUtility(object):
    def __init__(self, log_file, output_file):
        self.log_file = log_file
        self.output_file = output_file
        self.parsed_logs = {}

    def __parser(self):
        # parse log files, only message field given to the method
        parser = Parser()
        parsed_results, raw_results = parser.parse_logs(self.log_file)
        self.parsed_logs = parsed_results

    def write_parsed_message(self):
        # write message fields from all log entries to file
        # this file will be read by a particular method
        self.__parser()
        f = open(self.output_file, 'w')
        for entity, value in self.parsed_logs.items():
            f.write(value['message'] + '\n')
        f.close()

        return self.parsed_logs


if __name__ == '__main__':
    logfile = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/daemon.log'
    outputfile = 'test.log'
    utility = MiscUtility(logfile, outputfile)
    utility.write_parsed_message()
