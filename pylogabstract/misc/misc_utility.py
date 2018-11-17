from pylogabstract.parser.parser import Parser


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
        for entity, value in parsed_logs.items():
            f.write(value['message'] + '\n')
        f.close()

        return parsed_logs


if __name__ == '__main__':
    logfile = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/daemon.log'
    outputfile = 'test.log'
    utility = MiscUtility()
    utility.write_parsed_message(logfile, outputfile)
