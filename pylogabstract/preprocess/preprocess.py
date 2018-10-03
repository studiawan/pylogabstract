from collections import OrderedDict, defaultdict
from pylogabstract.parser.parser import Parser


class Preprocess(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.raw_logs = {}
        self.parsed_logs = OrderedDict()
        self.event_attributes = {}
        self.message_length_group = defaultdict(list)

    def __get_events(self):
        # parse logs
        parser = Parser(self.log_file)
        self.parsed_logs = parser.parse_logs()
        self.raw_logs = parser.raw_logs

    def get_unique_events(self):
        # get parsed logs
        self.__get_events()

        # get graph event_attributes
        unique_events_only = []
        unique_event_id = 0

        # get unique events
        for line_id, parsed_log in self.parsed_logs.items():
            # if not exist
            if parsed_log['message'] not in unique_events_only:
                unique_events_only.append(parsed_log['message'])
                message_length = len(parsed_log['message'].split(' '))
                self.message_length_group[message_length].append(unique_event_id)
                self.event_attributes[unique_event_id] = {'message': parsed_log['message'],
                                                          'message_length': message_length,
                                                          'cluster': unique_event_id,
                                                          'member': [line_id]}
                unique_event_id += 1

            # if exist
            else:
                for index, attr in self.event_attributes.items():
                    if parsed_log['message'] == attr['message']:
                        attr['member'].append(line_id)

    def get_partial_unique_events(self, indices):
        # get unique events based on given indices
        partial_unique_events = []
        for index, attr in self.event_attributes.items():
            if index in indices:
                partial_unique_events.append((index, attr))

        return partial_unique_events
