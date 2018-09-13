from collections import OrderedDict
from pylogabstract.parser.parser import Parser


class Preprocess(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.parsed_logs = OrderedDict()
        self.unique_events = []
        self.unique_events_length = 0
        self.event_attributes = {}

    def __get_events(self):
        # parse logs
        parser = Parser(self.log_file)
        self.parsed_logs = parser.parse_logs()

    def get_unique_events(self):
        # get parsed logs
        self.__get_events()

        # get graph event_attributes
        unique_events_only = []
        unique_event_id = 0

        # get unique events
        for line_id, parsed_log in self.parsed_logs.items():
            if parsed_log['message'] not in unique_events_only:
                unique_events_only.append(parsed_log['message'])
                self.event_attributes[unique_event_id] = {'message': parsed_log['message'],
                                                          'message_length': len(parsed_log['message'].split(' ')),
                                                          'cluster': unique_event_id,
                                                          'member': [line_id]}
                unique_event_id += 1
            else:
                for index, attr in self.event_attributes.items():
                    if parsed_log['message'] == attr['message']:
                        attr['member'].append(line_id)

        # get unique events for networkx
        self.unique_events_length = unique_event_id
        for index, attr in self.event_attributes.items():
            self.unique_events.append((index, attr))

        return self.unique_events
