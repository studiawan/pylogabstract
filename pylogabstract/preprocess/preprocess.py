from collections import defaultdict


class Preprocess(object):
    def __init__(self, parsed_logs, raw_logs):
        self.parsed_logs = parsed_logs
        self.raw_logs = raw_logs
        self.event_attributes = {}
        self.message_length_group = defaultdict(list)

    def get_unique_events(self):
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
