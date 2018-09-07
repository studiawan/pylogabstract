from re import sub
from nltk import corpus
import multiprocessing
import datefinder


class ParallelPreprocess(object):
    def __init__(self, log_file, refine_unique_events=True, count_groups=None):
        self.log_file = log_file
        self.logs = []
        self.log_length = 0
        self.preprocessed_logs = {}
        self.unique_events = []
        self.unique_events_length = 0
        self.event_attributes = {}
        self.refine_unique_events = refine_unique_events
        self.count_groups = count_groups
        self.events_withduplicates = []
        self.events_withduplicates_length = 0
        self.log_grammar = None
        self.preprocessed_logs_groundtruth = {}

    def __call__(self, line):
        # main method called when running in multiprocessing
        return self.__get_events(line)

    def __read_log(self):
        """Read a log file.
        """
        with open(self.log_file, 'r') as f:
            self.logs = f.readlines()
        self.log_length = len(self.logs)

    @staticmethod
    def __get_events(logs_with_id):
        log_index, line = logs_with_id
        line = line.lower()

        # GET month
        matches = datefinder.find_dates(line, source=True)
        months = []
        for match in matches:
            month = sub('[^a-zA-Z]', '', match[1])
            if month:
                months.append(month)

        # only leave alphabet, maintain word split
        line = line.split()
        line_split = []
        for li in line:
            alphabet_only = sub('[^a-zA-Z]', '', li)
            line_split.append(alphabet_only)

        # GET preprocessed_event_countgroup
        # remove more than one space
        line = ' '.join(line_split)
        line = ' '.join(line.split())
        preprocessed_event_countgroup = line

        # GET preprocessed_events
        # remove word with length only 1 character
        for index, word in enumerate(line_split):
            if len(word) == 1:
                line_split[index] = ''

        # remove more than one space
        line = ' '.join(line_split)
        line = ' '.join(line.split())

        # remove stopwords
        stopwords = corpus.stopwords.words('english')
        stopwords_month = stopwords
        if months:
            stopwords_month.extend(months)

        stopwords_result = [word for word in line.split() if word not in stopwords_month]
        preprocessed_events = ' '.join(stopwords_result)
        preprocessed_events_graphedge = preprocessed_events

        preprocessed_with_id = (log_index, preprocessed_events, preprocessed_event_countgroup,
                                preprocessed_events_graphedge)
        return preprocessed_with_id

    def get_unique_events(self):
        # read logs
        self.__read_log()
        logs_with_id = []
        for index, log in enumerate(self.logs):
            logs_with_id.append((index, log))

        # run preprocessing in parallel
        total_cpu = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=total_cpu)
        events = pool.map(self, logs_with_id)
        pool.close()
        pool.join()

        # get graph event_attributes
        unique_events_only = {}
        unique_event_id = 0
        unique_events_list = []
        for log_id, event, preprocessed_event_countgroup, preprocessed_events_graphedge in events:
            event_split = event.split()
            preprocessed_event_countgroup_split = preprocessed_event_countgroup.split()
            if event not in unique_events_only.values():
                unique_events_only[unique_event_id] = event
                count_group = [preprocessed_event_countgroup_split]
                self.event_attributes[unique_event_id] = {'preprocessed_event': event_split,
                                                          'preprocessed_event_countgroup': count_group,
                                                          'preprocessed_events_graphedge':
                                                              preprocessed_events_graphedge,
                                                          'cluster': unique_event_id,
                                                          'member': [log_id]}
                unique_event_id += 1
                unique_events_list.append(event_split)

            else:
                for index, attr in self.event_attributes.items():
                    if event_split == attr['preprocessed_event']:
                        attr['member'].append(log_id)
                        if preprocessed_event_countgroup_split not in attr['preprocessed_event_countgroup']:
                            attr['preprocessed_event_countgroup'].append(preprocessed_event_countgroup_split)

            # get preprocessed logs as dictionary
            self.preprocessed_logs[log_id] = event
            self.preprocessed_logs_groundtruth[log_id] = preprocessed_event_countgroup

        # refine unique events to remove repetitive words
        if self.refine_unique_events:
            # transpose unique events list
            unique_events_transpose = map(list, zip(*unique_events_list))

            # check if each transposed list has the same elements
            true_status = []
            for index, transposed in enumerate(unique_events_transpose):
                status = all(item == transposed[0] for item in transposed)
                if status:
                    true_status.append(index)

            # remove repetitive words
            for index, attr in self.event_attributes.items():
                attr['preprocessed_event'] = \
                    [y for x, y in enumerate(attr['preprocessed_event']) if x not in true_status]
                attr['preprocessed_event'] = ' '.join(attr['preprocessed_event'])
                attr['preprocessed_events_graphedge'] = attr['preprocessed_event']

        # get unique events for networkx
        self.unique_events_length = unique_event_id
        for index, attr in self.event_attributes.items():
            self.unique_events.append((index, attr))

        return self.unique_events
