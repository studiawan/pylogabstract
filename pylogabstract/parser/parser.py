from collections import OrderedDict
from pylogabstract.parser.model.ner_model import NERModel
from pylogabstract.parser.model.config import Config


class Parser(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.model = None
        self.config = None
        self.master_label = {}
        self.parsed_logs = OrderedDict()

    def __load_pretrained_model(self):
        # create instance of config
        self.config = Config()

        # load pretrained model
        self.model = NERModel(self.config)
        self.model.build()
        self.model.restore_session(self.config.dir_model)

    def __load_label(self):
        # load NER label and its corresponding human-readable field label
        with open(self.config.label_file, 'r') as f:
            label = f.readlines()

        labels = {}
        for line in label:
            line_split = line.split(' ')
            ner_label, final_label = line_split[0], line_split[1]
            labels[ner_label] = final_label.rstrip()

        return labels

    def __get_per_entity(self, words_raw, ner_label):
        # one entity can contain one or more words
        entity = OrderedDict()
        for index, label in enumerate(ner_label):
            if '-' in label:
                main_label = label.split('-')[1]
            else:
                main_label = label

            if main_label not in entity.keys():
                entity[main_label] = []

            entity[main_label].append(words_raw[index])

        # one entity is now one sentence
        final_entity = OrderedDict()
        for main_label, words in entity.items():
            final_label = self.master_label[main_label]
            final_entity[final_label] = ' '.join(words)

        if 'message' not in final_entity.keys():
            final_entity['message'] = ''

        return final_entity

    def parse_logs(self):
        # parse log files using pretrained model
        self.__load_pretrained_model()
        self.master_label = self.__load_label()
        with open(self.log_file) as f:
            for line_index, line in enumerate(f):
                words_raw = line.strip().split()
                ner_label = self.model.predict(words_raw)

                parsed = self.__get_per_entity(words_raw, ner_label)
                self.parsed_logs[line_index] = parsed

        return self.parsed_logs


if __name__ == "__main__":
    logfile = '/home/hudan/Git/prlogparser/datasets/casper-rw/debug'
    parser = Parser(logfile)
    result = parser.parse_logs()
    # for line_id, parsed_entry in result.items():
    #     print(line_id, parsed_entry)
