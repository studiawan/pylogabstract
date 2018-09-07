from collections import defaultdict
from pylogabstract.parser.model.ner_model import NERModel
from pylogabstract.parser.model.config import Config


class Parser(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.model = None

    def __load_pretrained_model(self):
        # create instance of config
        config = Config()

        # load pretrained model
        self.model = NERModel(config)
        self.model.build()
        self.model.restore_session(config.dir_model)

    @staticmethod
    def __get_per_entity(words_raw, ner_label):
        # one entity can contain one or more words
        entity = defaultdict(list)
        for index, label in enumerate(ner_label):
            if '-' in label:
                final_label = label.split('-')[1]
            else:
                final_label = label

            entity[final_label].append(words_raw[index])

        # one entity is now one sentence
        for final_label, words in entity.items():
            entity[final_label] = ' '.join(words)

        return entity

    def parse_logs(self):
        # parse log files using pretrained model
        self.__load_pretrained_model()
        with open(self.log_file) as f:
            for line in f:
                words_raw = line.strip().split(' ')
                ner_label = self.model.predict(words_raw)

                yield self.__get_per_entity(words_raw, ner_label)


if __name__ == "__main__":
    logfile = '/home/hudan/Git/prlogparser/datasets/casper-rw/auth.log'
    parser = Parser(logfile)
    result = parser.parse_logs()
    for r in result:
        print(r)
