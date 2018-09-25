from itertools import combinations


class LogAbstraction(object):
    def __init__(self):
        self.clusters = {}
        self.event_attributes = {}
        self.temp_abstractions = {}
        self.abstractions = {}

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

    @staticmethod
    def __check_asterisk(abstractions):
        # check for abstraction that potentially can be merged with another abstraction
        # there is a flag check=True in self.clusters
        # the check is run in abstraction per message_length group
        # checked_abstraction = {cluster_id: abstraction, ...}
        checked_abstraction = {}
        for cluster_id1, cluster_id2 in combinations(abstractions.keys(), 2):
            if abstractions[cluster_id1]['check'] or abstractions[cluster_id2]['check']:
                abstraction1 = abstractions[cluster_id1]['abstraction'].split()
                abstraction2 = abstractions[cluster_id2]['abstraction'].split()

                # symmetric difference
                sym_diff = list(set(abstraction1) ^ set(abstraction2))
                centre_index = int(len(sym_diff)/2)
                sym_diff1 = sym_diff[:centre_index]
                sym_diff2 = sym_diff[centre_index:]

                # check for merge
                merge = False
                for word1, word2 in zip(sym_diff1, sym_diff2):
                    if word1 == word2:
                        merge = True
                    elif (word1 != word2) and (word1 == '*' or word2 == '*'):
                        merge = True
                    elif (word1 != word2) and (word1 != '*') and (word2 != '*'):
                        merge = False
                        break

                # merge
                if merge:
                    pass
                else:
                    pass

        return checked_abstraction

    def __get_all_asterisk(self):
        # main loop to get asterisk
        # abstractions[message_length] = {cluster_id: abstraction, ...}
        candidate = []
        abstractions = {}
        for message_length, clusters in self.clusters.items():
            abstraction = {}
            for cluster_id, cluster in clusters.items():
                for node in cluster['nodes']:
                    message = self.event_attributes[node]['message'].split()
                    candidate.append(message)

                abstraction[cluster_id] = {'abstraction':  self.__get_asterisk(candidate),
                                           'nodes': cluster['nodes'],
                                           'check': cluster['check']}

            checked_abstraction = self.__check_asterisk(abstraction)
            abstractions[message_length] = checked_abstraction

        return abstractions

    def __get_final_abstraction(self):
        # restart abstraction id from 0, get abstraction and its log ids
        # self.abstractions is used here
        pass

    def get_abstraction(self):
        # get clusters and event attributes
        # self.clusters[message_length] = {cluster_id: {'nodes': list, 'check': bool}, ...}
        self.clusters = {}
        self.event_attributes = {}

        # get abstraction
        self.__get_all_asterisk()
        self.__get_final_abstraction()

        # self.abstractions[abstraction_id] = {'abstraction': str, 'logid': [int, ...]}
        return self.abstractions


if __name__ == '__main__':
    log_abstraction = LogAbstraction()
    abstraction_results = log_abstraction.get_abstraction()
