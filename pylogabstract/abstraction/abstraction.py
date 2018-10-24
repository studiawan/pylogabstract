import sys
from itertools import combinations
from collections import defaultdict, OrderedDict
# from pylogabstract.clustering.clustering import LogClustering
from pylogabstract.clustering.recursion_clustering import LogClustering
from pylogabstract.preprocess.hamming_similarity import HammingSimilarity
from pylogabstract.parser.parser import Parser
from pylogabstract.output.output import Output


class LogAbstraction(object):
    def __init__(self):
        self.abstractions_nonmerge = defaultdict(dict)
        self.abstractions_nonmerge_id = 0
        self.word_check = []

        # initiate log parsing
        self.parser = Parser()

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
    def __check_total_asterisk(abstraction1, abstraction2, cluster_id1, cluster_id2):
        # parent_id = smaller cluster merge into this cluster
        # child_id = merged cluster, not processed anymore
        total1 = 0
        total2 = 0
        for word1, word2 in zip(abstraction1, abstraction2):
            if word1 == '*':
                total1 += 1
            if word2 == '*':
                total2 += 1

        parent_id, child_id = -1, -1
        parent_abstraction, child_abstraction = [], []
        if total1 > total2:
            parent_id = cluster_id1
            child_id = cluster_id2
            parent_abstraction = abstraction1
            child_abstraction = abstraction2
        elif total1 < total2:
            parent_id = cluster_id2
            child_id = cluster_id1
            parent_abstraction = abstraction2
            child_abstraction = abstraction1
        elif (total1 == total2) and (total1 > 0) and (total2 > 0):
            parent_id = cluster_id1
            child_id = cluster_id2
            parent_abstraction = abstraction1
            child_abstraction = abstraction2

        return parent_abstraction, child_abstraction, parent_id, child_id

    @staticmethod
    def __get_partial_logs(nodes, event_attributes, parsed_logs, raw_logs):
        partial_parsed_logs = OrderedDict()
        partial_raw_logs = {}
        partial_event_attributes = {}
        for node in nodes:
            for line_index in event_attributes[node]['member']:
                partial_parsed_logs[line_index] = parsed_logs[line_index]
                partial_raw_logs[line_index] = raw_logs[line_index]

            partial_event_attributes[node] = event_attributes[node]

        return partial_parsed_logs, partial_raw_logs, partial_event_attributes

    def __check_word(self, word):
        if word not in self.word_check:
            alphabet_count = 0
            digit_count = 0
            for character in word:
                if character.isalpha():
                    alphabet_count += 1

                if character.isdigit():
                    digit_count += 1

            if alphabet_count == 1 or digit_count == 1 or digit_count == len(word):
                self.word_check.append(word)
                word = '*'

        else:
            word = '*'

        return word

    @staticmethod
    def __check_over_abstraction(abstraction):
        # check if there are too many asterisk in an abstraction
        abstraction_split = abstraction.split()
        abstraction_len = len(abstraction_split)

        asterisk_count = 0
        for word in abstraction_split:
            if word == '*':
                asterisk_count += 1

        over_abstraction = False
        if (asterisk_count == abstraction_len - 1) and (abstraction_len > 3):
            over_abstraction = True

        return over_abstraction

    def __get_all_asterisk(self, all_clusters, event_attributes, parsed_logs, raw_logs):
        # main loop to get asterisk
        # abstractions[message_length] = {cluster_id: abstraction, ...}
        for message_length, clusters in all_clusters.items():
            abstraction = {}
            for cluster_id, cluster in clusters.items():
                candidate = []
                for node in cluster['nodes']:
                    message = event_attributes[node]['message'].split()
                    candidate.append(message)

                asterisk = self.__get_asterisk(candidate)
                check_asterisk = set(asterisk.replace(' ', ''))
                over_abstraction = self.__check_over_abstraction(asterisk)

                # run clustering again if abstraction is all asterisk, such as * * * * *
                if check_asterisk == {'*'} or over_abstraction:
                    # get partial data only for the cluster that has all asterisk
                    partial_parsed_logs, partial_raw_logs, partial_event_attributes = \
                        self.__get_partial_logs(cluster['nodes'], event_attributes, parsed_logs, raw_logs)
                    partial_message_length_group = {
                        message_length: cluster['nodes']
                    }

                    # clustering again
                    log_clustering = LogClustering(partial_parsed_logs, partial_raw_logs,
                                                   partial_message_length_group, partial_event_attributes)
                    sub_cluster = log_clustering.get_clustering()

                    # recursion to get asterisk
                    self.__get_all_asterisk(sub_cluster, event_attributes, parsed_logs, raw_logs)

                else:
                    abstraction[self.abstractions_nonmerge_id] = {'abstraction': asterisk,
                                                                  'nodes': cluster['nodes'],
                                                                  'check': cluster['check']}
                    self.abstractions_nonmerge_id += 1

            # save all abstraction before merging here
            self.abstractions_nonmerge[message_length].update(abstraction)

    def __merge_abstraction(self, abstractions):
        checked_abstractions = {}
        cluster_id = 0
        checked_cluster_id = []
        checked_parent_id = []
        valid_combinations = []
        not_merge_id = []
        abstractionstr_abstractionid = {}

        # get abstraction that will not be checked (merged)
        for original_cluster_id, abstraction in abstractions.items():
            if abstraction['check'] is False:
                checked_abstractions[cluster_id] = {
                    'abstraction': abstractions[original_cluster_id]['abstraction'],
                    'nodes': abstractions[original_cluster_id]['nodes']
                }
                checked_cluster_id.append(original_cluster_id)
                cluster_id += 1

        # get valid combinations
        for cluster_id1, cluster_id2 in combinations(abstractions.keys(), 2):
            if abstractions[cluster_id1]['check'] and abstractions[cluster_id2]['check']:
                valid_combinations.append((cluster_id1, cluster_id2))

        # if there is valid combinations
        if valid_combinations:
            for cluster_id1, cluster_id2 in valid_combinations:
                if (cluster_id1 not in checked_cluster_id) and (cluster_id2 not in checked_cluster_id):
                    hs = HammingSimilarity()
                    hamming_similarity = hs.get_weighted_hamming(abstractions[cluster_id1]['abstraction'],
                                                                 abstractions[cluster_id2]['abstraction'])
                    if hamming_similarity > 0.:
                        abstraction1 = abstractions[cluster_id1]['abstraction'].split()
                        abstraction2 = abstractions[cluster_id2]['abstraction'].split()

                        # check parent and child abstraction
                        parent_abstraction, child_abstraction, parent_id, child_id = \
                            self.__check_total_asterisk(abstraction1, abstraction2, cluster_id1, cluster_id2)

                        # check for merge
                        # once merge = False, it will not continue checking
                        merge = False
                        parent_abstraction_check = []
                        for word1, word2 in zip(parent_abstraction, child_abstraction):
                            word1_check = self.__check_word(word1)
                            word2_check = self.__check_word(word2)
                            parent_abstraction_check.append(word1_check)

                            if word1 == word2:
                                merge = True

                            elif (word1 != word2) and (word1 == '*'):
                                merge = True

                            elif (word1 != word2) and (word1 != '*') and (word2 != '*'):
                                if word1_check == word2_check:
                                    merge = True
                                else:
                                    merge = False
                                    break

                            elif (word1 != word2) and (word1 != '*') and (word2 == '*'):
                                if word1_check == word2:
                                    merge = True
                                else:
                                    merge = False
                                    break

                        # merge abstractions
                        if merge:
                            # change asterisk here
                            if (parent_id != -1) and (child_id != -1):
                                abstractions[parent_id]['abstraction'] = ' '.join(parent_abstraction_check)
                                if abstractions[parent_id]['abstraction'] in abstractionstr_abstractionid.keys():
                                    existing_id = abstractionstr_abstractionid[abstractions[parent_id]['abstraction']]
                                    checked_abstractions[existing_id]['nodes'].extend(abstractions[child_id]['nodes'])

                                else:
                                    checked_abstractions[cluster_id] = {
                                        'abstraction': abstractions[parent_id]['abstraction'],
                                        'nodes': abstractions[cluster_id1]['nodes'] + abstractions[cluster_id2]['nodes']
                                    }
                                    abstractionstr_abstractionid[abstractions[parent_id]['abstraction']] = cluster_id
                                    cluster_id += 1

                                checked_cluster_id.append(child_id)
                                checked_parent_id.append(parent_id)

                            else:
                                not_merge_id.extend([cluster_id1, cluster_id2])

                        else:
                            not_merge_id.extend([cluster_id1, cluster_id2])

                    else:
                        not_merge_id.extend([cluster_id1, cluster_id2])

            # for cluster id that not in checked_cluster_id and checked_parent_id
            not_merge_id = set(not_merge_id)
            for index in not_merge_id:
                if (index not in checked_cluster_id) and (index not in checked_parent_id):
                    checked_abstractions[cluster_id] = {
                        'abstraction': abstractions[index]['abstraction'],
                        'nodes': abstractions[index]['nodes']
                    }
                    cluster_id += 1

            return checked_abstractions

        # if there are no valid combinations, just return the input abstractions
        else:
            return abstractions

    def __run_merge_abstraction(self):
        merged_abstractions = {}
        for message_length, abstraction in self.abstractions_nonmerge.items():
            merged_abstractions[message_length] = self.__merge_abstraction(abstraction)

        return merged_abstractions

    def __get_final_abstraction(self, abstractions, event_attributes, parsed_logs):
        # restart abstraction id from 0, get abstraction and its log ids
        # in this method, we include other fields such as timestamp, hostname, ip address, etc in abstraction
        final_abstractions = {}
        abstraction_id = 0
        for message_length, abstraction in abstractions.items():
            # get log ids per cluster
            for cluster_id, cluster in abstraction.items():
                log_ids = []
                for node in cluster['nodes']:
                    log_ids.extend(event_attributes[node]['member'])

                # get entities from raw logs per cluster (except the main message)
                candidates = defaultdict(list)
                candidates_log_ids = defaultdict(list)
                candidates_messages = defaultdict(list)
                for log_id in log_ids:
                    parsed = parsed_logs[log_id]
                    values = []
                    values_length = 0
                    message = []
                    for label, value in parsed.items():
                        if label != 'message':
                            value_split = value.split()
                            values.extend(value_split)
                            values_length += len(value_split)

                        # get the message here
                        else:
                            message_split = value.split()
                            message.extend(message_split)

                    # get abstraction candidates and their respective log ids
                    candidates[values_length].append(values)
                    candidates_log_ids[values_length].append(log_id)
                    candidates_messages[values_length].append(message)

                # get asterisk for entity and message and then set final abstraction
                for label_length, candidate in candidates.items():
                    entity_abstraction = self.__get_asterisk(candidate)
                    message_abstraction = self.__get_asterisk(candidates_messages[label_length])
                    final_abstractions[abstraction_id] = {
                        'abstraction': entity_abstraction + ' ' + message_abstraction,
                        'log_id': candidates_log_ids[label_length]
                    }
                    abstraction_id += 1

        return final_abstractions

    def get_abstraction(self, log_file):
        # initialize abstractions
        self.abstractions_nonmerge = defaultdict(dict)
        self.abstractions_nonmerge_id = 0

        # parsing logs
        parsed_logs, raw_logs = self.parser.parse_logs(log_file)

        # get clusters and event attributes
        # clusters[message_length] = {cluster_id: {'nodes': list, 'check': bool}, ...}
        log_clustering = LogClustering(parsed_logs, raw_logs)
        clusters = log_clustering.get_clustering()
        event_attributes = log_clustering.event_attributes

        # get abstraction
        self.__get_all_asterisk(clusters, event_attributes, parsed_logs, raw_logs)
        merged_abstractions = self.__run_merge_abstraction()
        final_abstractions = self.__get_final_abstraction(merged_abstractions, event_attributes, parsed_logs)

        # final_abstractions[abstraction_id] = {'abstraction': str, 'log_id': [int, ...]}
        return final_abstractions, raw_logs

if __name__ == '__main__':
    # get log file name
    if len(sys.argv) == 1:
        print('Please input dataset and file name after the command.')
        sys.exit(1)
    else:
        dataset = sys.argv[1]
        filename = sys.argv[2]
        print('Processing dataset:', dataset, 'filename:', filename, '...')

    # get log abstraction
    logfile = '/home/hudan/Git/pylogabstract/datasets/' + dataset + '/logs/' + filename
    log_abstraction = LogAbstraction()
    abstraction_results, rawlogs = log_abstraction.get_abstraction(logfile)

    # prepare ground truth for comparison
    abstraction_withid_file = \
        '/home/hudan/Git/pylogabstract/datasets/' + dataset + '/logs-abstraction_withid/' + filename
    abstractions_groundtruth_file = \
        '/home/hudan/Git/pylogabstract/datasets/' + dataset + '/logs-lineid_abstractionid/' + filename

    # write output to file
    Output.write_perabstraction(abstraction_results, rawlogs, 'results-perabstraction.txt')
    Output.write_perline(abstraction_results, rawlogs, 'results-perline.txt')
    Output.write_comparison(abstraction_withid_file, abstractions_groundtruth_file, abstraction_results,
                            rawlogs, 'results-comparison.txt')
