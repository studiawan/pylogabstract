from itertools import combinations
import multiprocessing


class HammingSimilarity(object):
    def __init__(self):
        pass

    @staticmethod
    def __isdifferent(value1, value2):
        different = 0
        if value1 == value2:
            different = 1
        elif value1 != value2:
            different = 0

        return different

    def get_weighted_hamming(self, string1, string2):
        # string1, string2 should be list? so we do not split each time we run operation
        # split
        string1_split = string1.split()
        string2_split = string2.split()

        # get invers index
        index = list(range(1, len(string1_split) + 1))
        index = sorted(index, reverse=True)

        # operation
        weighted_hamming = 0
        i = 0
        for word1, word2 in zip(string1_split, string2_split):
            isdifferent = self.__isdifferent(word1, word2)
            weighted_hamming += index[i] * isdifferent
            i += 1

        return round(weighted_hamming / sum(index), 3)


class ParallelHammingSimilarity(object):
    def __init__(self, event_attributes, event_indices):
        self.event_attributes = event_attributes
        self.event_indices = event_indices
        self.hamming_similarity = HammingSimilarity()
        self.edges_weight = []

    def __get_hamming_similarity(self, unique_event_id):
        # calculate hamming similarity
        string1_len = self.event_attributes[unique_event_id[0]]['message_length']
        string2_len = self.event_attributes[unique_event_id[1]]['message_length']

        # calculate only if message lengths are the same
        if string1_len == string2_len:
            string1 = self.event_attributes[unique_event_id[0]]['message']
            string2 = self.event_attributes[unique_event_id[1]]['message']
            similarity = self.hamming_similarity.get_weighted_hamming(string1, string2)

            if similarity > 0.:
                return round(similarity, 3)

    def __call__(self, unique_event_id):
        # get similarity from two strings
        similarity = self.__get_hamming_similarity(unique_event_id)
        distance_with_id = (unique_event_id[0], unique_event_id[1], similarity)
        return distance_with_id

    def get_parallel_hamming_similarity(self):
        # get unique event id combination
        event_id_combination = combinations(self.event_indices, 2)

        # get distance with multiprocessing
        total_cpu = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=total_cpu)
        similarity = pool.map(self, event_id_combination)
        pool.close()
        pool.join()

        # remove empty elements
        removed = []
        for index, distance in enumerate(similarity):
            if (distance[2] is None) or (distance[2] == 0):
                removed.append(index)

        # similarity as edge weight in a graph
        similarity = [y for x, y in enumerate(similarity) if x not in removed]
        self.edges_weight = similarity

        for d in similarity:
            print(d)

        return similarity
