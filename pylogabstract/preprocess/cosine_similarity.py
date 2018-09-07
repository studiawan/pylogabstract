from collections import Counter
from math import log, sqrt
from itertools import combinations
import multiprocessing


class CosineSimilarity(object):
    def __init__(self):
        self.string1 = ''
        self.string2 = ''
        self.docs = []
        self.total_docs = 0
        self.word_count = {}

    def __get_word_in_docs(self, word):
        # if word exist in dictionary
        count = 0
        if word in self.word_count.keys():
            count = self.word_count[word]
        else:
            for doc in self.docs:
                if word in doc:
                    count += 1
            self.word_count[word] = count

        return count

    def __get_tfidf(self, string):
        # calculate tf-idf
        string_split = string.split()
        term_frequency = Counter(string_split)          # calculate tf
        total_terms = len(string_split)
        tfidf = {}
        for term, frequency in term_frequency.most_common():
            normalized_tf = frequency / total_terms     # calculate normalized-tf
            wid = self.__get_word_in_docs(term)
            idf = 1 + log(self.total_docs / wid)        # calculate idf
            tfidf_val = normalized_tf * idf             # calculate tf-idf
            tfidf[term] = tfidf_val

        return tfidf

    @staticmethod
    def __get_doclength(tfidf):
        # get document length for cosine similarity
        length = 0
        for ti in tfidf.values():
            length += pow(ti, 2)

        length = sqrt(length)
        return length

    def get_cosine_similarity(self, string1, string2):
        # initialization
        self.string1 = string1
        self.string2 = string2
        self.docs = [self.string1, self.string2]
        self.total_docs = len(self.docs)

        # get tfidf fot both string
        tfidf1 = self.__get_tfidf(self.string1)
        tfidf2 = self.__get_tfidf(self.string2)

        # get numerator
        vector_products = 0
        intersection = set(tfidf1.keys()) & set(tfidf2.keys())
        for i in intersection:
            vector_products += tfidf1[i] * tfidf2[i]

        # get denominator
        length1 = self.__get_doclength(tfidf1)
        length2 = self.__get_doclength(tfidf2)

        # calculate cosine similarity
        try:
            cosine_similarity = vector_products / (length1 * length2)
        except ZeroDivisionError:
            cosine_similarity = 0

        cosine_similarity = round(cosine_similarity, 3)
        return cosine_similarity


class ParallelCosineSimilarity(object):
    def __init__(self, event_attributes, event_length, nodes=None):
        self.event_attributes = event_attributes
        self.event_length = event_length
        self.cosine_similarity = CosineSimilarity()
        self.edges_weight = []
        self.nodes = nodes

    def __get_cosine_similarity(self, unique_event_id):
        # calculate cosine similarity
        string1 = self.event_attributes[unique_event_id[0]]['preprocessed_events_graphedge']
        string2 = self.event_attributes[unique_event_id[1]]['preprocessed_events_graphedge']
        distance = self.cosine_similarity.get_cosine_similarity(string1, string2)
        if distance > 0.:
            return round(distance, 3)

    def __call__(self, unique_event_id):
        # get distance from two strings
        distance = self.__get_cosine_similarity(unique_event_id)
        distance_with_id = (unique_event_id[0], unique_event_id[1], distance)
        return distance_with_id

    def get_parallel_cosine_similarity(self):
        # get unique event id combination
        if self.nodes:
            event_id_combination = list(combinations(self.nodes, 2))
        else:
            event_id_combination = list(combinations(range(self.event_length), 2))

        # get distance with multiprocessing
        total_cpu = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=total_cpu)
        distances = pool.map(self, event_id_combination)
        pool.close()
        pool.join()

        # remove empty elements
        removed = []
        for index, distance in enumerate(distances):
            if distance[2] is None:
                removed.append(index)

        distances = [y for x, y in enumerate(distances) if x not in removed]
        self.edges_weight = distances
        return distances
