from collections import Counter
from math import log, sqrt
from itertools import combinations
from nltk import corpus
from random import randint, choice
import string
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

    def __get_tfidf(self, string_input):
        # calculate tf-idf
        string_split = string_input.split()
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
    def __init__(self, event_attributes, event_length, weighted=True):
        self.event_attributes = event_attributes
        self.event_length = event_length
        self.weighted = weighted
        self.weighted_cosine_similarity = WeightedCosineSimilarity()
        self.edges_weight = []

    def __get_cosine_similarity(self, unique_event_id):
        # calculate cosine similarity
        # only if message lengths are the same
        string1_len = self.event_attributes[unique_event_id[0]]['message_length']
        string2_len = self.event_attributes[unique_event_id[1]]['message_length']

        if string1_len == string2_len:
            string1 = self.event_attributes[unique_event_id[0]]['message']
            string2 = self.event_attributes[unique_event_id[1]]['message']
            if self.weighted:
                distance = self.weighted_cosine_similarity.get_weigted_cosine(string1, string2)
            else:
                self.cosine_similarity = CosineSimilarity()
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
        for d in distances:
            print(d)
        self.edges_weight = distances
        return distances


class HammingDistance(object):
    def __init__(self):
        self.word_frequency = {}
        self.stopwords = corpus.stopwords.words('english')
        self.allchar = string.ascii_letters + string.punctuation + string.digits

    def __get_random_string(self):
        random_string = ''.join(choice(self.allchar) for _ in range(randint(3, 3)))
        return random_string

    def __replace_stopword_with_random(self, message):
        nostopwords = []
        for word in message.split():
            if word in self.stopwords:
                word = self.__get_random_string()
            nostopwords.append(word)

        return nostopwords

    def __get_frequency(self, string_list):
        for word in string_list:
            if word in self.word_frequency.keys():
                self.word_frequency[word] += 1
            else:
                self.word_frequency[word] = 1

    def get_weighted_hamming_distance(self, string1, string2):
        string1_list = self.__replace_stopword_with_random(string1)
        string2_list = self.__replace_stopword_with_random(string2)
        string_len = len(string1_list)

        self.word_frequency = {}
        self.__get_frequency(string1_list)
        self.__get_frequency(string2_list)

        weighted = []
        for index, word1 in enumerate(string1_list):
            if word1 != string2_list[index]:
                weighted.append(2 * string_len)
            else:
                weighted.append(self.word_frequency[word1])

        denominator = 0
        for weight in weighted:
            denominator += 1 / weight

        weighted_hamming_distance = 1 / denominator
        return round(weighted_hamming_distance, 3)


class WeightedCosineSimilarity(object):
    """Li, B., & Han, L. (2013). Distance weighted cosine similarity measure for text classification.
       International Conference on Intelligent Data Engineering and Automated Learning (pp. 611-618).
    """
    def __init__(self):
        self.cosine = 0
        self.hamming = 0
        self.cs = None
        self.hd = HammingDistance()

    def get_weigted_cosine(self, string1, string2):
        self.cs = CosineSimilarity()
        self.cosine = self.cs.get_cosine_similarity(string1, string2)
        self.hamming = self.hd.get_weighted_hamming_distance(string1, string2)

        weighted_cosine = self.cosine / (self.hamming * self.hamming + self.cosine)
        return round(weighted_cosine, 3)


if __name__ == '__main__':
    str1 = 'bridge 0000:00:1e.0 32bit mmio pref: [e0000000, efffffff]'
    str2 = '0000:01:04.0 reg 14 32bit mmio: [e0000000, efffffff]'
    str3 = '0000:01:04.0 reg 30 32bit mmio: [fea00000, fea1ffff]'
    wcs = WeightedCosineSimilarity()
    dist1 = wcs.get_weigted_cosine(str1, str2)
    dist2 = wcs.get_weigted_cosine(str2, str3)
    print(dist1, dist2)
