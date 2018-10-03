import networkx as nx
from pylogabstract.preprocess.hamming_similarity import ParallelHammingSimilarity


class CreateGraph(object):
    def __init__(self, unique_events, event_attributes, event_indices):
        self.unique_events = unique_events
        self.event_attributes = event_attributes
        self.event_indices = event_indices
        self.similarity = []
        self.graph = nx.Graph()

    def __get_similarity(self):
        hamming_similarity = ParallelHammingSimilarity(self.event_attributes, self.event_indices)
        self.similarity = hamming_similarity.get_parallel_hamming_similarity()

    def create_graph(self):
        # create graph with previously created nodes and edges
        self.__get_similarity()
        self.graph.add_nodes_from(self.unique_events)
        self.graph.add_weighted_edges_from(self.similarity)

        return self.graph
