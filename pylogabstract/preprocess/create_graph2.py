import networkx as nx
from pylogabstract.preprocess.preprocess import Preprocess
from pylogabstract.preprocess.cosine_similarity import ParallelCosineSimilarity


class CreateGraph(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.preprocess = None
        self.unique_events = []
        self.unique_events_length = 0
        self.distances = []
        self.graph = nx.MultiGraph()
        self.graph_noattributes = nx.Graph()

    def __get_nodes(self):
        # preprocess logs and get unique events as nodes in a graph
        self.preprocess = Preprocess(self.log_file)
        self.unique_events = self.preprocess.get_unique_events()
        self.unique_events_length = self.preprocess.unique_events_length
        self.event_attributes = self.preprocess.event_attributes

    def __get_distances(self):
        # get cosine distance as edges with weight
        pcs = ParallelCosineSimilarity(self.event_attributes, self.unique_events_length)
        self.distances = pcs.get_parallel_cosine_similarity()

    def create_graph(self):
        # create graph with previously created nodes and edges
        self.__get_nodes()
        self.__get_distances()
        self.graph.add_nodes_from(self.unique_events)
        self.graph.add_weighted_edges_from(self.distances)

        return self.graph

    def create_graph_noattributes(self):
        nodes = range(self.unique_events_length)
        self.graph_noattributes.add_nodes_from(nodes)
        self.graph_noattributes.add_weighted_edges_from(self.distances)

        return self.graph_noattributes
