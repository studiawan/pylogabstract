from collections import defaultdict
from networkx.algorithms import community
from operator import itemgetter
from pylogabstract.preprocess.preprocess import Preprocess
from pylogabstract.preprocess.create_graph3 import CreateGraph
import community as commun


class LogAbstraction(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.clusters = defaultdict(list)
        self.cluster_id = 0
        self.message_length_group = {}
        self.event_attributes = {}

    @staticmethod
    def __convert_to_nodeid_clusterid(partition):
        part_dict = {}
        cluster_id = 0
        for p in partition:
            for node_id in p:
                part_dict[node_id] = cluster_id
            cluster_id += 1

        return part_dict

    @staticmethod
    def __convert_to_clusterid_nodeid(partitions):
        cluster = defaultdict(list)
        cluster_id = 0
        for partition in partitions:
            for p in partition:
                cluster[cluster_id].append(p)
                cluster_id += 1

        return cluster

    def __get_graph_cluster(self, graph):
        # clustering using Girvan-Newman method
        clusters = community.girvan_newman(graph, most_valuable_edge=lightest)
        max_modularity = 0.
        best_cluster = []
        for cluster in clusters:
            # calculate modularity for each cluster
            partition = self.__convert_to_nodeid_clusterid(cluster)
            modularity = commun.modularity(partition, graph)

            # best cluster has the highest modularity
            if max_modularity < modularity:
                max_modularity = modularity
                best_cluster = cluster

        best_cluster = self.__convert_to_clusterid_nodeid(best_cluster)
        return best_cluster

    def __get_clusters(self):
        # preprocess
        preprocess = Preprocess(self.log_file)
        preprocess.get_unique_events()
        self.message_length_group = preprocess.message_length_group
        self.event_attributes = preprocess.event_attributes

        for unique_event_id, group in self.message_length_group.items():
            # no graph needed
            if len(group) == 1:
                self.clusters[self.cluster_id].append(unique_event_id)
                self.cluster_id += 1

            # create graph for a particular group
            else:
                unique_events = preprocess.get_partial_unique_events(group)
                graph_model = CreateGraph(unique_events, self.event_attributes, group)
                graph = graph_model.create_graph()

                # clustering
                clusters = self.__get_graph_cluster(graph)
                for index, nodes in clusters.items():
                    self.clusters[self.cluster_id] = nodes
                    self.cluster_id += 1


def lightest(g):
    u, v, w = min(g.edges(data='weight'), key=itemgetter(2))
    return u, v
