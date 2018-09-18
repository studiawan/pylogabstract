from collections import defaultdict
from networkx.algorithms import community
from operator import itemgetter
from pylogabstract.preprocess.preprocess import Preprocess
from pylogabstract.preprocess.create_graph3 import CreateGraph
import community as commun
import networkx as nx


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
            cluster[cluster_id].extend(partition)
            cluster_id += 1

        return cluster

    def __get_initial_modularity(self, graph):
        clusters = {}
        cluster_id = 0

        # get initial clusters with connected component
        components = nx.connected_components(graph)
        final_components = []
        for component in components:
            final_components.append(component)
            clusters[cluster_id] = list(component)
            cluster_id += 1

        # get initial modularity
        partition = self.__convert_to_nodeid_clusterid(final_components)
        modularity = commun.modularity(partition, graph)

        return clusters, modularity

    def __get_valid_graph(self, graph):
        # remove nodes without edges
        isolated_nodes = nx.isolates(graph)
        isolated = []
        for node in isolated_nodes:
            isolated.append(node)
            self.clusters[self.cluster_id].append(node)
            self.cluster_id += 1
        graph.remove_nodes_from(isolated)

        if graph.edges():
            # if only one edge exist, no clustering
            if len(graph.edges()) == 1:
                self.clusters[self.cluster_id].extend(graph.nodes())
                self.cluster_id += 1
                return None
            else:
                return graph
        else:
            return None

    @staticmethod
    def __get_edge_weight_sum(graph):
        weight_sum = 0
        for edges in graph.edges(data='weight'):
            weight_sum += edges[2]

        return weight_sum

    def __get_graph_cluster(self, graph):
        # the initial max modularity is the modularity of the initial graph before clustering
        # the initial cluster configuration is the graph's connected component
        # best_cluster, max_modularity = self.__get_initial_modularity(graph)
        best_cluster, max_modularity = [], -1.

        # clustering using Girvan-Newman method
        clusters = community.girvan_newman(graph, most_valuable_edge=lightest)
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

        for message_length, group in self.message_length_group.items():
            print(message_length, group)
            # no graph needed
            group_length = len(group)
            if group_length == 1:
                self.clusters[self.cluster_id].extend(group)
                self.cluster_id += 1

            # create graph for a particular group
            else:
                unique_events = preprocess.get_partial_unique_events(group)
                graph_model = CreateGraph(unique_events, self.event_attributes, group)
                graph = graph_model.create_graph()

                # only process connected components with edges > 1
                graph = self.__get_valid_graph(graph)

                # clustering with valid graph only
                if graph is not None:
                    clusters = self.__get_graph_cluster(graph)
                    for index, nodes in clusters.items():
                        self.clusters[self.cluster_id].extend(nodes)
                        self.cluster_id += 1

    def get_abstraction(self):
        self.__get_clusters()
        return self.clusters


def lightest(g):
    u, v, w = min(g.edges(data='weight'), key=itemgetter(2))
    return u, v


if __name__ == '__main__':
    logfile = '/home/hudan/Git/prlogparser/datasets/casper-rw/kern.log'
    log_abstraction = LogAbstraction(logfile)
    results = log_abstraction.get_abstraction()
    event_attr = log_abstraction.event_attributes

    print('\nCLUSTERING RESULT')
    for i, result in results.items():
        for r in result:
            print(r, event_attr[r]['message'])
        print('---')