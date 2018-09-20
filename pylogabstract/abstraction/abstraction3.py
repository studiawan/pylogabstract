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
        self.preprocess = None

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
                components = nx.connected_components(graph)
                removed_nodes = []
                for component in components:
                    # if a component only has two vertices with one edge
                    if len(component) == 2:
                        component_node = list(component)
                        if graph.has_edge(component_node[0], component_node[1]) or \
                                graph.has_edge(component_node[1], component_node[0]):
                            self.clusters[self.cluster_id].extend(list(component))
                            self.cluster_id += 1
                            removed_nodes.extend(component_node)

                graph.remove_nodes_from(removed_nodes)
                if graph.nodes():
                    return graph
                else:
                    return None
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
        max_modularity = -1.
        best_cluster = []

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
        self.preprocess = Preprocess(self.log_file)
        self.preprocess.get_unique_events()
        self.message_length_group = self.preprocess.message_length_group
        self.event_attributes = self.preprocess.event_attributes

        for message_length, group in self.message_length_group.items():
            print(message_length, group)
            # no graph needed
            group_length = len(group)
            if group_length == 1:
                self.clusters[self.cluster_id].extend(group)
                self.cluster_id += 1

            # create graph for a particular group
            else:
                unique_events = self.preprocess.get_partial_unique_events(group)
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

    def __refine_cluster(self):
        removed_cluster = []
        new_clusters = []
        for cluster_id, cluster in self.clusters.items():
            if len(cluster) > 3:
                removed_cluster.append(cluster_id)
                unique_events = self.preprocess.get_partial_unique_events(cluster)
                graph_model = CreateGraph(unique_events, self.event_attributes, cluster)
                graph = graph_model.create_graph()

                # graph = self.__get_valid_graph(graph)
                clusters = self.__get_graph_cluster(graph)
                new_clusters.append(clusters)

        for new_cluster in new_clusters:
            for cluster_id, cluster in new_cluster.items():
                self.clusters[self.cluster_id].extend(cluster)
                self.cluster_id += 1

        # empty/remove the current cluster
        # self.clusters.pop(cluster_id, None)
        for cluster_id in removed_cluster:
            self.clusters[cluster_id] = list()

    def get_abstraction(self):
        self.__get_clusters()
        self.__refine_cluster()
        return self.clusters


def lightest(g):
    u, v, w = min(g.edges(data='weight'), key=itemgetter(2))
    return u, v


if __name__ == '__main__':
    logfile = '/home/hudan/Git/prlogparser/datasets/casper-rw/dmesg.3'
    log_abstraction = LogAbstraction(logfile)
    results = log_abstraction.get_abstraction()
    event_attr = log_abstraction.event_attributes

    print('\nCLUSTERING RESULT')
    for i, result in results.items():
        for r in result:
            print(r, event_attr[r]['message'])
        print('---')
