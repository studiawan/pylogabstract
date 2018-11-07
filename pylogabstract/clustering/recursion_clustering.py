import community as commun
import networkx as nx
import sys
from collections import defaultdict
from networkx.algorithms import community
from operator import itemgetter
from pylogabstract.preprocess.preprocess import Preprocess
from pylogabstract.preprocess.create_graph import CreateGraph
from pylogabstract.parser.parser import Parser
from pylogabstract.clustering.force_clustering import ForceClustering


class LogClustering(object):
    def __init__(self, parsed_logs, raw_logs, partial_message_length_group=None, partial_event_attributes=None):
        self.clusters = defaultdict(dict)
        self.cluster_id = 0
        self.message_length_group = {}
        self.event_attributes = {}
        self.preprocess = None
        self.parsed_logs = parsed_logs
        self.raw_logs = raw_logs
        self.partial_message_length_group = partial_message_length_group
        self.partial_event_attributes = partial_event_attributes
        self.__BOTTOM_DENSITY = 0.8
        self.__TOP_DENSITY = 1.0
        self.__MAX_EDGES = 10000

    @staticmethod
    def __convert_to_nodeid_clusterid(partition):
        # output: {nodeid:clusterid, ...}
        part_dict = {}
        cluster_id = 0
        for p in partition:
            for node_id in p:
                part_dict[node_id] = cluster_id
            cluster_id += 1

        return part_dict

    @staticmethod
    def __convert_to_clusterid_nodeid(partitions):
        # output: {clusterid: [nodeid, ...], ...}
        cluster = defaultdict(list)
        cluster_id = 0
        for partition in partitions:
            cluster[cluster_id].extend(partition)
            cluster_id += 1

        return cluster

    def __check_weight(self, message_length, graph, nodes):
        # separate two nodes if edge weight < 0.5
        all_weight = nx.get_edge_attributes(graph, 'weight')
        try:
            weight = all_weight[(nodes[0], nodes[1])]
        except KeyError:
            weight = all_weight[(nodes[1], nodes[0])]

        if weight < 0.5:
            status = True
        else:
            status = False

        if status:
            for node in nodes:
                self.clusters[message_length][self.cluster_id] = {
                    'nodes': [node],
                    'check': False
                }
                self.cluster_id += 1
        else:
            self.clusters[message_length][self.cluster_id] = {
                'nodes': nodes,
                'check': False
            }
            self.cluster_id += 1

    def __get_valid_graph(self, message_length, graph):
        # remove nodes without edges
        isolated_nodes = nx.isolates(graph)
        isolated = []
        for node in isolated_nodes:
            isolated.append(node)
            self.clusters[message_length][self.cluster_id] = {
                'nodes': [node],
                'check': False
            }
            self.cluster_id += 1
        graph.remove_nodes_from(isolated)

        # if graph has edges
        if graph.edges():
            # if only one edge exist, no clustering
            if len(graph.edges()) == 1:
                self.__check_weight(message_length, graph, list(graph.nodes()))
                return None

            else:
                # check each connected component in the graph
                components = nx.connected_components(graph)
                removed_nodes = []
                for component in components:
                    # remove nodes if a component only has two vertices with one edge
                    if len(component) == 2:
                        component_node = list(component)
                        self.__check_weight(message_length, graph, component_node)
                        removed_nodes.extend(component_node)

                graph.remove_nodes_from(removed_nodes)

                # if nodes still exists after removal
                if graph.nodes():
                    return graph
                else:
                    return None
        else:
            return None

    def __get_graph_cluster(self, graph):
        # initialize maximum modularity and best cluster
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

    def __get_partial_unique_events(self, indices):
        partial_unique_events = []
        for index, attr in self.event_attributes.items():
            if index in indices:
                partial_unique_events.append((index, attr))

        return partial_unique_events

    def __get_clusters(self, message_length_group):
        for message_length, group in message_length_group.items():
            # no graph needed as there is only one node
            group_length = len(group)
            if group_length == 1:
                self.clusters[message_length][self.cluster_id] = {
                    'nodes': group,
                    'check': False
                }
                self.cluster_id += 1

            # create graph for a particular group
            else:
                unique_events = self.__get_partial_unique_events(group)
                graph_model = CreateGraph(unique_events, self.event_attributes, group)
                graph = graph_model.create_graph()

                # if it is a complete graph, no clustering needed
                graph_density = nx.density(graph)

                # high density graph
                if (self.__BOTTOM_DENSITY < graph_density <= self.__TOP_DENSITY) and \
                        (len(graph.edges) >= self.__MAX_EDGES):
                    force_clustering = ForceClustering(graph, self.cluster_id)
                    clusters, self.cluster_id = force_clustering.get_clusters()
                    self.clusters[message_length].update(clusters)

                else:
                    # clustering with valid graph only
                    graph = self.__get_valid_graph(message_length, graph)
                    if graph is not None:
                        clusters = self.__get_graph_cluster(graph)
                        for index, nodes in clusters.items():
                            if len(nodes) <= 3:
                                self.clusters[message_length][self.cluster_id] = {
                                    'nodes': nodes,
                                    'check': True
                                }
                                self.cluster_id += 1
                            else:
                                # recursion here
                                message_length_group_recursion = {message_length: nodes}
                                self.__get_clusters(message_length_group_recursion)

    def __run_preprocess(self):
        # preprocess
        if self.partial_message_length_group is None and self.partial_event_attributes is None:
            self.preprocess = Preprocess(self.parsed_logs, self.raw_logs)
            self.preprocess.get_unique_events()
            self.message_length_group = self.preprocess.message_length_group
            self.event_attributes = self.preprocess.event_attributes

        else:
            self.message_length_group = self.partial_message_length_group
            self.event_attributes = self.partial_event_attributes

    def get_clustering(self):
        self.__run_preprocess()
        self.__get_clusters(self.message_length_group)

        return self.clusters


def lightest(g):
    # get lightest edge weight in a graph
    u, v, w = min(g.edges(data='weight'), key=itemgetter(2))
    return u, v


if __name__ == '__main__':
    # parse log file
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
    parser = Parser()
    parsed_results, raw_results = parser.parse_logs(logfile)
    print_true_only = True

    # get clusters
    log_clustering = LogClustering(parsed_results, raw_results)
    results = log_clustering.get_clustering()
    event_attr = log_clustering.event_attributes

    # print clustering results
    for msg_length, result in results.items():
        for cl_id, clust in result.items():
            for no in clust['nodes']:
                if print_true_only:
                    if clust['check']:
                        print(cl_id, no, event_attr[no]['message'])
                else:
                    print(cl_id, no, clust['check'], event_attr[no]['message'])
            print('---')
        print('---\n')
