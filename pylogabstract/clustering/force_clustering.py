import networkx as nx


class ForceClustering(object):
    def __init__(self, graph, cluster_id):
        self.graph = graph
        self.__WEIGHT_THRESHOLD = 0.75
        self.cluster_id = cluster_id

    def __remove_edges(self):
        removed_edges = []
        for edge in self.graph.edges.data():
            if edge[2]['weight'] < self.__WEIGHT_THRESHOLD:
                removed_edges.append((edge[0], edge[1]))

        for edge in removed_edges:
            self.graph.remove_edge(edge[0], edge[1])

    def get_clusters(self):
        self.__remove_edges()
        clusters = {}
        components = nx.connected_components(self.graph)
        for component in components:
            clusters[self.cluster_id] = {
                'nodes': list(component),
                'check': True
            }

            self.cluster_id += 1

        return clusters, self.cluster_id
