import networkx as nx
import community


class Louvain(object):
    def __init__(self, gexf_file):
        self.gexf_file = gexf_file
        self.graph = None
        self.partition = {}
        self.clusters = {}

    def __get_community(self):
        self.graph = nx.read_gexf(self.gexf_file)
        self.partition = community.best_partition(self.graph)

    def get_cluster(self):
        self.__get_community()
        for node_id, partition_id in self.partition.items():
            if partition_id not in self.clusters.keys():
                self.clusters[partition_id] = []
            self.clusters[partition_id].append(node_id)

        return self.clusters
