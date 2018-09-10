import networkx as nx
import os
from pylogabstract.preprocess.create_graph2 import CreateGraph
from pylogabstract.clustering.louvain import Louvain


class Abstraction(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.graph_model = None
        self.graph = None
        self.clusters = {}
        self.cluster_id = 0

    def __create_graph(self, cluster=None):
        # create new temporary graph based on subgraph nodes
        if cluster:
            subgraph = [int(node) for node in cluster]
            graph_noattributes = self.graph_noattributes.subgraph(subgraph)

        # create graph
        else:
            self.graph_model = CreateGraph(self.log_file)
            self.graph = self.graph_model.create_graph()
            self.graph_noattributes = self.graph_model.create_graph_noattributes()
            graph_noattributes = self.graph_noattributes

        # write to gexf file
        filename = self.log_file.split('/')[-1]
        gexf_file = os.path.join('/', 'tmp', filename + '.gexf')
        nx.write_gexf(graph_noattributes, gexf_file)

        return gexf_file

    def __get_community(self, subgraph=None, previous_cluster=None):
        # prepare graph or subgraph
        if subgraph:
            print('subgraph', subgraph)
            gexf_file = self.__create_graph(subgraph)
        else:
            gexf_file = self.__create_graph()

        # graph clustering based on Louvain community detection
        louvain = Louvain(gexf_file)
        clusters = louvain.get_cluster()

        # stop-recursion case: if there is no more partition
        if list(clusters.values())[0] == previous_cluster:
            # print('stop recursion', clusters)
            nodes = [int(node) for node in list(clusters.values())[0]]
            self.clusters[self.cluster_id] = nodes
            self.cluster_id += 1

        # recursion case: graph clustering
        else:
            print('recursion', clusters)
            for cluster_id, cluster in clusters.items():
                previous_cluster = cluster
                self.__get_community(cluster, previous_cluster)

        return self.clusters

    def get_abstraction(self):
        clusters = self.__get_community()
        return clusters


if __name__ == '__main__':
    logfile = '/home/hudan/Git/prlogparser/datasets/casper-rw/auth.log'
    a = Abstraction(logfile)
    results = a.get_abstraction()

    for k, v in results.items():
        print(k, v)
