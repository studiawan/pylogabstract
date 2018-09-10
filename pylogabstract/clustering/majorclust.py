

class MajorClust(object):
    """The implementation of MajorClust graph clustering algorithm [Stein1999]_.

    References
    ----------
    .. [Stein1999] B. Stein and O. Niggemann, On the nature of structure and its identification,
                   Proceedings of the 25th International Workshop on Graph-Theoretic Concepts in Computer Science,
                   pp. 122-134, 1999.
    """
    def __init__(self, graph):
        """The constructor of class MajorClust.

        Parameters
        ----------
        graph   : graph
            A graph to be clustered.
        """
        self.graph = graph
        self.clusters = {}

    def get_majorclust(self, graph):
        """The main method to run MajorClust algorithm.

        Parameters
        ----------
        graph   : graph
            A graph to be clustered.

        Returns
        -------
        clusters = dict[list]
            Dictionary of list containing node identifier for each cluster.
        """
        # run majorclust algorithm
        self._majorclust(graph)
        self._get_cluster()

        return self.clusters

    def _majorclust(self, graph):
        """The main procedure of MajorClust which is visiting every node to be evaluated for its neighbor cluster.

        Parameters
        ----------
        graph   : graph
            A graph to be clustered. It can be original graph or the refined one.
        """
        reclusters = set()
        terminate = False
        while not terminate:
            terminate = True
            for node in graph.nodes(data=True):
                initial_cluster = node[1]['cluster']
                current_cluster = self._re_majorclust(node, graph)
                recluster = (node[0], initial_cluster, current_cluster)

                # if has not checked, recluster again
                if initial_cluster != current_cluster and recluster not in reclusters:
                    reclusters.add(recluster)
                    node[1]['cluster'] = current_cluster
                    terminate = False

    @staticmethod
    def _re_majorclust(node, graph):
        """Evaluating the neighbor nodes.

        Parameters
        ----------
        node    : node
            A node in a processed graph.
        graph   : graph
            A graph to be clustered. It can be original graph or the refined one.
        """
        # reclustering
        visited_neighbor, visited_neigbor_num, neighbor_weights = {}, {}, {}

        # observe neighboring edges and nodes
        for current_node, neighbor_node, weight in graph.edges([node[0]], data='weight'):
            neighbor_weights[neighbor_node] = weight
            visited_neighbor[graph.node[neighbor_node]['cluster']] = \
                visited_neighbor.get(graph.node[neighbor_node]['cluster'], 0.0) + weight

        # get the weight
        for k, v in visited_neighbor.items():
            visited_neigbor_num.setdefault(v, []).append(k)

        # attach a node to the cluster of majority of neighbor nodes
        current_cluster = visited_neigbor_num[max(visited_neigbor_num)][0] \
            if visited_neigbor_num else node[1]['cluster']

        return current_cluster

    def _get_cluster(self):
        """Get cluster in the form of dictionary of node identifier list. The cluster id is in incremental integer.
        """
        unique_cluster = self._get_unique_cluster()
        cluster_id = 0
        for uc in unique_cluster:
            nodes = []
            for node in self.graph.nodes(data=True):
                print(node)
                if node[1]['cluster'] == uc:
                    nodes.append(node[0])
                    node[1]['cluster'] = cluster_id
            self.clusters[cluster_id] = nodes
            cluster_id += 1

    def _get_unique_cluster(self):
        """Get unique cluster identifier.

        Returns
        -------
        unique_cluster  : set
            A set containing unique cluster identifier.
        """
        cluster = [n[1] for n in self.graph.nodes(data='cluster')]
        unique_cluster = set(cluster)

        return unique_cluster
