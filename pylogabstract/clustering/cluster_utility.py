from itertools import combinations
from datetime import datetime
import networkx as nx


class ClusterUtility(object):
    """A class contains some utilities to do clustering algorithm.
    """
    @staticmethod
    def get_geometric_mean(weights):
        """Get geometric mean or intensity in a clique. A clique can be a k-clique or maximal clique.

        Parameters
        ----------
        weights : list[float]
            List of edge weight in a clique.

        Returns
        -------
        gmean   : float
            Geometric mean of given edge weights.
        """
        multiplication = 1
        for weight in weights:
            multiplication *= weight

        gmean = 0.0
        if multiplication > 0:
            k = float(len(weights))
            if k > 0.0:
                gmean = multiplication ** (1 / k)

        gmean = round(gmean, 5)
        return gmean

    @staticmethod
    def get_weighted_cliques(graph, cliques, threshold):
        """Get weighted cliques based on given intensity threshold.

        A clique which its weight are less then threshold is omiited.
        This procedure will filter unsignificant cliques.

        Parameters
        ----------
        graph       : graph
            A graph to check for its weighted cliques.
        cliques     : list[frozenset]
            List of clique list found.
        threshold   : float
            Intensity (geometric mean) threshold.

        Returns
        -------
        weighted_cliques    : list[frozenset]
            List of clique with significant weight.
        """
        weighted_kcliques = []
        for clique in cliques:
            weights = []
            for u, v in combinations(clique, 2):
                reduced_precision = round(graph[u][v][0]['weight'], 5)
                weights.append(reduced_precision)
            gmean = ClusterUtility.get_geometric_mean(weights)

            if gmean > threshold:
                weighted_kcliques.append(frozenset(clique))

        return weighted_kcliques

    @staticmethod
    def set_cluster_id(graph, clusters):
        """Set incremental cluster identifier start from 0.

        Parameters
        ----------
        graph       : graph
            Graph to be set for its cluster id.
        clusters    : dict[list]
            Dictionary contains list of node in a particular cluster.
        """
        for cluster_id, cluster in clusters.items():
            for node in cluster:
                graph.node[node]['cluster'] = cluster_id

    @staticmethod
    def get_cluster_property(graph, clusters, year, edges_dict, logtype):
        """Get cluster property.

        Parameters
        ----------
        graph           : graph
            Graph to be analyzed.
        clusters        : dict[list]
            Dictionary contains sequence of nodes in all clusters.
        year            : str
            Year of the log file. We need this parameter since log file does not provide it.
        edges_dict      : dict
            Dictionary of edges. Keys: (node1, node2), values: index.
        logtype         : str
            Type of event log, e.g., auth or kippo

        Returns
        -------
        cluster_property    : dict
            Property of a cluster. For example: frequency of event logs.

        Notes
        -----
        frequency           : frequency of event logs in a cluster.
        member              : number of nodes in a cluster.
        interarrival_rate   : inter-arrival rate of event logs timestamp in a cluster.
        edges_number        : number of edges in a cluster.
        density             : density of a cluster.
        interarrival_time   : inter-arrival time in a cluster.
        """
        cluster_property = {}      # event log frequency per cluster
        num_edges = ClusterUtility.get_num_edges(clusters, edges_dict)
        for cluster_id, nodes in clusters.items():
            properties = {}
            datetimes = []
            for node_id in nodes:
                properties['frequency'] = properties.get('frequency', 0) + graph.node[node_id]['frequency']
                properties['member'] = properties.get('member', 0) + 1
                datetimes.append(graph.node[node_id]['start'])
                datetimes.append(graph.node[node_id]['end'])

            # get inter-arrival rate
            sorted_datetimes = sorted(datetimes)
            start, end = None, None
            if logtype == 'auth':
                start_temp = sorted_datetimes[0].split()
                end_temp = sorted_datetimes[-1].split()  # note that it is -1 not 1
                start = datetime.strptime(' '.join(start_temp[:2]) + ' ' + year + ' ' + ' '.join(start_temp[2:]),
                                          '%b %d %Y %H:%M:%S')
                end = datetime.strptime(' '.join(end_temp[:2]) + ' ' + year + ' ' + ' '.join(end_temp[2:]),
                                        '%b %d %Y %H:%M:%S')
            elif logtype == 'kippo':
                start_temp = sorted_datetimes[0]
                end_temp = sorted_datetimes[-1]
                start = datetime.strptime(start_temp, '%Y-%m-%d %H:%M:%S')
                end = datetime.strptime(end_temp, '%Y-%m-%d %H:%M:%S')

            interarrival_times = end - start
            interarrival = interarrival_times.seconds if interarrival_times.seconds != 0 else 1
            properties['interarrival_time'] = interarrival
            properties['interarrival_rate'] = float(interarrival) / float(properties['frequency'])
            properties['edges_number'] = num_edges[cluster_id]
            properties['density'] = ClusterUtility.get_cluster_density(properties['edges_number'], properties['member'])

            # set cluster property
            cluster_property[cluster_id] = properties

        return cluster_property

    @staticmethod
    def get_num_edges(clusters, edges_dict):
        """Find number of edges in the cluster.

        Parameters
        ----------
        clusters    : dict
            Dictionary contains sequence of nodes in all clusters.
        edges_dict  : dict
            Dictionary of edges. Keys: (node1, node2), values: index.

        Returns
        -------
        num_edges   : dict
            Number of edges per cluster. Key: cluster id, values: number of edges.
        """
        num_edges = {}
        edges = edges_dict.keys()
        for cluster_id, cluster in clusters.items():
            if len(cluster) == 1:
                num_edges[cluster_id] = 0
            else:
                for u, v in combinations(cluster, 2):
                    if (u, v) in edges or (v, u) in edges:
                        num_edges[cluster_id] = num_edges.get(cluster_id, 0) + 1

        return num_edges

    @staticmethod
    def get_cluster_density(num_edges, num_nodes):
        """Get cluster density.

        Note that a cluster is equal with a subgraph. And the formula of the subgraph density is
        based on [WikiDenseSubgraph2016]_.

        References
        ----------
        .. [WikiDenseSubgraph2016] Dense subgraph. https://en.wikipedia.org/wiki/Dense_subgraph.

        Parameters
        ----------
        num_edges   : int
            Number of edges in a clusters.
        num_nodes   : int
            Number of nodes in a clusters.

        Returns
        -------
        cluster_density : float
            The cluster density value for a cluster.
        """
        cluster_density = float(num_edges) / float(num_nodes)
        return cluster_density

    @staticmethod
    def remove_outcluster(graph):
        """Remove edges that connect to other clusters.

        This method will first find any edges in the cluster member. If edges connecting to a node does not belong to
        the current cluster, then it will be removed.
        """
        # remove edge outside cluster
        for node in graph.nodes_iter(data=True):
            neighbors = graph.neighbors(node[0])
            for neighbor in neighbors:
                # if cluster id of current node is not the same of the connecting node
                if graph.node[node[0]]['cluster'] != graph.node[neighbor]['cluster']:
                    try:
                        graph.remove_edge(node[0], neighbor)
                    except nx.exception.NetworkXError:
                        graph.remove_edge(neighbor, node[0])
        return graph
