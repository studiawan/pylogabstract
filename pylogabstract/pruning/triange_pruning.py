from operator import itemgetter


class TrianglePruning(object):
    """Class for edge pruning based weakest edge weight in each triangle found in the graph.
    """
    def __init__(self, graph):
        """Initialization of class TrianglePruning.

        Parameters
        ----------
        graph   : graph
            Analyzed graph.
        """
        self.graph = graph
        self.__MAX_EDGES = 10000

    def __remove_edge(self, triangle):
        """Remove the weakest edge in a triangle.

        Parameters
        ----------
        triangle    :
            Tuple of triangle nodes.
        """
        # get weight in triangle
        weight = dict()
        if self.graph.has_edge(triangle[0], triangle[1]):
            weight_data = self.graph.get_edge_data(triangle[0], triangle[1])
            if weight_data is None:
                weight_data = self.graph.get_edge_data(triangle[1], triangle[0])
            weight[(triangle[0], triangle[1])] = weight_data
        else:
            return

        if self.graph.has_edge(triangle[0], triangle[2]):
            weight_data = self.graph.get_edge_data(triangle[0], triangle[2])
            if weight_data is None:
                weight[(triangle[0], triangle[2])] = self.graph.get_edge_data(triangle[2], triangle[0])
            weight[(triangle[0], triangle[2])] = weight_data
        else:
            return

        if self.graph.has_edge(triangle[1], triangle[2]):
            weight_data = self.graph.get_edge_data(triangle[1], triangle[2])
            if weight_data is None:
                weight[(triangle[1], triangle[2])] = self.graph.get_edge_data(triangle[2], triangle[1])
            weight[(triangle[1], triangle[2])] = weight_data
        else:
            return

        # initiate minimum weight value
        min_weight = (triangle[0], triangle[1], weight[(triangle[0], triangle[1])])

        # get minimum weight
        for nodes, w in weight.items():
            if w['weight'] < min_weight[2]['weight']:
                min_weight = (nodes[0], nodes[1], w)

        # remove edge that has the minimum weight
        self.graph.remove_edge(min_weight[0], min_weight[1])

    def prune_graph(self):
        """Find all triangles in a graph [Schank2005]_. The implementation is a modification
           of [triangleinequality2013]_.

        References
        ----------
        .. [Schank2005] Schank, T., & Wagner, D. Finding, counting and listing all triangles in large graphs,
                        an experimental study. In International Workshop on Experimental and Efficient Algorithms,
                        pp. 606-609, 2005, Springer Berlin Heidelberg.
        .. [triangleinequality2013] Finding Triangles in a Graph.
                                    https://triangleinequality.wordpress.com/2013/09/11/finding-triangles-in-a-graph/

        """
        sorted_degree = sorted(self.graph.degree(), key=itemgetter(1), reverse=True)
        a = {}

        for index, info in enumerate(sorted_degree):
            a[info[0]] = {'index': index, 'set': set()}

        removed_edges = []
        for first_vertex, degree in sorted_degree:
            for second_vertex in self.graph.neighbors(first_vertex):
                if a[first_vertex]['index'] < a[second_vertex]['index']:
                    for third_vertex in a[first_vertex]['set'].intersection(a[second_vertex]['set']):
                        removed_edges.append((first_vertex, second_vertex, third_vertex))
                    a[second_vertex]['set'].add(first_vertex)

        for removed_edge in removed_edges:
            self.__remove_edge(removed_edge)

        if len(self.graph.edges) > self.__MAX_EDGES:
            self.prune_graph()
        else:
            return self.graph
