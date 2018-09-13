from pylogabstract.preprocess.create_graph2 import CreateGraph
from networkx.algorithms import community
from operator import itemgetter


class Abstraction(object):
    def __init__(self, log_file):
        self.log_file = log_file
        self.graph_model = None
        self.graph = None
        self.clusters = {}
        self.cluster_id = 0

    def __create_graph(self):
        # create graph
        self.graph_model = CreateGraph(self.log_file)
        self.graph = self.graph_model.create_graph()
        self.graph_noattributes = self.graph_model.create_graph_noattributes()
        self.messages = {}

    def __get_clusters(self):
        self.__create_graph()
        comp = community.girvan_newman(self.graph, most_valuable_edge=lightest)
        for c in next(comp):
            self.clusters[self.cluster_id] = sorted(c)
            self.cluster_id += 1

        return self.clusters

    def __get_messages(self):
        for cluster_id, cluster in self.clusters.items():
            cluster_messages = []
            for c in cluster:
                cluster_messages.append(self.graph.nodes[c]['message'])
                print(c, self.graph.nodes[c]['message'], self.graph.nodes[c]['preprocessed_message'])
            print('---')
            self.messages[cluster_id] = cluster_messages

        return self.messages

    @staticmethod
    def __get_asterisk(candidate):
        # candidate: list of list
        abstraction = ''

        # transpose row to column
        candidate_transpose = list(zip(*candidate))
        candidate_length = len(candidate)

        if candidate_length > 1:
            # get abstraction
            abstraction_list = []
            for index, message in enumerate(candidate_transpose):
                message_length = len(set(message))
                if message_length == 1:
                    abstraction_list.append(message[0])
                else:
                    abstraction_list.append('*')

            abstraction = ' '.join(abstraction_list)

        elif candidate_length == 1:
            abstraction = ' '.join(candidate[0])

        return abstraction

    def __get_abstraction_asterisk(self):
        pass

    def get_abstraction(self):
        clusters = self.__get_clusters()
        self.__get_messages()
        return clusters


def lightest(g):
    u, v, w = min(g.edges(data='weight'), key=itemgetter(2))
    return u, v

if __name__ == '__main__':
    logfile = '/home/hudan/Git/prlogparser/datasets/casper-rw/debug'
    a = Abstraction(logfile)
    results = a.get_abstraction()

    # for k, val in results.items():
    #     print(val)
