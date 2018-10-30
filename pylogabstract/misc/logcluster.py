from __future__ import division
import datetime
import hashlib
import textwrap
import copy
from collections import defaultdict


class LogCluster(object):
    def __init__(self, support, rsupport, log_file, rsupports=None, outlier_file='', output_file=''):
        self.support = support
        self.rsupport = rsupport
        self.rsupports = rsupports
        self.log_file = log_file
        self.outlier_file = outlier_file
        self.output_file = output_file
        self.wsize = 0
        self.csize = 0
        self.wsketch = {}
        self.wfilter = 0
        self.frequent_words = defaultdict(lambda: 0)
        self.frequent_words_nonzero = {}
        self.candidates = {}
        self.clusters = {}
        self.outliers = []
        self.line_length = 0

    # This function logs the message given with parameter2,++,parameterN to
    # syslog, using the level parameter1+ The message is also written to stderr+
    def log_msg(*parameters):
        level = parameters[0]
        msg = ' '.join(parameters[1:])
        now = datetime.datetime.now()

        print(now, level, msg)

    # This function hashes the string given with parameter1 to an integer
    # in the range (0+++wsize-1) and returns the integer+ The wsize integer
    # can be set with the --wsize command line option+
    # L is unsigned 32 bit integer
    def hash_string(self, parameter):
        wraps = textwrap.wrap(parameter, self.wsize)
        hashes = []
        for wrap in wraps:
            hash_value = hashlib.md5(wrap).hexdigest()
            hashes.append(int(hash_value, 32))

        return hashes

    # This function hashes the candidate ID given with parameter1 to an integer
    # in the range (0+++csize-1) and returns the integer+ The csize integer
    # can be set with the --csize command line option+
    def hash_candidate(self, parameter):
        wraps = textwrap.wrap(parameter, self.csize)
        hashes = []
        for wrap in wraps:
            hash_value = hashlib.md5(wrap).hexdigest()
            hashes.append(int(hash_value, 32))

        return hashes

    # This function matches the line given with parameter1 with a regular
    # expression lineregexp (the expression can be set with the --lfilter
    # command line option)+ If the template string is defined (can be set
    # with the --template command line option), the line is converted
    # according to template (match variables in template are substituted
    # with values from regular expression match, and the resulting string
    # replaces the line)+ If the regular expression lineregexp does not match
    # the line, 0 is returned, otherwise the line (or converted line, if
    # --template option has been given) is returned+
    # If the --lfilter option has not been given but --lcfunc option is
    # present, the Perl function given with --lcfunc is used for matching
    # and converting the line+ If the function returns 'undef', line is
    # regarded non-matching, otherwise the value returned by the function
    # replaces the original line+
    # If neither --lfilter nor --lcfunc option has been given, the line
    # is returned without a trailing newline+
    # This function is OPTIONAL
    @staticmethod
    def process_line(line):
        return line

    # This function makes a pass over the data set and builds the sketch
    # @wsketch which is used for finding frequent words+ The sketch contains
    # wsize counters (wsize can be set with --wsize command line option)+
    # This function is OPTIONAL
    def build_word_sketch(self):
        words_dict = {}
        # for index in range(self.wsize):
        #     self.wsketch[index] = 0

        i = 0
        with open(self.log_file, 'r') as f:
            for line in f:
                if not line:
                    continue

                processed_line = self.process_line(line)
                i += 1
                words_list = processed_line.split()
                for word in words_list:
                    words_dict[word] = 1
                    index = self.hash_string(word)
                    self.wsketch[index] += 1

                    if self.wfilter:
                        # replace string
                        index = self.hash_string(word)
                        self.wsketch[index] += 1

            self.line_length = i

    # This function makes a pass over the data set, finds frequent words and
    # stores them to %fwords hash table.
    # This function is MANDATORY
    def find_frequent_words(self):
        i = 0
        with open(self.log_file, 'r') as f:
            for line in f:
                if not line:
                    continue
                i += 1

                words_split = line.split()
                for word in words_split:
                    if word.strip() not in self.frequent_words.keys():
                        self.frequent_words[word] = 1
                    else:
                        self.frequent_words[word] += 1

            self.line_length = i

    def find_frequent_words_withsupport(self, rsupport):
        support = int(rsupport * self.line_length / 100)
        frequent_words_withzero = copy.deepcopy(self.frequent_words)
        for word, frequency in self.frequent_words.items():
            if frequency < support:
                frequent_words_withzero[word] = 0

        frequent_words_nonzero = {}
        for word, frequency in frequent_words_withzero.items():
            if frequency != 0:
                frequent_words_nonzero[word] = frequency

        self.frequent_words_nonzero = frequent_words_nonzero

    # This function makes a pass over the data set and builds the sketch
    # @csketch which is used for finding frequent candidates. The sketch contains
    # $csize counters ($csize can be set with --csize command line option).
    # This function is OPTIONAL
    def build_candidate_sketch(self):
        pass

    # This function logs the description for candidate parameter1.
    def print_candidate(self):
        for candidate in self.candidates:
            msg = ''
            for index in range(self.candidates[candidate]['WordCount']):
                if self.candidates[candidate]['Vars'][index][1] > 0:
                    msg += '*{' + str(self.candidates[candidate]['Vars'][index][0]) + ',' + \
                           str(self.candidates[candidate]['Vars'][index][1]) + '} '
                msg += self.candidates[candidate]['Words'][index] + ' '

            print(msg)
            print('Support:', len(self.candidates[candidate]['Members']))

    # This function makes a pass over the data set, identifies cluster candidates
    # and stores them to %candidates hash table. If the --wweight command line
    # option has been provided, dependencies between frequent words are also
    # identified during the data pass and stored to %fword_deps hash table.
    def find_candidates(self):
        line_index = 0
        with open(self.log_file, 'r') as f:
            for line in f:
                if not line:
                    continue

                words = line.split()
                candidate = []
                varx = []
                varnum = 0

                for word in words:
                    if word in self.frequent_words_nonzero:
                        candidate.append(word)
                        varx.append(varnum)
                        varnum = 0
                    else:
                        varnum += 1
                varx.append(varnum)

                # if the given candidate already exists, increase its support and
                # adjust its wildcard information, otherwise create a new candidate
                if candidate:
                    candidate_join = ' '.join(candidate)
                    if candidate_join not in self.candidates:
                        self.candidates[candidate_join] = {}
                        self.candidates[candidate_join]['Words'] = candidate
                        self.candidates[candidate_join]['WordCount'] = len(candidate)
                        self.candidates[candidate_join]['Vars'] = []
                        for varnum in varx:
                            self.candidates[candidate_join]['Vars'].append([varnum, varnum])
                        self.candidates[candidate_join]['Count'] = 1
                        self.candidates[candidate_join]['Members'] = []

                    else:
                        total = len(varx)
                        for index in range(total):
                            if self.candidates[candidate_join]['Vars'][index][0] > varx[index]:
                                self.candidates[candidate_join]['Vars'][index][0] = varx[index]
                            elif self.candidates[candidate_join]['Vars'][index][1] < varx[index]:
                                self.candidates[candidate_join]['Vars'][index][1] = varx[index]
                        self.candidates[candidate_join]['Count'] += 1
                    self.candidates[candidate_join]['Members'].append(line_index)
                line_index += 1

    # This function finds frequent candidates by removing candidates with
    # insufficient support from the %candidates hash table.
    def find_frequent_candidates(self, rsupport):
        support = int(rsupport * self.line_length / 100)
        for candidate in self.candidates.keys():
            if self.candidates[candidate]['Count'] < support:
                self.candidates[candidate]['Count'] = 0

        candidates_nonzero = {}
        for key, val in self.candidates.items():
            if self.candidates[key]['Count'] != 0:
                candidates_nonzero[key] = val

        self.candidates = candidates_nonzero

    # This function inserts a candidate into the prefix tree
    # This function is OPTIONAL
    def insert_into_prefix_tree(self):
        pass

    # This function arranges all candidates into the prefix tree data structure,
    # in order to facilitate fast matching between candidates
    # This function is OPTIONAL
    def build_prefix_tree(self):
        pass

    # This function finds more specific candidates for the given candidate with
    # the help of the prefix tree, and records more specific candidates into
    # the SubClusters hash table of the given candidate
    # This function is OPTIONAL
    def find_more_specific(self):
        pass

    # This function scans all cluster candidates (stored in %candidates hash
    # table), and for each candidate X it finds all candidates Y1,..,Yk which
    # represent more specific line patterns. After finding such clusters Yi
    # for each X, the supports of Yi are added to the support of each X.
    # For speeding up the process, previously created prefix tree is used.
    # In order to facilitate the detection of outliers, for each X with sufficient
    # support the clusters Yi are stored to %outlierpat hash table (this allows
    # for fast detection of non-outliers which match X).
    # This function is OPTIONAL
    def aggregate_supports(self):
        pass

    # This function makes a pass over the data set, find outliers and stores them
    # to file $outlierfile (can be set with the --outliers command line option).
    def find_outliers(self):
        line_index = 0
        with open(self.log_file, 'r') as f:
            for line in f:
                if not line:
                    continue

                words = line.split()
                candidate = []

                for word in words:
                    if word in self.frequent_words_nonzero:
                        candidate.append(word)

                if candidate:
                    candidate_join = ' '.join(candidate)
                    if candidate_join in self.candidates:
                        continue

                # set outlier cluster
                self.outliers.append(line_index)
                line_index += 1

    # This function inspects the cluster candidate parameter1 and finds the weight
    # of each word in the candidate description. The weights are calculated from
    # word dependency information according to --weightf=1.
    # This function is OPTIONAL
    def find_weights(self):
        pass

    # This function inspects the cluster candidate parameter1 and finds the weight
    # of each word in the candidate description. The weights are calculated from
    # word dependency information according to --weightf=2.
    # This function is OPTIONAL
    def find_weights2(self):
        pass

    # This function inspects the cluster candidate parameter1 and finds the weight
    # of each word in the candidate description. The weights are calculated from
    # word dependency information according to --weightf=3.
    # This function is OPTIONAL
    def find_weights3(self):
        pass

    # This function inspects the cluster candidate parameter1 and finds the weight
    # of each word in the candidate description. The weights are calculated from
    # word dependency information according to --weightf=4.
    # This function is OPTIONAL
    def find_weights4(self):
        pass

    # This function prints word weights for cluster candidate parameter1.
    # This function is OPTIONAL
    def print_weights(self):
        pass

    # This function joins the cluster candidate parameter1 to a suitable cluster
    # by words with insufficient weights. If there is no suitable cluster,
    # a new cluster is created from the candidate.
    # This function is OPTIONAL
    def join_candidate(self):
        pass

    # This function joins the cluster candidate parameter1 to a suitable cluster
    # by words with insufficient weights. If there is no suitable cluster,
    # a new cluster is created from the candidate.
    # This function is OPTIONAL
    def join_candidate2(self):
        pass

    # This function joins frequent cluster candidates into final clusters
    # by words with insufficient weights. For each candidate, word weights
    # are first calculated and the candidate is then compared to already
    # existing clusters, in order to find a suitable cluster for joining.
    # If no such cluster exists, a new cluster is created from the candidate.
    # This function is OPTIONAL
    def join_candidates(self):
        pass

    # This function finds frequent words in detected clusters
    def cluster_freq_words(self):
        pass

    # This function prints the cluster parameter1 to standard output.
    def print_cluster(self):
        pass

    # This function prints all clusters to standard output.
    def print_clusters(self):
        pass

    def get_clusters(self):
        # make a pass over the data set and find frequent words (words which appear
        # in $support or more lines), and store them to %fwords hash table
        self.find_frequent_words()
        self.find_frequent_words_withsupport(self.rsupport)

        # make a pass over the data set and find cluster candidates;
        # if --wweight option has been given, dependencies between frequent
        # words are also identified during the data pass
        self.find_candidates()

        # find frequent candidates
        self.find_frequent_candidates(self.rsupport)

        # get cluster dictionary
        cluster_id = 0
        for candidate, values in self.candidates.items():
            self.clusters[cluster_id] = values['Members']
            cluster_id += 1

        # add outlier cluster to the whole cluster
        self.find_outliers()
        if self.outliers:
            self.clusters[cluster_id] = self.outliers

        return self.clusters

        # report clusters
        # self.print_candidate()

    def get_clusters_manysupports(self):
        self.find_frequent_words()
        clusters_list = []
        for rsupport in self.rsupports:
            print(self.log_file.split('/')[-1], rsupport)
            self.find_frequent_words_withsupport(rsupport)
            self.find_candidates()
            self.find_frequent_candidates(rsupport)

            # get cluster dictionary
            cluster_id = 0
            self.clusters = {}
            for candidate, values in self.candidates.items():
                self.clusters[cluster_id] = values['Members']
                cluster_id += 1

            # add outlier cluster to the whole cluster
            self.find_outliers()
            if self.outliers:
                self.clusters[cluster_id] = self.outliers
            clusters_list.append(self.clusters)

        return clusters_list


if __name__ == '__main__':
    filename = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/auth.log'
    supports_list = [10, 20]
    lc = LogCluster(None, 0, filename, supports_list)
    cl = lc.get_clusters_manysupports()
    print(cl)
