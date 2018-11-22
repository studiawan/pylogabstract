"""
Description : This file implements the LogMine algorithm for log parsing
Author      : LogPAI team
License     : MIT
"""

import sys
import re
import os
from pylogabstract.misc.alignment import *
import copy
import hashlib
import pandas as pd
from datetime import datetime
from collections import defaultdict
from pylogabstract.misc.misc_utility import MiscUtility


class partition():
    def __init__(self, idx, log="", lev=-1):
        self.logs_idx = [idx]
        self.patterns = [log]
        self.level = lev


class LogMine():
    def __init__(self, indir, outdir, log_format, max_dist=0.001, levels=2, k=1, k1=1, k2=1, alpha=100, rex=[],
                 parsed_logs=None):
        self.logformat = log_format
        self.path = indir
        self.savePath = outdir
        self.rex = rex
        self.levels = levels
        self.max_dist = max_dist
        self.k = k
        self.k1 = k1
        self.k2 = k2
        self.alpha = alpha
        self.df_log = None
        self.logname = None
        self.level_clusters = {}
        self.abstractions = {}
        self.raw_logs = {}
        self.parsed_logs = parsed_logs

    def parse(self, logname):
        # print('Parsing file: ' + os.path.join(self.path, logname))
        self.logname = logname
        starttime = datetime.now()
        self.load_data()
        for lev in range(self.levels):
            if lev == 0:
                # Clustering
                self.level_clusters[0] = self.get_clusters(self.df_log['Content_'], lev)
            else:
                # Clustering
                patterns = [c.patterns[0] for c in self.level_clusters[lev-1]]
                self.max_dist *= self.alpha
                clusters = self.get_clusters(patterns, lev, self.level_clusters[lev-1])

                # Generate patterns
                for cluster in clusters:
                    cluster.patterns = [self.sequential_merge(cluster.patterns)]
                self.level_clusters[lev] = clusters
        self.dump()
        # print('Parsing done. [Time taken: {!s}]'.format(datetime.now() - starttime))

    def dump(self):
        # if not os.path.isdir(self.savePath):
        #     os.makedirs(self.savePath)

        templates = [0] * self.df_log.shape[0]
        ids = [0] * self.df_log.shape[0]
        templates_occ = defaultdict(int)

        abstraction_temp = defaultdict(list)
        for cluster in self.level_clusters[self.levels-1]:
            EventTemplate = cluster.patterns[0]
            EventId = hashlib.md5(' '.join(EventTemplate).encode('utf-8')).hexdigest()[0:8]
            Occurences = len(cluster.logs_idx)
            templates_occ[EventTemplate] += Occurences

            for idx in cluster.logs_idx:
                ids[idx] = EventId
                templates[idx] = EventTemplate

            abstraction_temp[EventTemplate].extend(cluster.logs_idx)

        abstraction_id = 0
        for abstraction_str, log_ids in abstraction_temp.items():
            self.abstractions[abstraction_id] = {
                'abstraction': abstraction_str,
                'log_id': log_ids
            }
            abstraction_id += 1

        self.df_log['EventId'] = ids
        self.df_log['EventTemplate'] = templates

        occ_dict = dict(self.df_log['EventTemplate'].value_counts())
        df_event = pd.DataFrame()
        df_event['EventTemplate'] = self.df_log['EventTemplate'].unique()
        df_event['Occurrences'] = self.df_log['EventTemplate'].map(occ_dict)
        df_event['EventId'] = self.df_log['EventTemplate'].map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()[0:8])

        # self.df_log.drop("Content_", inplace=True, axis=1)
        # self.df_log.to_csv(os.path.join(self.savePath, self.logname + '_structured.csv'), index=False)
        # df_event.to_csv(os.path.join(self.savePath, self.logname + '_templates.csv'), index=False, columns=["EventId","EventTemplate","Occurrences"])

    def get_clusters(self, logs, lev, old_clusters=None):
        clusters = []
        old_clusters = copy.deepcopy(old_clusters)
        for logidx, log in enumerate(logs):
            match = False
            for cluster in clusters:
                dis = self.msgDist(log, cluster.patterns[0]) if lev == 0 else self.patternDist(log, cluster.patterns[0])
                if dis and dis < self.max_dist:
                    if lev == 0:
                        cluster.logs_idx.append(logidx)
                    else:
                        cluster.logs_idx.extend(old_clusters[logidx].logs_idx)
                        cluster.patterns.append(old_clusters[logidx].patterns[0])
                    match = True

            if not match:
                if lev == 0:
                    clusters.append(partition(logidx, log, lev)) # generate new cluster
                else:
                    old_clusters[logidx].level = lev
                    clusters.append(old_clusters[logidx]) # keep old cluster

        return clusters

    def sequential_merge(self, logs):
        log_merged = logs[0]
        for log in logs[1:]:
            log_merged = self.pair_merge(log_merged, log)
        return log_merged

    def pair_merge(self, loga, logb):
        loga, logb = water(loga.split(), logb.split())
        logn = []
        for idx, value in enumerate(loga):
            logn.append('<*>' if value != logb[idx] else value)
        return " ".join(logn)

    def print_cluster(self, cluster):
        # print "------start------"
        # print "level: {}".format(cluster.level)
        # print "idxs: {}".format(cluster.logs_idx)
        # print "patterns: {}".format(cluster.patterns)
        # print "count: {}".format(len(cluster.patterns))
        # for idx in cluster.logs_idx:
        #     print self.df_log.iloc[idx]['Content_']
        # print "------end------"
        pass

    def msgDist(self, seqP, seqQ):
        dis = 1
        seqP = seqP.split()
        seqQ = seqQ.split()
        maxlen = max(len(seqP), len(seqQ))
        minlen = min(len(seqP), len(seqQ))
        for i in range(minlen):
            dis -= (self.k if seqP[i]==seqQ[i] else 0 * 1.0) / maxlen
        return dis

    def patternDist(self, seqP, seqQ):
        dis = 1
        seqP = seqP.split()
        seqQ = seqQ.split()
        maxlen = max(len(seqP), len(seqQ))
        minlen = min(len(seqP), len(seqQ))
        for i in range(minlen):
            if seqP[i] == seqQ[i]:
                if seqP[i] == "<*>":
                    dis -= self.k2 * 1.0 / maxlen
                else:
                    dis -= self.k1 * 1.0 / maxlen
        return dis

    def load_data(self):
        def preprocess(line):
            for currentRex in self.rex:
                line = re.sub(currentRex, '', line)
            return line

        headers, regex = self.generate_logformat_regex(self.logformat)
        self.df_log = self.log_to_dataframe(os.path.join(self.path, self.logname), regex, headers, self.logformat)
        self.df_log['Content_'] = self.df_log['Content'].map(preprocess)

    def log_to_dataframe(self, log_file, regex, headers, logformat):
        ''' Function to transform log file to dataframe '''
        log_messages = []
        linecount = 0
        with open(log_file, 'r') as fin:
            for line in fin.readlines():
                try:
                    match = regex.search(line.strip())
                    message = [match.group(header) for header in headers]
                    log_messages.append(message)

                    self.raw_logs[linecount] = line
                    linecount += 1
                except Exception as e:
                    pass
        logdf = pd.DataFrame(log_messages, columns=headers)
        logdf.insert(0, 'LineId', None)
        logdf['LineId'] = [i + 1 for i in range(linecount)]
        return logdf

    def generate_logformat_regex(self, logformat):
        '''
        Function to generate regular expression to split log messages
        '''
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' +', '\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                regex += '(?P<%s>.*?)' % header
                headers.append(header)
        regex = re.compile('^' + regex + '$')
        return headers, regex

    def get_abstractions(self):
        self.abstractions = self.__get_final_abstraction(self.abstractions)
        return self.abstractions, self.raw_logs

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

    def __get_final_abstraction(self, abstractions):
        # restart abstraction id from 0, get abstraction and its log ids
        # in this method, we include other fields such as timestamp, hostname, ip address, etc in abstraction
        final_abstractions = {}
        abstraction_id = 0

        parsed_logs = self.parsed_logs
        for abs_id, abstraction in abstractions.items():
            # get entities from raw logs per cluster (except the main message)
            candidates = defaultdict(list)
            candidates_log_ids = defaultdict(list)
            candidates_messages = defaultdict(list)
            log_ids = abstraction['log_id']

            for log_id in log_ids:
                parsed = parsed_logs[log_id]
                values = []
                values_length = 0
                message = []
                for label, value in parsed.items():
                    if label != 'message':
                        value_split = value.split()
                        values.extend(value_split)
                        values_length += len(value_split)

                    # get the message here
                    else:
                        message_split = value.split()
                        message.extend(message_split)

                # get abstraction candidates and their respective log ids
                candidates[values_length].append(values)
                candidates_log_ids[values_length].append(log_id)
                candidates_messages[values_length].append(message)

            # get asterisk for entity and message and then set final abstraction
            for label_length, candidate in candidates.items():
                entity_abstraction = self.__get_asterisk(candidate)
                message_abstraction = self.__get_asterisk(candidates_messages[label_length])
                final_abstractions[abstraction_id] = {
                    'abstraction': entity_abstraction + ' ' + message_abstraction,
                    'log_id': candidates_log_ids[label_length]
                }
                abstraction_id += 1

        return final_abstractions


if __name__ == '__main__':
    input_dir = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/'     # The input directory of log file
    output_dir = ''  # The output directory of parsing results
    log_file = 'auth.log'   # The input log file name
    log_format = '<Content>'  # HDFS log format
    levels = 2  # The levels of hierarchy of patterns
    max_dist = 0.001    # The maximum distance between any log message in a cluster and the cluster representative
    k = 1  # The message distance weight (default: 1)
    regex = []  # Regular expression list for optional preprocessing (default: [])

    # parse logs
    utility = MiscUtility()
    path = input_dir + log_file
    msg_dir = '/home/hudan/Git/pylogabstract/results/'
    msg_file = msg_dir + log_file
    parsedlogs, _ = utility.write_parsed_message(path, msg_file)

    parser = LogMine(msg_dir, output_dir, log_format, rex=regex, levels=levels, max_dist=max_dist, k=k,
                     parsed_logs=parsedlogs)
    parser.parse(log_file)
    abstraction_results, rawlogs = parser.get_abstractions()

    for k, v in abstraction_results.items():
        print(k, v)
