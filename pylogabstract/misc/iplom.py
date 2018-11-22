"""This is implementation of IPLoM method proposed by Makanju et al. in [Adetokunbo2009]_ and [Adetokunbo2012]_.

Original source of IPLoM from He et al. based on the reference [He2016a]_ and it is available
on GitHub [He2016b]_. We modify the original source to comply Python code style and add some methods
to get cluster. The methods in this class has not documented and please refer to [He2016b]_
to get more complete descriptions.

References
----------
.. [Adetokunbo2009] M. Adetokunbo, A. N. Zincir-Heywood, and E. E. Milios, Clustering event logs using
                    iterative partitioning, Proceedings of the 15th ACM SIGKDD International Conference on
                    Knowledge Discovery and Data Mining, 2009, pp. 1255-1264.
.. [Adetokunbo2012] M. Adetokunbo, A. N. Zincir-Heywood, and E. E. Milios, A lightweight algorithm for
                    message type extraction in system application logs, IEEE Transactions on Knowledge
                    and Data Engineering, 2012, 24(11), pp. 1921-1936.
.. [He2016a]        P. He, J. Zhu, S. He, J. Li, and R. Lyu, An evaluation study on log parsing and its use
                    in log mining, Proceedings of the 46th Annual IEEE/IFIP International Conference on
                    Dependable Systems and Networks, 2016.
.. [He2016b]        P. He, Logparser: A python package of log parsers with benchmarks for log template extraction.
                    https://github.com/cuhk-cse/logparser.
"""

import copy
import sys
import time
import os
import gc
import re
from collections import defaultdict
from pylogabstract.misc.misc_utility import MiscUtility


class Partition:
    """Wrap around the logs and the step number
    """

    def __init__(self, step_no, num_of_logs=0, len_of_logs=0):
        self.logLL = []
        self.stepNo = step_no
        self.valid = True
        self.numOfLogs = num_of_logs
        self.lenOfLogs = len_of_logs


class Event:
    eventId = 1

    def __init__(self, event_str):
        self.eventStr = event_str
        self.eventId = Event.eventId
        self.eventCount = 0
        Event.eventId += 1


class ParaIPLoM:
    def __init__(self, path='', logname='', save_path='',
                 save_file_name='template', max_event_len=120, step2support=0, pst=0.0,
                 ct=0.175, lower_bound=0.25, upper_bound=0.9, use_pst=False,
                 removable=True, remove_col=[], regular=True, rex=['([0-9]+\.){3}[0-9]'], parsed_logs=None):
        """This is the constructor of class Para.

        Parameters
        ----------
        max_event_len   : int
            The length of the longest log/event, which is used in step 1 to split logs into partitions
            according to their length.
        path            : str
            The path of the input file.
        step2support    : int
            The support threshold to create a new partition, partitions which contains less than step2Support logs
            will not go through step 2.
        pst             : float
            Partition support ratio threshold.
        ct              : float
            Cluster goodness threshold used in DetermineP1P2 in step 3. If the columns with unique term
            more than ct, we skip step 3.
        """
        self.maxEventLen = max_event_len
        self.path = path
        self.logname = logname
        self.savePath = save_path
        self.saveFileName = save_file_name
        self.step2Support = step2support
        self.PST = pst
        self.CT = ct
        self.lowerBound = lower_bound
        self.upperBound = upper_bound
        self.usePST = use_pst
        self.removable = removable
        self.removeCol = remove_col
        self.regular = regular
        self.rex = rex
        self.parsed_logs = parsed_logs
        # print 'max', self.maxEventLen


class IPLoM:
    def __init__(self, para):
        self.para = para
        self.partitions_L = []
        self.eventsL = []
        self.output = []
        self.logs = {}
        self.parsed_logs = {}

        # Initialize some partitions which contain logs with different length
        for logLen in range(self.para.maxEventLen + 1):
            self.partitions_L.append(Partition(step_no=1, num_of_logs=0, len_of_logs=logLen))

    def main_process(self):
        t1 = time.time()
        self.step1()
        self.step2()
        self.step3()
        self.step4()
        self.get_output()
        t2 = time.time()

        # if not os.path.exists(self.para.savePath):
        #    os.makedirs(self.para.savePath)
        # else:
        #    self.delete_all_files(self.para.savePath)

        # self.write_event_tofile(self.para.savePath + 'logTemplates.txt')
        # self.write_log_with_eventid(self.para.savePath + self.para.saveFileName)

        # self.PrintPartitions()
        # clusters = self.get_clusters()
        # for cluster in clusters:
        #    print cluster

        # print('this process takes', t2 - t1)
        # print('*********************************************')
        gc.collect()
        Event.eventId = 1
        return t2 - t1

    def step1(self):
        with open(self.para.path + self.para.logname) as lines:
            line_count = 1
            for line in lines:
                if line not in ['\n', '\r\n']:
                    self.logs[line_count - 1] = line
                    # If line is empty, skip
                    if line.strip() == "":
                        continue

                    if self.para.regular:
                        for currentRex in self.para.rex:
                            line = re.sub(currentRex, '', line)
                        line = re.sub('node-[0-9]+', 'node-', line)  # For HPC only

                    word_seq = line.strip().split()
                    # word_seq = line.strip().split('\t')[1].split()
                    # print (wordSeq)
                    if self.para.removable:
                        word_seq = [word for i, word in enumerate(word_seq) if i not in self.para.removeCol]
                    # print (wordSeq)

                    # Generate terms list, with ID in the end
                    word_seq.append(str(line_count))
                    # print (wordSeq)
                    line_count += 1
                    # if lineCount%100 == 0:
                    # 	print(lineCount)

                    # Add current log to the corresponding partition
                    self.partitions_L[len(word_seq) - 1].logLL.append(word_seq)
                    self.partitions_L[len(word_seq) - 1].numOfLogs += 1

            for partition in self.partitions_L:
                if partition.numOfLogs == 0:
                    partition.valid = False

                elif self.para.usePST and 1.0 * partition.numOfLogs / line_count < self.para.PST:
                    for logL in partition.logLL:
                        self.partitions_L[0].logLL.append(logL)
                        self.partitions_L[0].numOfLogs += 1
                    partition.valid = False

    def step2(self):
        for partition in self.partitions_L:
            if not partition.valid:
                continue

            if partition.numOfLogs <= self.para.step2Support:
                continue

            # Avoid going through newly generated partitions
            if partition.stepNo == 2:
                break

            """
            For each column, create a set to hold the unique tokens in that column.
            And finally, calculate the number of the unique tokens in each column
            """
            unique_tokens_count_ls = []
            for columnIdx in range(partition.lenOfLogs):
                unique_tokens_count_ls.append(set())

            for logL in partition.logLL:
                for columnIdx in range(partition.lenOfLogs):
                    unique_tokens_count_ls[columnIdx].add(logL[columnIdx])

            # Find the column with minimum unique tokens
            min_column_idx = 0
            min_column_count = len(unique_tokens_count_ls[0])

            for columnIdx in range(partition.lenOfLogs):
                if min_column_count > len(unique_tokens_count_ls[columnIdx]):
                    min_column_count = len(unique_tokens_count_ls[columnIdx])
                    min_column_idx = columnIdx

            # If there is one column with one unique term, do not split this partition
            if min_column_count == 1:
                continue

            # From split-token to log list
            log_dll = {}
            for logL in partition.logLL:
                if logL[min_column_idx] not in log_dll:
                    log_dll[logL[min_column_idx]] = []
                log_dll[logL[min_column_idx]].append(logL)

            for key in log_dll:
                if self.para.usePST and 1.0 * len(log_dll[key]) / partition.numOfLogs < self.para.PST:
                    self.partitions_L[0].logLL += log_dll[key]
                    self.partitions_L[0].numOfLogs += len(log_dll[key])
                else:
                    new_partition = Partition(step_no=2, num_of_logs=len(log_dll[key]), len_of_logs=partition.lenOfLogs)
                    new_partition.logLL = log_dll[key]
                    self.partitions_L.append(new_partition)

            partition.valid = False

    def step3(self):
        for partition in self.partitions_L:
            if not partition.valid:
                continue

            if partition.stepNo == 3:
                break

            # Debug
            # print ("*******************************************")
            # print ("Step 2 Partition:")
            # print ("*******************************************")
            # for logL in partition.logLL:
            # 	print (' '.join(logL))

            # Find two columns that my cause split in this step
            p1, p2 = self.determine_p1p2(partition)

            if p1 == -1 or p2 == -1:
                continue
            try:
                p1_set = set()
                p2_set = set()
                map_relation_1ds = {}
                map_relation_2ds = {}

                # Construct token sets for p1 and p2, dictionary to record the mapping relations between p1 and p2
                for logL in partition.logLL:
                    p1_set.add(logL[p1])
                    p2_set.add(logL[p2])

                    if logL[p1] == logL[p2]:
                        print("!!  p1 may be equal to p2")

                    if logL[p1] not in map_relation_1ds:
                        map_relation_1ds[logL[p1]] = set()
                    map_relation_1ds[logL[p1]].add(logL[p2])

                    if logL[p2] not in map_relation_2ds:
                        map_relation_2ds[logL[p2]] = set()
                    map_relation_2ds[logL[p2]].add(logL[p1])

                # originp1S = copy.deepcopy(p1Set)
                # originp2S = copy.deepcopy(p2Set)

                # Construct sets to record the tokens in 1-1, 1-M, M-1 relationships, the left-tokens in p1Set & p2Set
                # are in M-M relationships
                one_to_one_s = set()
                one_to_m_p1d = {}
                one_to_m_p2d = {}

                # select 1-1 and 1-M relationships
                for p1Token in p1_set:
                    if len(map_relation_1ds[p1Token]) == 1:
                        if len(map_relation_2ds[list(map_relation_1ds[p1Token])[0]]) == 1:
                            one_to_one_s.add(p1Token)
                    else:
                        is_one_to_m = True
                        for p2Token in map_relation_1ds[p1Token]:
                            if len(map_relation_2ds[p2Token]) != 1:
                                is_one_to_m = False
                                break
                        if is_one_to_m:
                            one_to_m_p1d[p1Token] = 0

                # delete the tokens which are picked to 1-1 and 1-M relationships from p1Set, so that the left are M-M
                for deleteToken in one_to_one_s:
                    p1_set.remove(deleteToken)
                    p2_set.remove(list(map_relation_1ds[deleteToken])[0])

                for deleteToken in one_to_m_p1d:
                    for deleteTokenP2 in map_relation_1ds[deleteToken]:
                        p2_set.remove(deleteTokenP2)
                    p1_set.remove(deleteToken)

                # select M-1 relationships
                for p2Token in p2_set:
                    if len(map_relation_2ds[p2Token]) != 1:
                        is_one_to_m = True
                        for p1Token in map_relation_2ds[p2Token]:
                            if len(map_relation_1ds[p1Token]) != 1:
                                is_one_to_m = False
                                break
                        if is_one_to_m:
                            one_to_m_p2d[p2Token] = 0

                # delete the tokens which are picked to M-1 relationships from p2Set, so that the left are M-M
                for deleteToken in one_to_m_p2d:
                    p2_set.remove(deleteToken)
                    for deleteTokenP1 in map_relation_2ds[deleteToken]:
                        p1_set.remove(deleteTokenP1)

                # calculate the #Lines_that_match_S
                for logL in partition.logLL:
                    if logL[p1] in one_to_m_p1d:
                        one_to_m_p1d[logL[p1]] += 1

                    if logL[p2] in one_to_m_p2d:
                        one_to_m_p2d[logL[p2]] += 1

            except KeyError as er:
                print(er)
                print('erre: ' + str(p1) + '\t' + str(p2))

            # print ('set1:')
            # for setTerm in originp1S:
            # 	print (setTerm)

            # print ('set2:')
            # for setTerm in originp2S:
            # 	print (setTerm)

            new_partitions_d = {}
            if partition.stepNo == 2:
                new_partitions_d["dumpKeyforMMrelationInStep2__"] = Partition(step_no=3, num_of_logs=0,
                                                                              len_of_logs=partition.lenOfLogs)
            # Split partition
            for logL in partition.logLL:
                # If is 1-1
                if logL[p1] in one_to_one_s:
                    if logL[p1] not in new_partitions_d:
                        new_partitions_d[logL[p1]] = Partition(step_no=3, num_of_logs=0,
                                                               len_of_logs=partition.lenOfLogs)
                    new_partitions_d[logL[p1]].logLL.append(logL)
                    new_partitions_d[logL[p1]].numOfLogs += 1

                # This part can be improved. The split_rank can be calculated once.
                # If is 1-M
                elif logL[p1] in one_to_m_p1d:
                    # print ('1-M: ' + str(len( mapRelation1DS[logL[p1]] )) + str(oneToMP1D[logL[p1]]))
                    split_rank = self.get_rank_posistion(len(map_relation_1ds[logL[p1]]), one_to_m_p1d[logL[p1]], True)
                    # print ('result: ' + str(split_rank))
                    if split_rank == 1:
                        if logL[p1] not in new_partitions_d:
                            new_partitions_d[logL[p1]] = Partition(step_no=3, num_of_logs=0,
                                                                   len_of_logs=partition.lenOfLogs)
                        new_partitions_d[logL[p1]].logLL.append(logL)
                        new_partitions_d[logL[p1]].numOfLogs += 1
                    else:
                        if logL[p2] not in new_partitions_d:
                            new_partitions_d[logL[p2]] = Partition(step_no=3, num_of_logs=0,
                                                                   len_of_logs=partition.lenOfLogs)
                        new_partitions_d[logL[p2]].logLL.append(logL)
                        new_partitions_d[logL[p2]].numOfLogs += 1

                # If is M-1
                elif logL[p2] in one_to_m_p2d:
                    # print ('M-1: ' + str(len( mapRelation2DS[logL[p2]] )) + str(oneToMP2D[logL[p2]]))
                    split_rank = self.get_rank_posistion(len(map_relation_2ds[logL[p2]]), one_to_m_p2d[logL[p2]], False)
                    # print ('result: ' + str(split_rank))
                    if split_rank == 1:
                        if logL[p1] not in new_partitions_d:
                            new_partitions_d[logL[p1]] = Partition(step_no=3, num_of_logs=0,
                                                                   len_of_logs=partition.lenOfLogs)
                        new_partitions_d[logL[p1]].logLL.append(logL)
                        new_partitions_d[logL[p1]].numOfLogs += 1
                    else:
                        if logL[p2] not in new_partitions_d:
                            new_partitions_d[logL[p2]] = Partition(step_no=3, num_of_logs=0,
                                                                   len_of_logs=partition.lenOfLogs)
                        new_partitions_d[logL[p2]].logLL.append(logL)
                        new_partitions_d[logL[p2]].numOfLogs += 1

                # M-M
                else:
                    if partition.stepNo == 2:
                        new_partitions_d["dumpKeyforMMrelationInStep2__"].logLL.append(logL)
                        new_partitions_d["dumpKeyforMMrelationInStep2__"].numOfLogs += 1
                    else:
                        if len(p1_set) < len(p2_set):
                            if logL[p1] not in new_partitions_d:
                                new_partitions_d[logL[p1]] = Partition(step_no=3, num_of_logs=0,
                                                                       len_of_logs=partition.lenOfLogs)
                            new_partitions_d[logL[p1]].logLL.append(logL)
                            new_partitions_d[logL[p1]].numOfLogs += 1
                        else:
                            if logL[p2] not in new_partitions_d:
                                new_partitions_d[logL[p2]] = Partition(step_no=3, num_of_logs=0,
                                                                       len_of_logs=partition.lenOfLogs)
                            new_partitions_d[logL[p2]].logLL.append(logL)
                            new_partitions_d[logL[p2]].numOfLogs += 1

                            # debug
                            # print ('p1: ' + str(p1) + '\t' + 'p2: ' + str(p2))
                            # print ("*******************************************")
                            # print ("Step 3 1-1:")
                            # print ("*******************************************")
                            # for logL in partition.logLL:
                            # if logL[p1] in oneToOneS:
                            # print (' '.join(logL))

                            # print ("*******************************************")
                            # print ("Step 3 1-M:")
                            # print ("*******************************************")
                            # for logL in partition.logLL:
                            # if logL[p1] in oneToMP1D:
                            # print (' '.join(logL))

                            # print ("*******************************************")
                            # print ("Step 3 M-1:")
                            # print ("*******************************************")
                            # for logL in partition.logLL:
                            # if logL[p2] in oneToMP2D:
                            # print (' '.join(logL))

                            # if partition.stepNo == 2:
                            # print ("*******************************************")
                            # print ("Step 3 M-M: from 2")
                            # print ("*******************************************")
                            # for logL in newPartitionsD["dumpKeyforMMrelationInStep2__"].logLL:
                            # print (' '.join(logL))
                            # else:
                            # print ("$$$$$$$$$$$$From step 1 and split for M-M$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

            # print ("Debug End")

            if "dumpKeyforMMrelationInStep2__" in new_partitions_d \
                    and new_partitions_d["dumpKeyforMMrelationInStep2__"].numOfLogs == 0:
                new_partitions_d["dumpKeyforMMrelationInStep2__"].valid = False
            # Add all the new partitions to collection
            for key in new_partitions_d:
                if self.para.usePST and 1.0 * new_partitions_d[key].numOfLogs / partition.numOfLogs < self.para.PST:
                    self.partitions_L[0].logLL += new_partitions_d[key].logLL
                    self.partitions_L[0].numOfLogs += new_partitions_d[key].numOfLogs
                else:
                    self.partitions_L.append(new_partitions_d[key])

            partition.valid = False

    def step4(self):
        self.partitions_L[0].valid = False
        if not self.para.usePST and self.partitions_L[0].numOfLogs != 0:
            event = Event(['Outlier'])
            event.eventCount = self.partitions_L[0].numOfLogs
            self.eventsL.append(event)

            for logL in self.partitions_L[0].logLL:
                logL.append(str(event.eventId))

        for partition in self.partitions_L:
            if not partition.valid:
                continue

            if partition.numOfLogs == 0:
                print(str(partition.stepNo) + '\t')

            unique_tokens_count_ls = []
            for columnIdx in range(partition.lenOfLogs):
                unique_tokens_count_ls.append(set())

            for logL in partition.logLL:
                for columnIdx in range(partition.lenOfLogs):
                    unique_tokens_count_ls[columnIdx].add(logL[columnIdx])

            e = copy.deepcopy(partition.logLL[0])[:partition.lenOfLogs]
            for columnIdx in range(partition.lenOfLogs):
                if len(unique_tokens_count_ls[columnIdx]) == 1:
                    continue
                else:
                    e[columnIdx] = '*'
            event = Event(e)
            event.eventCount = partition.numOfLogs

            self.eventsL.append(event)

            for logL in partition.logLL:
                logL.append(str(event.eventId))

    def get_output(self):
        if not self.para.usePST and self.partitions_L[0].numOfLogs != 0:
            for logL in self.partitions_L[0].logLL:
                self.output.append(logL[-2:] + logL[:-2])
        for partition in self.partitions_L:
            if not partition.valid:
                continue
            for logL in partition.logLL:
                self.output.append(logL[-2:] + logL[:-2])
                # self.output.sort(key = lambda logL:int(logL[0]))
                # print ("output log length: " + str(len(self.output)))

    def write_event_tofile(self, event_file_path):
        write_event = open(event_file_path, 'w')
        for event in self.eventsL:
            write_event.write(str(event.eventId) + '\t' + ' '.join(event.eventStr) + '\n')
        write_event.close()

    def write_log_with_eventid(self, output_path):
        for logL in self.output:
            current_event_id = logL[1]
            write_output = open(output_path + str(current_event_id) + '.txt', 'a')
            log_str = str(logL[0]) + '\t' + ' '.join(logL[2:])
            write_output.write(log_str + '\n')
            write_output.close()

    """
    For 1-M and M-1 mappings, you need to decide whether M side are constants or variables.
    This method is to decide which side to split

    cardOfS           : The number of unique values in this set
    Lines_that_match_S: The number of lines that have these values
    one_m             : If the mapping is 1-M, this value is True. Otherwise, False
    """

    def get_rank_posistion(self, card_of_s, lines_that_match_s, one_m):
        distance = 0.0
        try:
            distance = 1.0 * card_of_s / lines_that_match_s
        except ZeroDivisionError as er1:
            print(er1)
            print("card_of_s: " + str(card_of_s) + '\t' + 'lines_that_match_s: ' + str(lines_that_match_s))
        if distance <= self.para.lowerBound:
            if one_m:
                split_rank = 2
            else:
                split_rank = 1
        elif distance >= self.para.upperBound:
            if one_m:
                split_rank = 1
            else:
                split_rank = 2
        else:
            if one_m:
                split_rank = 1
            else:
                split_rank = 2

        return split_rank

    def determine_p1p2(self, partition):
        if partition.lenOfLogs > 2:
            count_1 = 0

            unique_tokens_count_ls = []
            for columnIdx in range(partition.lenOfLogs):
                unique_tokens_count_ls.append(set())

            for logL in partition.logLL:
                for columnIdx in range(partition.lenOfLogs):
                    unique_tokens_count_ls[columnIdx].add(logL[columnIdx])

            # Count how many columns have only one unique term
            for columnIdx in range(partition.lenOfLogs):
                if len(unique_tokens_count_ls[columnIdx]) == 1:
                    count_1 += 1

            # Debug
            # strDebug = ''
            # for columnIdx in range(partition.lenOfLogs):
            # 	strDebug += str(len(uniqueTokensCountLS[columnIdx])) + ' '
            # print (strDebug)
            # If the columns with unique term more than a threshold, we return (-1, -1) to skip step 3
            gc = 1.0 * count_1 / partition.lenOfLogs

            if gc < self.para.CT:
                return self.get_mapping_position(partition, unique_tokens_count_ls)
            else:
                return -1, -1

        elif partition.lenOfLogs == 2:
            return 0, 1
        else:
            return -1, -1

    def get_mapping_position(self, partition, unique_tokens_count_ls):
        p1 = p2 = -1

        # Caculate #unqiueterms in each column, and record how many column with each #uniqueterms
        num_of_unique_tokens_d = {}
        for columnIdx in range(partition.lenOfLogs):
            if len(unique_tokens_count_ls[columnIdx]) not in num_of_unique_tokens_d:
                num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] = 0
            num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] += 1

        if partition.stepNo == 2:
            # Find the largest card and second largest card
            max_idx = second_max_idx = -1
            max_count = second_max_count = 0
            for key in num_of_unique_tokens_d:
                if num_of_unique_tokens_d[key] > max_count:
                    second_max_idx = max_idx
                    second_max_count = max_count
                    max_idx = key
                    max_count = num_of_unique_tokens_d[key]
                elif num_of_unique_tokens_d[key] > second_max_count and num_of_unique_tokens_d[key] != max_count:
                    second_max_idx = key
                    second_max_count = num_of_unique_tokens_d[key]

            # Debug
            # print ("largestIdx: " + str(maxIdx) + '\t' + "secondIdx: " + str(secondMaxIdx) + '\t')
            # print ("largest: " + str(maxCount) + '\t' + "second: " + str(secondMaxCount) + '\t')

            # If the frequency of the freq_card>1 then
            if max_idx > 1:
                for columnIdx in range(partition.lenOfLogs):
                    if num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] == max_count:
                        # if len( unique_tokens_count_ls[columnIdx] ) == maxIdx:
                        if p1 == -1:
                            p1 = columnIdx
                        else:
                            p2 = columnIdx
                            break

                for columnIdx in range(partition.lenOfLogs):
                    if p2 != -1:
                        break
                    if num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] == second_max_count:
                        # if len( unique_tokens_count_ls[columnIdx] ) == secondMaxIdx:
                        p2 = columnIdx
                        break

            # If the frequency of the freq_card==1 then
            else:
                for columnIdx in range(len(unique_tokens_count_ls)):
                    if num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] == max_count:
                        # if len( unique_tokens_count_ls[columnIdx] ) == maxIdx:
                        p1 = columnIdx
                        break

                for columnIdx in range(len(unique_tokens_count_ls)):
                    if num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] == second_max_count:
                        # if len( unique_tokens_count_ls[columnIdx] ) == secondMaxIdx:
                        p2 = columnIdx
                        break

            if p1 == -1 or p2 == -1:
                return -1, -1
            else:
                return p1, p2

        # If it is from step 1
        else:
            min_idx = second_min_idx = -1
            min_count = second_min_count = sys.maxsize
            for key in num_of_unique_tokens_d:
                if num_of_unique_tokens_d[key] < min_count:
                    second_min_idx = min_idx
                    second_min_count = min_count
                    min_idx = key
                    min_count = num_of_unique_tokens_d[key]
                elif num_of_unique_tokens_d[key] < second_min_count and num_of_unique_tokens_d[key] != min_count:
                    second_min_idx = key
                    second_min_count = num_of_unique_tokens_d[key]

            # Debug
            # print ("smallestIdx: " + str(minIdx) + '\t' + "secondIdx: " + str(secondMinIdx) + '\t')
            # print ("smallest: " + str(minCount) + '\t' + "second: " + str(secondMinCount) + '\t')

            for columnIdx in range(len(unique_tokens_count_ls)):
                if num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] == min_count:
                    # if len( unique_tokens_count_ls[columnIdx] ) == minIdx:
                    if p1 == -1:
                        p1 = columnIdx
                        break

            for columnIdx in range(len(unique_tokens_count_ls)):
                if num_of_unique_tokens_d[len(unique_tokens_count_ls[columnIdx])] == second_min_count:
                    # if len( unique_tokens_count_ls[columnIdx] ) == secondMinIdx:
                    p2 = columnIdx
                    break

            return p1, p2

    def print_partitions(self):
        abstractions = defaultdict(list)
        for idx in range(len(self.partitions_L)):
            if self.partitions_L[idx].valid:
                # print ('Partition {}:(from step {}) Valid:{}'.format(idx, self.partitions_L[idx].stepNo,
                #                                                     self.partitions_L[idx].valid))

                for log in self.partitions_L[idx].logLL:
                    print(log[-2], log[-1])
                    abstractions[log[-1]].append(log[-2])
                    # get log line number
                print("*****************************************")

        for k, v in abstractions.items():
            print(k, v)

    def get_clusters(self):
        clusters = {}
        cluster_id = 0
        for idx in range(len(self.partitions_L)):
            if self.partitions_L[idx].valid:
                cluster = []
                for log in self.partitions_L[idx].logLL:
                    cluster.append(int(log[-2]) - 1)  # zero-based index
                clusters[cluster_id] = cluster
                cluster_id += 1

        return clusters

    def get_logs(self):
        return self.logs

    def print_event_stats(self):
        for event in self.eventsL:
            if event.eventCount > 1:
                print(str(event.eventId) + '\t' + str(event.eventCount))
                print(event.eventStr)

    def delete_all_files(self, dir_path):
        file_list = os.listdir(dir_path)
        for fileName in file_list:
            os.remove(dir_path + "/" + fileName)

    def get_abstraction(self):
        # final_abstractions[abstraction_id] = {'abstraction': str, 'log_id': [int, ...]}
        absid_logid = defaultdict(list)
        for idx in range(len(self.partitions_L)):
            if self.partitions_L[idx].valid:
                for log in self.partitions_L[idx].logLL:
                    # print(int(log[-2]) - 1, int(log[-1]) - 1)
                    absid_logid[int(log[-1]) - 1].append(int(log[-2]) - 1)  # zero-based index

        abstractions = {}
        abstraction_id = 0
        for event in self.eventsL:
            abstractions[abstraction_id] = {
                'abstraction': ' '.join(event.eventStr),
                'log_id': absid_logid[event.eventId - 1]
            }
            abstraction_id += 1

        abstractions = self.__get_final_abstraction(abstractions)
        return abstractions, self.logs

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

        parsed_logs = self.para.parsed_logs
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
    # set input path
    dataset_path = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/'
    analyzed_file = 'auth.log'
    outputfile = '/home/hudan/Git/pylogabstract/results/' + analyzed_file

    # parse logs
    utility = MiscUtility()
    parsedlogs, _ = utility.write_parsed_message(dataset_path + analyzed_file, outputfile)

    msg_path = '/home/hudan/Git/pylogabstract/results/'
    OutputPath = '/home/hudan/Git/pylogabstract/results/misc/'
    parameter = ParaIPLoM(path=msg_path, logname=analyzed_file, save_path=OutputPath, parsed_logs=parsedlogs)

    # call IPLoM and get abstractions
    myparser = IPLoM(parameter)
    time = myparser.main_process()
    abstractions_results, rawlogs = myparser.get_abstraction()
    for k, v in abstractions_results.items():
        print(k, v)

    print('Time:', time)
