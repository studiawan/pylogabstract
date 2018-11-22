# Drain v1 by Pinjia He.
# https://github.com/logpai/logparser/blob/master/logparser/DrainV1.py

import re
import os
import time
import gc
from collections import defaultdict
from pylogabstract.misc.misc_utility import MiscUtility


class Logcluster:
    def __init__(self, logTemplate='', logIDL=None):
        self.logTemplate = logTemplate
        if logIDL is None:
            logIDL = []
        self.logIDL = logIDL


class Node:
    def __init__(self, childD=None, depth=0, digitOrtoken=None):
        if childD is None:
            childD = dict()
        self.childD = childD
        self.depth = depth
        self.digitOrtoken = digitOrtoken


"""
rex: regular expressions used in preprocessing (step1)
path: the input path stores the input log file name
depth: depth of all leaf nodes
st: similarity threshold
maxChild: max number of children of an internal node
logName:the name of the input file containing raw log messages
removable: whether to remove a few columns or not
removeCol: the index of column needed to remove
savePath: the output path stores the file containing structured logs
saveFileName: the output file name prefix for all log groups
saveTempFileName: the output template file name
"""


class ParaDrain:
    def __init__(self, rex=None, path='', depth=4, st=0.4, maxChild=100, logName='', removable=True,
                 removeCol=[], savePath='', saveFileName='template', saveTempFileName='logTemplates.txt',
                 parsed_logs=None):
        self.path = path
        self.depth = depth - 2
        self.st = st
        self.maxChild = maxChild
        self.logName = logName
        self.removable = removable
        self.removeCol = removeCol
        self.savePath = savePath
        self.saveFileName = saveFileName
        self.saveTempFileName = saveTempFileName
        if rex is None:
            rex = []
        self.rex = rex
        self.parsed_logs = parsed_logs


class Drain:
    def __init__(self, para):
        self.para = para
        self.abstractions = {}
        self.raw_logs = {}

    def hasNumbers(self, s):
        return any(char.isdigit() for char in s)

    def treeSearch(self, rn, seq):
        retLogClust = None

        seqLen = len(seq)
        if seqLen not in rn.childD:
            return retLogClust

        parentn = rn.childD[seqLen]

        currentDepth = 1
        for token in seq:
            if currentDepth >= self.para.depth or currentDepth > seqLen:
                break

            if token in parentn.childD:
                parentn = parentn.childD[token]
            elif '*' in parentn.childD:
                parentn = parentn.childD['*']
            else:
                return retLogClust
            currentDepth += 1

        logClustL = parentn.childD

        retLogClust = self.FastMatch(logClustL, seq)

        return retLogClust

    def addSeqToPrefixTree(self, rn, logClust):
        seqLen = len(logClust.logTemplate)
        if seqLen not in rn.childD:
            firtLayerNode = Node(depth=1, digitOrtoken=seqLen)
            rn.childD[seqLen] = firtLayerNode
        else:
            firtLayerNode = rn.childD[seqLen]

        parentn = firtLayerNode

        currentDepth = 1
        for token in logClust.logTemplate:

            # Add current log cluster to the leaf node
            if currentDepth >= self.para.depth or currentDepth > seqLen:
                if len(parentn.childD) == 0:
                    parentn.childD = [logClust]
                else:
                    parentn.childD.append(logClust)
                break

            # If token not matched in this layer of existing tree.
            if token not in parentn.childD:
                if not self.hasNumbers(token):
                    if '*' in parentn.childD:
                        if len(parentn.childD) < self.para.maxChild:
                            newNode = Node(depth=currentDepth + 1, digitOrtoken=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD['*']
                    else:
                        if len(parentn.childD) + 1 < self.para.maxChild:
                            newNode = Node(depth=currentDepth + 1, digitOrtoken=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        elif len(parentn.childD) + 1 == self.para.maxChild:
                            newNode = Node(depth=currentDepth + 1, digitOrtoken='*')
                            parentn.childD['*'] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD['*']

                else:
                    if '*' not in parentn.childD:
                        newNode = Node(depth=currentDepth + 1, digitOrtoken='*')
                        parentn.childD['*'] = newNode
                        parentn = newNode
                    else:
                        parentn = parentn.childD['*']

            # If the token is matched
            else:
                parentn = parentn.childD[token]

            currentDepth += 1

    # seq1 is template
    def SeqDist(self, seq1, seq2):
        assert len(seq1) == len(seq2)
        simTokens = 0
        numOfPar = 0

        for token1, token2 in zip(seq1, seq2):
            if token1 == '*':
                numOfPar += 1
                continue
            if token1 == token2:
                simTokens += 1

        retVal = float(simTokens) / len(seq1)

        return retVal, numOfPar

    def FastMatch(self, logClustL, seq):
        retLogClust = None

        maxSim = -1
        maxNumOfPara = -1
        maxClust = None

        for logClust in logClustL:
            curSim, curNumOfPara = self.SeqDist(logClust.logTemplate, seq)
            if curSim > maxSim or (curSim == maxSim and curNumOfPara > maxNumOfPara):
                maxSim = curSim
                maxNumOfPara = curNumOfPara
                maxClust = logClust

        if maxSim >= self.para.st:
            retLogClust = maxClust

        return retLogClust

    def getTemplate(self, seq1, seq2):
        assert len(seq1) == len(seq2)
        retVal = []

        i = 0
        for word in seq1:
            if word == seq2[i]:
                retVal.append(word)
            else:
                retVal.append('*')

            i += 1

        return retVal

    def outputResult(self, logClustL):
        # writeTemplate = open(self.para.savePath + self.para.saveTempFileName, 'w')

        idx = 1
        for logClust in logClustL:
            # writeTemplate.write(' '.join(logClust.logTemplate) + '\n')
            # writeID = open(self.para.savePath + self.para.saveFileName + str(idx) + '.txt', 'w')
            # for logID in logClust.logIDL:
            #     writeID.write(str(logID) + '\n')
            # writeID.close()

            self.abstractions[idx-1] = {
                'abstraction': ' '.join(logClust.logTemplate),
                'log_id': logClust.logIDL
            }

            idx += 1

        # writeTemplate.close()

    def printTree(self, node, dep):
        pStr = ''
        for i in range(dep):
            pStr += '\t'

        if node.depth == 0:
            pStr += 'Root Node'
        elif node.depth == 1:
            pStr += '<' + str(node.digitOrtoken) + '>'
        else:
            pStr += node.digitOrtoken

        print(pStr)

        if node.depth == self.para.depth:
            return 1
        for child in node.childD:
            self.printTree(node.childD[child], dep + 1)

    def deleteAllFiles(self, dirPath):
        fileList = os.listdir(dirPath)
        for fileName in fileList:
            os.remove(dirPath + fileName)

    def mainProcess(self):

        t1 = time.time()
        rootNode = Node()
        logCluL = []
        line_index = 0

        with open(self.para.path + self.para.logName) as lines:
            count = 0
            for line in lines:
                if line not in ['\n', '\r\n']:
                    self.raw_logs[line_index] = line
                    # logID = int(line.split('\t')[0])
                    logID = line_index
                    # logmessageL = line.strip().split('\t')[1].split()
                    logmessageL = line.strip().split()
                    logmessageL = [word for i, word in enumerate(logmessageL) if i not in self.para.removeCol]

                    cookedLine = ' '.join(logmessageL)

                    for currentRex in self.para.rex:
                        cookedLine = re.sub(currentRex, '', cookedLine)
                    # cookedLine = re.sub(currentRex, 'core', cookedLine) #For BGL only

                    # cookedLine = re.sub('node-[0-9]+','node-',cookedLine) #For HPC only

                    logmessageL = cookedLine.split()

                    matchCluster = self.treeSearch(rootNode, logmessageL)

                    # Match no existing log cluster
                    if matchCluster is None:
                        newCluster = Logcluster(logTemplate=logmessageL, logIDL=[logID])
                        logCluL.append(newCluster)
                        self.addSeqToPrefixTree(rootNode, newCluster)

                    # Add the new log message to the existing cluster
                    else:
                        newTemplate = self.getTemplate(logmessageL, matchCluster.logTemplate)
                        matchCluster.logIDL.append(logID)
                        if ' '.join(newTemplate) != ' '.join(matchCluster.logTemplate):
                            matchCluster.logTemplate = newTemplate

                    count += 1
                    if count % 5000 == 0:
                        print(count)

                    line_index += 1

        # if not os.path.exists(self.para.savePath):
        #     os.makedirs(self.para.savePath)
        # else:
        #     self.deleteAllFiles(self.para.savePath)

        self.outputResult(logCluL)
        t2 = time.time()

        print('this process takes', t2 - t1)
        print('*********************************************')
        gc.collect()
        return t2 - t1

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
    path = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/'
    analyzed_file = 'auth.log'
    outputfile = '/home/hudan/Git/pylogabstract/results/' + analyzed_file

    # parse logs
    utility = MiscUtility()
    parsedlogs, _ = utility.write_parsed_message(path + analyzed_file, outputfile)

    # run Drain method
    msg_path = '/home/hudan/Git/pylogabstract/results/'
    removeCol = []
    st = 0.5
    depth = 4
    parserPara = ParaDrain(path=msg_path, st=st, logName=analyzed_file, removeCol=removeCol, depth=depth,
                           parsed_logs=parsedlogs)
    myParser = Drain(parserPara)
    myParser.mainProcess()
    abstraction_results, rawlogs = myParser.get_abstractions()

    for k, v in abstraction_results.items():
        print(k, v)
