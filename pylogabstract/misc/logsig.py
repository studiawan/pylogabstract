import random
import math
import time
import operator
import re
import os
import gc
from collections import defaultdict
from pylogabstract.misc.misc_utility import MiscUtility


class ParaLogSig:
    def __init__(self, path='', logname='', removable=True, removeCol=[],
                 regular=True,
                 rex=[], savePath='',
                 saveFileName='template', groupNum=14, parsed_logs=None):  # line 66,change the regular expression replacement code
        self.path = path
        self.logname = logname
        self.removable = removable
        self.removeCol = removeCol
        self.regular = regular
        self.rex = rex
        self.savePath = savePath
        self.saveFileName = saveFileName
        self.groupNum = groupNum  # partition into k groups
        self.parsed_logs = parsed_logs


class LogSig:
    def __init__(self, para, wordLL=[], loglineNum=0, termpairLLT=[], logNumPerGroup=[], groupIndex=dict(),
                 termPairLogNumLD=[], logIndexPerGroup=[]):
        self.para = para
        self.wordLL = []
        self.loglineNum = 0
        self.termpairLLT = []
        self.logNumPerGroup = []
        self.groupIndex = dict()  # each line corresponding to which group
        self.termPairLogNumLD = []
        self.logIndexPerGroup = []
        self.logs = {}
        self.abstractions = {}

    # Load datasets and use regular expression to split it and remove some columns
    def termpairGene(self):
        # print('Loading Log File...')
        # print(self.para.path + self.para.logname)
        # print(self.para.regular)
        # print(self.para.groupNum)
        line_index = 0
        with open(self.para.path + self.para.logname) as lines:
            for line in lines:
                if line not in ['\n', '\r\n']:
                    self.logs[line_index] = line
                    line_index += 1

                    if self.para.regular:
                        for currentRex in self.para.rex:
                            line = re.sub(currentRex, '', line)
                            # line=re.sub(currentRex,'core.',line) # For BGL data only
                            # line=re.sub('node-[0-9]+','node-',line) #For HPC only
                    wordSeq = line.strip().split()
                    if self.para.removable:
                        wordSeq = [word for i, word in enumerate(wordSeq) if i not in self.para.removeCol]
                    self.wordLL.append(tuple(wordSeq))

    # initialize different variables
    def initialization(self):
        # print('Generating term pairs...')
        i = 0
        for wordL in self.wordLL:
            wordLT = []
            for j in range(len(wordL)):
                for k in range(j + 1, len(wordL), 1):
                    termpair = (wordL[j], wordL[k])
                    wordLT.append(termpair)
            self.termpairLLT.append(wordLT)
            i += 1

        # print('initializing...')
        # termPairLogNumLD, used to account the occurance of each termpair of each group
        for i in range(self.para.groupNum):
            newDict = dict()
            self.termPairLogNumLD.append(newDict)
            # initialize the item value to zero
            self.logNumPerGroup.append(0)

        # divide logs into initial groupNum groups randomly, the group number of each log is stored in the groupIndex
        self.loglineNum = len(self.wordLL)
        for i in range(self.loglineNum):
            ran = random.randint(0, self.para.groupNum - 1)  # group number from 0 to k-1
            self.groupIndex[i] = ran
            self.logNumPerGroup[ran] += 1  # count the number of loglines per group

        # count the frequency of each termpairs per group
        i = 0
        for termpairLT in self.termpairLLT:
            j = 0
            for key in termpairLT:
                currGroupIndex = self.groupIndex[i]
                if key not in self.termPairLogNumLD[currGroupIndex]:
                    self.termPairLogNumLD[currGroupIndex][key] = 1
                else:
                    self.termPairLogNumLD[currGroupIndex][key] += 1
                j += 1
            i += 1
        # print('=======initial group division(Random Select)=====================')
        # print('Log Number of each group is: ', self.logNumPerGroup)

    # use local search, for each log, find the group that it should be moved to.
    # in this process, termpairs occurange should also make some changes and logNumber of corresponding should be changed
    def LogMessParti(self):
        changed = True
        while changed:
            changed = False
            i = 0
            for termpairLT in self.termpairLLT:
                curGroup = self.groupIndex[i]
                alterGroup = potenFunc(curGroup, self.termPairLogNumLD, self.logNumPerGroup, i, termpairLT,
                                       self.para.groupNum)
                if curGroup != alterGroup:
                    changed = True
                    self.groupIndex[i] = alterGroup
                    # update the dictionary of each group
                    for key in termpairLT:
                        # minus 1 from the current group count on this key
                        self.termPairLogNumLD[curGroup][key] -= 1
                        if self.termPairLogNumLD[curGroup][key] == 0:
                            del self.termPairLogNumLD[curGroup][key]
                        # add 1 to the alter group
                        if key not in self.termPairLogNumLD[alterGroup]:
                            self.termPairLogNumLD[alterGroup][key] = 1
                        else:
                            self.termPairLogNumLD[alterGroup][key] += 1
                    self.logNumPerGroup[curGroup] -= 1
                    self.logNumPerGroup[alterGroup] += 1
                i += 1
        # print('===================================================')
        # print(self.logNumPerGroup)
        # print self.groupIndex
        # print('===================================================')

    # calculate the occurancy of each word of each group, and for each group, save the words that
    # happen more than half all log number to be candidateTerms(list of dict, words:frequency),
    def signatConstr(self):
        abstractions = {}
        abstraction_id = 0

        # create the folder to save the resulted templates
        # if not os.path.exists(self.para.savePath):
        #     os.makedirs(self.para.savePath)
        # else:
        #     deleteAllFiles(self.para.savePath)

        wordFreqPerGroup = []
        candidateTerm = []
        candidateSeq = []
        signature = []

        # save the all the log indexs of each group: logIndexPerGroup
        for t in range(self.para.groupNum):
            dic = dict()
            newlogIndex = []
            newCandidate = dict()
            wordFreqPerGroup.append(dic)
            self.logIndexPerGroup.append(newlogIndex)
            candidateSeq.append(newCandidate)

        # count the occurence of each word of each log per group
        # and save into the wordFreqPerGroup, which is a list of dictionary,
        # where each dictionary represents a group, key is the word, value is the occurence
        lineNo = 0
        for wordL in self.wordLL:
            groupIndex = self.groupIndex[lineNo]
            self.logIndexPerGroup[groupIndex].append(lineNo)
            for key in wordL:
                if key not in wordFreqPerGroup[groupIndex]:
                    wordFreqPerGroup[groupIndex][key] = 1
                else:
                    wordFreqPerGroup[groupIndex][key] += 1
            lineNo += 1

        # calculate the halfLogNum and select those words whose occurence is larger than halfLogNum
        # as constant part and save into candidateTerm
        for i in range(self.para.groupNum):
            halfLogNum = math.ceil(self.logNumPerGroup[i] / 2.0)
            dic = dict((k, v) for k, v in wordFreqPerGroup[i].items() if v >= halfLogNum)
            candidateTerm.append(dic)

        # scan each logline's each word that also is a part of candidateTerm, put these words together
        # as a new candidate sequence, thus, each raw log will have a corresponding candidate sequence
        # and count the occurence of these candidate sequence of each group and select the most frequent
        # candidate sequence as the signature, i.e. the templates
        lineNo = 0
        for wordL in self.wordLL:
            curGroup = self.groupIndex[lineNo]
            newCandiSeq = []

            for key in wordL:
                if key in candidateTerm[curGroup]:
                    newCandiSeq.append(key)

            keySeq = tuple(newCandiSeq)
            if keySeq not in candidateSeq[curGroup]:
                candidateSeq[curGroup][keySeq] = 1
            else:
                candidateSeq[curGroup][keySeq] += 1
            lineNo += 1

        for i in range(self.para.groupNum):
            sig = max(candidateSeq[i].items(), key=operator.itemgetter(1))[0]
            # sig=max(candidateSeq[i].iteritems(), key=operator.itemgetter(1))[0]
            signature.append(sig)

            # save abstractions == signatures
            abstractions[abstraction_id] = {
                'abstraction': ' '.join(sig),
                'log_id': self.logIndexPerGroup[i]
            }
            abstraction_id += 1

        # save the templates
        # with open(self.para.savePath + 'logTemplates.txt', 'w') as fi:
        #     for j in range(len(signature)):
        #         # pjhe
        #         fi.write(' '.join(signature[j]) + '\n')

        abstractions = self.__get_final_abstraction(abstractions)
        return abstractions

    # save the grouped loglines into different templates.txt
    def templatetxt(self):
        for i in range(len(self.logIndexPerGroup)):
            numLogOfEachGroup = self.logIndexPerGroup[i]
            with open(self.para.savePath + self.para.saveFileName + str(i + 1) + '.txt', 'w') as f:
                for log_ID in numLogOfEachGroup:
                    print(log_ID, self.logs[log_ID].rstrip())
                    f.write(str(log_ID + 1) + '\n')
                print('---')

    def mainProcess(self):
        self.termpairGene()
        t1 = time.time()
        self.initialization()
        self.LogMessParti()
        self.abstractions = self.signatConstr()
        timeInterval = time.time() - t1
        # self.templatetxt()
        # print('this process takes', timeInterval)
        # print('*********************************************')
        gc.collect()
        return timeInterval

        # calculate the potential value that would be used in the local search

    def get_clusters(self):
        # get clusters
        clusters = {}
        for log_id, cluster_id in self.groupIndex.items():
            if cluster_id not in clusters:
                clusters[cluster_id] = [log_id]
            else:
                clusters[cluster_id].append(log_id)

        return clusters

    def get_abstractions(self):
        return self.abstractions, self.logs

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


def potenFunc(curGroupIndex, termPairLogNumLD, logNumPerGroup, lineNum, termpairLT, k):
    maxDeltaD = 0
    maxJ = curGroupIndex
    for i in range(k):
        returnedDeltaD = getDeltaD(logNumPerGroup, termPairLogNumLD, curGroupIndex, i, lineNum, termpairLT)
        if returnedDeltaD > maxDeltaD:
            maxDeltaD = returnedDeltaD
            maxJ = i
    return maxJ


# part of the potential function
def getDeltaD(logNumPerGroup, termPairLogNumLD, groupI, groupJ, lineNum, termpairLT):
    deltaD = 0
    Ci = logNumPerGroup[groupI]
    Cj = logNumPerGroup[groupJ]
    for r in termpairLT:
        if r in termPairLogNumLD[groupJ]:
            deltaD += (
            pow(((termPairLogNumLD[groupJ][r] + 1) / (Cj + 1.0)), 2) - pow((termPairLogNumLD[groupI][r] / (Ci + 0.0)),
                                                                           2))
        else:
            deltaD += (pow((1 / (Cj + 1.0)), 2) - pow((termPairLogNumLD[groupI][r] / (Ci + 0.0)), 2))
    deltaD = deltaD * 3
    return deltaD


# delete the files under this dirPath
def deleteAllFiles(dirPath):
    fileList = os.listdir(dirPath)
    for fileName in fileList:
        os.remove(dirPath + "/" + fileName)


if __name__ == '__main__':
    # set input path
    dataset_path = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/'
    analyzed_file = 'auth.log'
    outputfile = '/home/hudan/Git/pylogabstract/results/' + analyzed_file

    # parse logs
    utility = MiscUtility()
    parsedlogs = utility.write_parsed_message(dataset_path + analyzed_file, outputfile)

    # get cluster number
    groundtruth_file = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs-abstraction_withid/' + analyzed_file
    cluster_number = utility.get_cluster_number(groundtruth_file)

    # call LogSig and get clusters
    para = ParaLogSig(path=dataset_path, logname=analyzed_file, groupNum=cluster_number, parsed_logs=parsedlogs)
    myparser = LogSig(para)
    time = myparser.mainProcess()
    abstractions_result, rawlogs = myparser.get_abstractions()

    for k, v in abstractions_result.items():
        print(k, v)
