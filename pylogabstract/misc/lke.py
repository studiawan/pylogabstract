"""This is the implementation of Log Key Extraction (LKE) method [Fu2009]_.

The original code of LKE is provided by Pinjia He in his GitHub account [He2016c]_.
The methods in this class has not documented and please refer to [He2016c]_
to get more complete descriptions.

References
----------
.. [Fu2009]  Fu, Qiang, Jian-Guang Lou, Yi Wang, and Jiang Li., Execution Anomaly Detection in Distributed Systems
             through Unstructured Log Analysis, International Conference on Data Mining, vol. 9, pp. 149-158. 2009.
.. [He2016c] P. He, Logparser: A python package of log parsers with benchmarks for log template extraction.
             https://github.com/cuhk-cse/logparser.
"""

from numpy import *
import math
import time
import re
import os
import sys


class ParaLKE:
    def __init__(self, path='', data_name='', logname='', removable=True,
                 remove_col=[], threshold2=5, regular=True, rex=[],
                 save_path='', save_filename=''):
        self.path = path
        self.logname = logname
        self.dataName = data_name
        self.removable = removable
        self.removeCol = remove_col
        self.threshold2 = threshold2
        self.rex = rex
        self.savePath = save_path
        self.saveFileName = save_filename
        self.regular = regular


class LKE:
    def __init__(self, para, word_ll=[], word_len=[], groups=[], logline_num_per_group=[], word_len_per_group=[],
                 word_occu_of_posi_lld=[], loglines_of_groups=[], flat_log_line_groups=[], new_groups=[]):
        self.para = para
        self.wordLL = []
        self.wordLen = []
        self.groups = []                # the list of list of words list, each group->each log lines->each words
        self.loglineNumPerGroup = []    # how many lines in each groups
        self.wordLenPerGroup = []       # maximum word positions in one group
        self.wordOccuOfPosiLLD = []     # each word in each position in each group occurance/frequency
        self.loglinesOfGroups = []
        self.flatLogLineGroups = []
        self.newGroups = []
        self.logs = []

    def para_erasing(self):
        # print('Loading log files and split into words lists...')
        # print('threshold2 is:', self.para.threshold2)
        # print(self.para.path + self.para.dataName + '/' + self.para.logname)
        # print('Processing ' + self.para.logname)
        with open(self.para.path + self.para.dataName + '/' + self.para.logname) as lines:
            for line in lines:
                self.logs.append(line)
                if self.para.regular:
                    for currentRex in self.para.rex:
                        line = re.sub(currentRex, '', line)
                    # line=re.sub(currentRex,'core.',line) # For BGL data only
                    # line=re.sub('node-[0-9]+','node-',line) #For HPC only
                word_seq = line.strip().split()
                if self.para.removable:
                    word_seq = [word for i, word in enumerate(word_seq) if i not in self.para.removeCol]
                self.wordLen.append(len(word_seq))
                self.wordLL.append(tuple(word_seq))
        # delete all the files insidd this directory
        # if not os.path.exists(self.para.savePath + self.para.dataName + '/'):
        #    os.makedirs(self.para.savePath + self.para.dataName + '/')
        # else:
        #    delete_all_files(self.para.savePath + self.para.dataName + '/')

    def clustering(self, t1):
        sys.setrecursionlimit(100000000)  # set the recursion limits number
        v = math.floor(sum(self.wordLen) / len(self.wordLen))
        # print('the parameter v is: %d' % v)
        log_num = len(self.wordLen)
        # print('there are about %d loglines' % log_num)
        load_data_time = 0
        cal_data_time = 0
        # In order to save time, load distArraydata, if exist, do not calculate the edit distance again:
        # if os.path.exists(self.para.savePath + self.para.dataName + 'editDistance.csv'):
        #     print('Loading data instead of calculating..')
        #     print self.para.savePath + self.para.dataName + 'editDistance.csv'
        #     dist_mat = genfromtxt(self.para.savePath + self.para.dataName + 'editDistance.csv', delimiter=',')
        #     dist_list = genfromtxt(self.para.savePath + self.para.dataName + 'distArray.csv', delimiter=',')
        #     load_data_time = time.time() - t1
        # else:
        #     print('calculating distance....')
        path = self.para.savePath + self.para.dataName
        dist_mat, dist_list = cal_distance(self.wordLL, v, path)
        cal_data_time = time.time() - t1
        dist_array = array(dist_list)
        threshold1 = getk_means_threshold(dist_array)
        # print('the threshold1 is: %s' % threshold1)

        # connect two loglines with distance < threshold, log_dict is a dictionary
        # where the key is line num while
        log_dict = {}
        for i in range(log_num):
            log_line_set = set()
            for j in range(i + 1, log_num):
                if dist_mat[i, j] < threshold1:
                    log_line_set.add(j)
            log_dict[i] = log_line_set

        # use DFS to get the initial group.
        flag = zeros((log_num, 1))  # used to label whether line has been visited, 0 represents not visited
        for key in log_dict:
            if flag[key] == 1:
                continue
            group_loglist = []
            group_loglist.append(key)  # add the key of dict into the list firstly, and then add others
            flag[key] = 1  # line is visited
            dfs_traversal(key, log_dict, flag, group_loglist)
            self.loglinesOfGroups.append(group_loglist)
            self.loglineNumPerGroup.append(len(group_loglist))

        # print('================get the initial groups splitting=============')
        word_len_array = array(self.wordLen)
        for row in self.loglinesOfGroups:
            # print row
            each_line_log_list = []
            self.wordLenPerGroup.append(max(word_len_array[row]))
            for colu in row:
                each_line_log_list.append(self.wordLL[colu])
            self.groups.append(each_line_log_list)
        # print('========================================================================')
        # print('there are %s groups' % (len(self.wordLenPerGroup)))
        return load_data_time, cal_data_time

    def print_each_cluster(self):
        cluster_index = 0
        for rows in self.loglinesOfGroups:
            print('Cluster #', cluster_index)
            cluster_index += 1
            for row in rows:
                print(self.logs[row].strip())
            print('========================================================================')

    def get_clusters(self):
        clusters = {}
        cluster_id = 0
        for rows in self.loglinesOfGroups:
            clusters[cluster_id] = rows
            cluster_id += 1

        return clusters

    def get_logs(self):
        return self.logs

    # split the current group recursively.
    def splitting(self):
        print ('splitting into different groups...')
        print ('the threshold2 is %d' % self.para.threshold2)
        group_num = len(self.groups)  # how many groups initially
        for i in range(group_num):
            split_each_group(self.groups[i], self.para.threshold2, self.loglinesOfGroups[i])

        # to flat the list of list of list to list of many lists, that is only one layer lists nested
        merge_lists(self.groups, self.newGroups)
        merge_lists(self.loglinesOfGroups, self.flatLogLineGroups)
        print('Merge the lists together...')
        print('there are %s different groups' % (len(self.flatLogLineGroups)))

    # extract the templates according to the logs in each group
    def extracting(self):
        templates = []
        for i in range(len(self.flatLogLineGroups)):
            group_len = len(self.flatLogLineGroups[i])
            each_group = self.newGroups[i]
            if group_len == 1:
                templates.append(each_group[0])
            else:
                common_part = lcs(each_group[0], each_group[1])
                for k in range(2, group_len):
                    if not com_exit(common_part, each_group[k]):
                        common_part = lcs(common_part, each_group[k])
                        if len(common_part) == 0:
                            print('there is no common part in this group')
                            common_part = []
                            break
                if len(common_part) != 0:
                    templates.append(common_part)
        with open(self.para.savePath + self.para.dataName + '/' + 'logTemplates.txt', 'w') as fi:
            for j in range(len(templates)):
                fi.write(' '.join(templates[j]) + '\n')

    # save to logs in groups into different template txt
    def templatetxt(self):
        for i in range(len(self.flatLogLineGroups)):
            group_len = len(self.flatLogLineGroups[i])
            num_log_of_each_group = self.flatLogLineGroups[i]
            with open(self.para.savePath + self.para.dataName + '/' + self.para.saveFileName + str(i + 1) + '.txt',
                      'w') as f:
                for j in range(group_len):
                    f.write(str(num_log_of_each_group[j] + 1) + '\n')

    def main_process(self):
        self.para_erasing()
        t1 = time.time()
        load_data_time, cal_data_time = self.clustering(t1)
        # self.print_each_cluster()
        # self.get_clusters()
        # print clusters
        self.splitting()
        self.extracting()
        time_interval = time.time() - t1
        self.templatetxt()
        # print('this process takes', time_interval)
        # print('*********************************************')
        return load_data_time, cal_data_time, time_interval


# merge the list of lists(many layer) into one list of list
def merge_lists(init_group, flat_log_line_groups):
    for i in range(len(init_group)):
        if not list_contained(init_group[i]):
            flat_log_line_groups.append(init_group[i])
        else:
            merge_lists(init_group[i], flat_log_line_groups)


# find out whether a list contained a list
def list_contained(group):
    for i in range(len(group)):
        if str(type(group[i])) == "<type 'list'>":
            return True
    return False


# for each group, According to the splittable and groupLen, decide whether to split
# it iteratively until it cannot be splitted any more
def split_each_group(each_group, threshold2, loglines_each_group):
    group_len = len(each_group)
    if group_len <= 1:
        return
    return_values = posi_to_split(each_group, threshold2)
    splittable = return_values['splittable']
    if splittable == 'yes':
        diffwords = return_values['diffWordList']
        posi = return_values['minIndex']
        con_or_para_divi = return_values['con_or_para_divi']
        print('the different words are:', diffwords)
        # each item in the diffwords corresponds to a group
        for k in range(len(diffwords)):
            newgroups = []
            newlogline_group = []
            for t in range(group_len):  # each line in a group
                if len(con_or_para_divi[t]) < posi + 1:
                    newgroups.append(each_group[t])
                    newlogline_group.append(loglines_each_group[t])
                    break
                if con_or_para_divi[t][posi] == diffwords[k]:
                    newgroups.append(each_group[t])
                    newlogline_group.append(loglines_each_group[t])

            each_group.append(newgroups)
            loglines_each_group.append(newlogline_group)
        for t in range(group_len):
            each_group.pop(0)
            loglines_each_group.pop(0)
        for i in range(len(diffwords)):
            split_each_group(each_group[i], threshold2, loglines_each_group[i])


def delete_all_files(dir_path):
    file_list = os.listdir(dir_path)
    for fileName in file_list:
        os.remove(dir_path + "/" + fileName)


# find the position that should be splitted, that is to find minimum num of different variable parts of each position
# or find the entropy
def posi_to_split(each_group, threshold2):
    group_len = len(each_group)
    word_label = []
    no_common = False
    common_part = lcs(each_group[0], each_group[1])
    splittable = 'yes'
    return_values = {}
    for i in range(2, group_len):
        if not com_exit(common_part, each_group[i]):
            common_part = lcs(common_part, each_group[i])
            if len(common_part) == 0:
                no_common = True
                print('there is no common part in this group')
                break

    for k in range(group_len):
        new_word_label = []
        for t in range(len(each_group[k])):
            if each_group[k][t] in common_part:
                new_word_label.append(1)  # 1 represent constant
            else:
                new_word_label.append(0)  # 0 represents variable
        word_label.append(new_word_label)

    con_or_para_divi = []
    part_label = []
    seq_len = []
    # connect the continuous constant words or variable words as a big part(be a sequence)
    for i in range(group_len):
        start = 0
        newcon_or_para_ll = []
        new_part_label = []
        j = 1
        end = -1
        while j < len(each_group[i]):
            if word_label[i][j - 1] != word_label[i][j]:
                end = j - 1
                newcon_or_para = []
                newcon_or_para = newappend(start, end, each_group[i], newcon_or_para)
                newcon_or_para_ll.append(newcon_or_para)
                new_part_label.append(word_label[i][end])
                start = j
            j += 1

        lastnewcon_or_para = []
        for j in range(end + 1, len(each_group[i]), 1):
            lastnewcon_or_para.append(each_group[i][j])
        newcon_or_para_ll.append(lastnewcon_or_para)
        new_part_label.append(word_label[i][end + 1])
        con_or_para_divi.append(newcon_or_para_ll)
        part_label.append(new_part_label)
        seq_len.append(len(new_part_label))

    max_len = max(seq_len)

    # convert list into tuple as list could not be the key of dict
    for i in range(group_len):
        for j in range(len(con_or_para_divi[i])):
            con_or_para_divi[i][j] = tuple(con_or_para_divi[i][j])

    # initialize the list of dict of (part: occurance)
    part_occu_ld = list()
    for t in range(max_len):  # wordLenPerGroup is the maximum number of positions in one group
        word_occu_d = {}
        part_occu_ld.append(word_occu_d)

    for j in range(group_len):  # the j-th word sequence
        for k in range(len(con_or_para_divi[j])):  # the k-th word in word sequence
            key = con_or_para_divi[j][k]
            if key not in part_occu_ld[k]:
                part_occu_ld[k][key] = 1
                continue
            part_occu_ld[k][key] += 1
    num_of_diff_parts = list()
    num_of_posi = len(part_occu_ld)
    for i in range(num_of_posi):
        num_of_diff_parts.append(len(part_occu_ld[i]))

    min_num = inf
    min_index = -1
    for i in range(num_of_posi):
        if num_of_diff_parts[i] == 1:
            continue
        if num_of_diff_parts[i] < threshold2:
            if num_of_diff_parts[i] < min_num:
                min_num = num_of_diff_parts[i]
    if min_num == inf:
        splittable = 'no'
        # no minmum that smaller than threshold2, which means all position except are parameters, no need to split
        return_values['splittable'] = 'no'
        return return_values

    index_of_min_item = []
    for i in range(num_of_posi):
        if num_of_diff_parts[i] == min_num:
            index_of_min_item.append(i)

    if len(index_of_min_item) == 1:
        min_index = index_of_min_item[0]  # minmum position

    # multiple position that has same minmum num of words
    min_entropy = inf
    if len(index_of_min_item) > 1:
        for j in range((len(index_of_min_item) - 1), -1, -1):
            entropy_j = entropy(part_occu_ld[index_of_min_item[j]], num_of_diff_parts[index_of_min_item[j]])
            if entropy_j < min_entropy:
                min_entropy = entropy_j
                min_index = j

    diff_word_list = list(part_occu_ld[min_index].keys())
    return_values['splittable'] = 'yes'
    if len(diff_word_list) == 1:
        return_values['splittable'] = 'no'
    return_values['minIndex'] = min_index
    return_values['diffWordList'] = diff_word_list
    return_values['conOrParaDivi'] = con_or_para_divi
    return return_values


def com_exit(common_part, seq):
    for i, itemI in enumerate(seq):
        if itemI not in common_part:
            return False
    return True


# find the common part of two sequences
def lcs(seq1, seq2):
    lengths = [[0 for j in range(len(seq2) + 1)] for i in range(len(seq1) + 1)]
    # row 0 and column 0 are initialized to 0 already
    for i in range(len(seq1)):
        for j in range(len(seq2)):
            if seq1[i] == seq2[j]:
                lengths[i + 1][j + 1] = lengths[i][j] + 1
            else:
                lengths[i + 1][j + 1] = max(lengths[i + 1][j], lengths[i][j + 1])
    # read the substring out from the matrix
    result = []
    len_of_seq1, len_of_seq2 = len(seq1), len(seq2)
    while len_of_seq1 != 0 and len_of_seq2 != 0:
        if lengths[len_of_seq1][len_of_seq2] == lengths[len_of_seq1 - 1][len_of_seq2]:
            len_of_seq1 -= 1
        elif lengths[len_of_seq1][len_of_seq2] == lengths[len_of_seq1][len_of_seq2 - 1]:
            len_of_seq2 -= 1
        else:
            assert seq1[len_of_seq1 - 1] == seq2[len_of_seq2 - 1]
            result.insert(0, seq1[len_of_seq1 - 1])
            len_of_seq1 -= 1
            len_of_seq2 -= 1
    return result


def newappend(start, end, word_list, newcon_or_para):
    for i in range(start, end + 1, 1):
        newcon_or_para.append(word_list[i])
    return newcon_or_para


def entropy(words_on_posit, total_num):
    entropy = 0
    for key in words_on_posit:
        temp = float(words_on_posit[key]) / total_num
        entropy -= math.log(temp) * temp
    return entropy


def dfs_traversal(key, log_dict, flag, group_loglist):
    for nodes in log_dict[key]:
        if flag[nodes] == 0:
            group_loglist.append(nodes)
            flag[nodes] = 1
            dfs_traversal(nodes, log_dict, flag, group_loglist)


# k-means where k equals 2 to divide the edit distance into two groups
def getk_means_threshold(dist_array):
    # print('kMeans calculation...')
    dist_array_size = len(dist_array)
    # random choose two centroids
    min_value = min(dist_array)
    centroids = zeros((2, 1))
    range_value = float(max(dist_array) - min_value)
    centroids[:] = random.rand(2, 1) * range_value + min_value
    max_inner_dist = zeros((2, 1))
    cluster_changed = True
    cluster_assment = zeros((dist_array_size, 1))
    while cluster_changed:
        cluster_changed = False
        for i in range(dist_array_size):
            min_index = -1
            if math.fabs(dist_array[i] - centroids[0]) < math.fabs(dist_array[i] - centroids[1]):
                min_index = 0
            else:
                min_index = 1
            if cluster_assment[i] != min_index:
                cluster_changed = True
            cluster_assment[i] = min_index
        for cent in range(2):
            indexs = where(cluster_assment == cent)[0]
            dis_in_clust = dist_array[indexs]
            max_inner_dist[cent] = min(dis_in_clust)
            centroids[cent] = mean(dis_in_clust, axis=0)
    return max(max_inner_dist)


# calculate the distance betweent each two logs and save into a matrix
def cal_distance(word_ll, v, path):
    # print('calculate distance between every two logs...')
    log_num = len(word_ll)
    dist_list = []
    dist_mat = zeros((log_num, log_num))
    for i in range(log_num):
        for j in range(i, log_num):
            dist = edit_dist_of_seq(word_ll[i], word_ll[j], v)
            dist_mat[i][j] = dist_mat[j][i] = dist
            dist_list.append(dist)
    dist_array = array(dist_list)
    savetxt(path + 'editDistance.csv', dist_mat, delimiter=',')
    savetxt(path + 'distArray.csv', dist_array, delimiter=',')
    return dist_mat, dist_array


# the edit distance of two logs
def edit_dist_of_seq(word_list1, word_list2, v):
    m = len(word_list1) + 1
    n = len(word_list2) + 1
    d = []
    t = s = 0
    for i in range(m):
        d.append([t])
        t += 1 / (math.exp(i - v) + 1)
    del d[0][0]
    for j in range(n):
        d[0].append(s)
        s += 1 / (math.exp(j - v) + 1)
    for i in range(1, m):
        for j in range(1, n):
            if word_list1[i - 1] == word_list2[j - 1]:
                d[i].insert(j, d[i - 1][j - 1])
            else:
                weight = 1.0 / (math.exp(i - 1 - v) + 1)
                minimum = min(d[i - 1][j] + weight, d[i][j - 1] + weight, d[i - 1][j - 1] + 2 * weight)
                d[i].insert(j, minimum)
    return d[-1][-1]

if __name__ == '__main__':
    # set input path
    dataset_path = '/home/hudan/Git/pylogabstract/datasets/casper-rw/logs/'
    analyzed_file = 'auth.log'
    OutputPath = '/home/hudan/Git/pylogabstract/results/misc'
    para = ParaLKE(path=dataset_path, logname=analyzed_file, save_path=OutputPath)

    # call IPLoM and get clusters
    myparser = LKE(para)
    time = myparser.main_process()
    clusters = myparser.get_clusters()
    original_logs = myparser.logs
    myparser.print_each_cluster()
