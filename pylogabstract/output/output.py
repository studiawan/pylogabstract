from pylogabstract.abstraction.abstraction_utility import AbstractionUtility


class Output(object):
    @staticmethod
    def write_perline(abstractions, raw_logs, perline_file):
        # get line id and abstraction id
        abstraction_label = {}
        for abstraction_id, abstraction in abstractions.items():
            for line_id in abstraction['log_id']:
                abstraction_label[line_id] = abstraction_id

        # write log per line with abstraction id
        f_perline = open(perline_file, 'w')
        for line_id, log in raw_logs.items():
            f_perline.write(str(abstraction_label[line_id]) + '; ' + log)
        f_perline.close()

    @staticmethod
    def write_perabstraction(abstractions, raw_logs, perabstraction_file):
        # write logs per abstraction to file
        f_perabstraction = open(perabstraction_file, 'w')
        for abstraction_id, abstraction in abstractions.items():
            f_perabstraction.write('Abstraction #' + str(abstraction_id) + ' ' + abstraction['abstraction'] + '\n')
            for line_id in abstraction['log_id']:
                f_perabstraction.write(str(line_id) + ' ' + raw_logs[line_id])
            f_perabstraction.write('\n')
        f_perabstraction.close()

    @staticmethod
    def write_comparison(abstraction_withid_file, abstractions_groundtruth_file, abstractions_prediction,
                         raw_logs, comparison_file):
        # abstraction_withid_file       = list of ground truth abstraction strings
        # abstractions_groundtruth_file = ground truth
        # abstractions_prediction       = results from proposed method
        abstraction_withid = AbstractionUtility.read_json(abstraction_withid_file)
        abstractions_groundtruth = AbstractionUtility.read_json(abstractions_groundtruth_file)

        f_comparison = open(comparison_file, 'w')
        nomatch_ids = []
        for abstraction_id, abstraction in abstractions_prediction.items():
            new_id = -1
            for groundtruth_id, groundtruth_abstraction in abstraction_withid.items():
                if abstraction['abstraction'] in groundtruth_abstraction:
                    new_id = groundtruth_id

            # if id exist, write abstractions side by side
            if new_id != -1:
                # write ground truth
                f_comparison.write('\nGround truth: ' + abstraction_withid[new_id] + '\n')
                for line_id, abs_id in abstractions_groundtruth.items():
                    if abs_id == new_id:
                        f_comparison.write(str(line_id) + ' ' + raw_logs[line_id])

                # write prediction
                f_comparison.write('\nPrediction  : ' + abstraction['abstraction'] + '\n')
                for line_id in abstraction['log_id']:
                    f_comparison.write(str(line_id) + ' ' + raw_logs[line_id])

                dash = '-'
                for _ in range(len(abstraction_withid[new_id])):
                    dash += '-'
                f_comparison.write(dash + '\n')

            # if id not exist, write abstractions later
            else:
                nomatch_ids.append(abstraction_id)

        # write no match abstractions
        for nomatch_id in nomatch_ids:
            f_comparison.write('\nNo match prediction: ' + abstractions_prediction[nomatch_id]['abstraction'] + '\n')
            for line_id in abstractions_prediction[nomatch_id]['log_id']:
                f_comparison.write(str(line_id) + ' ' + raw_logs[line_id])

        f_comparison.close()
