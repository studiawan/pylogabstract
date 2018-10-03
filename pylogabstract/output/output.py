
class WriteText(object):
    @staticmethod
    def write_perabstraction(final_abstraction, log_file, perabstraction_file):
        # read log file
        with open(log_file, 'r') as f:
            logs = f.readlines()

        # write logs per abstraction to file
        f_perabstraction = open(perabstraction_file, 'w')
        for abstraction_id, abstraction in final_abstraction.items():
            f_perabstraction.write('Abstraction #' + str(abstraction_id) + ' ' + abstraction['abstraction'] + '\n')
            for line_id in abstraction['original_id']:
                f_perabstraction.write(str(line_id) + ' ' + logs[line_id])
            f_perabstraction.write('\n')
        f_perabstraction.close()

    @staticmethod
    def write_perline(final_abstraction, log_file, perline_file):
        # read log file
        with open(log_file, 'r') as f:
            logs = f.readlines()

        # get line id and abstraction id
        abstraction_label = {}
        for abstraction_id, abstraction in final_abstraction.items():
            for line_id in abstraction['original_id']:
                abstraction_label[line_id] = abstraction_id

        # write log per line with abstraction id
        f_perline = open(perline_file, 'w')
        for line_id, log in enumerate(logs):
            f_perline.write(str(abstraction_label[line_id]) + '; ' + log)
        f_perline.close()
