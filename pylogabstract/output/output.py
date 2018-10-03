
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
