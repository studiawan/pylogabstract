from optparse import OptionParser
from pylogabstract.abstraction.abstraction import LogAbstraction
from pylogabstract.output.output import Output


def main():
    parser = OptionParser(usage='usage: pylogabstract [options]')
    parser.add_option('-i', '--input',
                      action='store',
                      dest='input_file',
                      help='Input log file.')
    parser.add_option('-o', '--output',
                      action='store',
                      dest='output_file',
                      help='Output abstraction file.')

    # get options
    (options, args) = parser.parse_args()
    input_file = options.input_file
    output_file = options.output_file

    if options.input_file:
        # get abstraction
        log_abstraction = LogAbstraction()
        abstractions, raw_logs = log_abstraction.get_abstraction(input_file)

        if options.output_file:
            print('Write results to', output_file)
            Output.write_abstraction_only(abstractions, output_file)

        else:
            print('No output file. Print results on terminal.')
            for abstraction_id, abstraction in abstractions.items():
                print('#' + str(abstraction_id) + ' ' + abstraction['abstraction'])

    else:
        print('Please see help: pylogabstract -h')


if __name__ == "__main__":
    main()
