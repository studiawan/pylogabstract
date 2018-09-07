from optparse import OptionParser
from pylogabstract.abstraction.abstraction import AutoAbstraction


def main():
    parser = OptionParser(usage='usage: %prog [options]')
    parser.add_option('-i', '--input',
                      action='store',
                      dest='input_file',
                      default='input.log',
                      help='Input log file.')
    parser.add_option('-o', '--output',
                      action='store',
                      dest='output_file',
                      default='output.log',
                      help='Output abstraction file.')

    # get options
    (options, args) = parser.parse_args()
    input_file = options.input_file
    output_file = options.output_file

    # get abstraction
    aa = AutoAbstraction(input_file)
    aa.get_abstraction()
    aa.write_abstraction(output_file)


if __name__ == "__main__":
    main()
