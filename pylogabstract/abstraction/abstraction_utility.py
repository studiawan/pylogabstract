import json


class AbstractionUtility(object):
    @staticmethod
    def read_json(json_file):
        # read json data
        with open(json_file, 'r') as f:
            data = json.load(f)

        # change json key string to int
        converted_data = {}
        for key, value in data.items():
            converted_data[int(key)] = value

        return converted_data

    @staticmethod
    def get_abstractionid_from_groundtruth(abstraction_withid_file, abstractions):
        # read ground truth
        abstraction_withid = AbstractionUtility.read_json(abstraction_withid_file)
        groundtruth_length = len(abstraction_withid.keys())

        lineid_abstractionid = {}
        for abstraction_id, abstraction in abstractions.items():
            # if abstraction exist in ground truth, get id from dictionary key
            new_id = -1
            for groundtruth_id, groundtruth_abstraction in abstraction_withid.items():
                if abstraction['abstraction'] in groundtruth_abstraction:
                    new_id = groundtruth_id

            # if not exist, new id is dictionary length + 1
            if new_id == -1:
                new_id = groundtruth_length
                groundtruth_length += 1

            # set new abstraction id based on ground truth id
            for lineid in abstraction['log_id']:
                lineid_abstractionid[lineid] = new_id

        return lineid_abstractionid
