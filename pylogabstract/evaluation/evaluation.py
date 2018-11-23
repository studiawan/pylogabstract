
class Evaluation(object):
    def __init__(self, abstractionid_logids_groundtruth, abstractionid_logids_prediction,
                 lineid_abstractionid_prediction):
        self.abstractionid_logids_groundtruth = abstractionid_logids_groundtruth
        self.abstractionid_logids_prediction = abstractionid_logids_prediction
        self.lineid_abstractionid_prediction = lineid_abstractionid_prediction

    def __get_tp_fp_fn(self):
        tp, fp, fn = 0., 0., 0.
        for abstraction_id, log_ids in self.abstractionid_logids_groundtruth.items():
            log_ids_len = len(log_ids)
            if abstraction_id in self.abstractionid_logids_prediction.keys():
                # true positive
                # assigns two log messages with the [same] abstractions
                # to the [same] abstractions
                tp += log_ids_len

            else:
                log_id_check = []
                for log_id in log_ids:
                    log_id_check.append(self.lineid_abstractionid_prediction[log_id])

                check_len = len(set(log_id_check))

                # false positive
                # assigns two log messages with the [different] abstractions
                # to the [same] abstractions
                if check_len == 1:
                    fp += log_ids_len

                # false negative
                # assigns two log messages with the [same] abstractions
                # to the [different] abstractions
                elif check_len > 1:
                    fn += log_ids_len

        return tp, fp, fn

    @staticmethod
    def __get_precision_recall(tp, fp, fn):
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)

        precision = round(precision, 3)
        recall = round(recall, 3)

        return precision, recall

    def get_metrics(self):
        tp, fp, fn = self.__get_tp_fp_fn()
        precision, recall = self.__get_precision_recall(tp, fp, fn)
        accuracy = (2 * precision * recall) / (precision + recall)
        accuracy = round(accuracy, 3)

        metrics = {
            'tp': tp,
            'fp': fp,
            'fn': fn,
            'precision': precision,
            'recall': recall,
            'accuracy': accuracy
        }

        return metrics
