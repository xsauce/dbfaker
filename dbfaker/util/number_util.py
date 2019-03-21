class NumberUtil:
    @classmethod
    def split_group(cls, total, group_cnt):
        row_cnt_list = []
        every_process_row_cnt = total / group_cnt
        if every_process_row_cnt == 0:
            return [total]
        tmp = total
        while tmp >= every_process_row_cnt:
            tmp -= every_process_row_cnt
            row_cnt_list.append(every_process_row_cnt)
        row_cnt_list[-1] += tmp
        return row_cnt_list