import re

from faker import Faker

if __name__ == "__main__":
    a = "${birthdate} + timedelta(days=1)"
    p = r'\$\{([\w\.]+)\}'
    print re.findall(p, a)

    b = "max(${company.established_at}, ${user.birthdate} + timedelta(days=365 * 22))"
    print re.findall(r'\$\{([\w\.]+)\}', b)

    a = "birthdate"
    print a.split(".")

    c = "dadfds"
    print re.findall(r'\$\{([\w\.]+)\}', c)

    row_cnt_list = []
    total = 1001
    every_process_row_cnt = total / 5
    tmp = total
    while tmp > every_process_row_cnt:
        tmp -= every_process_row_cnt
        row_cnt_list.append(every_process_row_cnt)
    row_cnt_list[-1] += tmp
    print row_cnt_list