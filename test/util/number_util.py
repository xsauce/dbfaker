from dbfaker.util.number_util import NumberUtil


def test():
    print NumberUtil.split_group(1001, 5)
    print NumberUtil.split_group(1000, 5)
    print NumberUtil.split_group(1999, 5)

if __name__ == "__main__":
    test()