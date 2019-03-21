from dbfaker.exec_plan.exec_plan_generator import ExecPlanGenerator
from test.util.yaml_util_test import test

if __name__ == "__main__":
    data = test()
    one_tbl = data['tables'].keys()[0]
    g = ExecPlanGenerator(data)
    print g.build_table_def(one_tbl, data['tables'][one_tbl])

