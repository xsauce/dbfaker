import argparse
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
from exec_plan.exec_plan_generator import ExecPlanGenerator
from util import yaml_util
import json

class DBFaker:
    def __init__(self, **kwargs):
        self.template = kwargs['template']
        self.process_count = kwargs['process_count']

    def run(self):
        # parse template file
        template_data = self.parse_template(self.template)
        print '###[STEP 1] parse template file=%s, template_data=%s' % (self.template, json.dumps(template_data))

        template_path = os.path.split(self.template)[0]
        template_name = os.path.split(self.template)[1].split('.')[0]
        output_path = os.path.join(template_path, template_name + '.py')
        generator = ExecPlanGenerator(template_data, self.process_count)

        # generate python file
        with open(output_path, 'w') as f:
            f.write(generator.generate_plan(template_data))

        print '###[STEP 2] generate execute python file=%s' % output_path

        # run python file
        st = time.time()
        os.system("python %s" % output_path)
        et = time.time()
        print '###[STEP 3] finish to fake data, cost=%s s' % int(et - st)

    def parse_template(self, template):
        return yaml_util.parse(template)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-t', '--template', type=str, required=True, help='table template')
    parser.add_argument('-p', '--process_count', type=int, required=True, help='process count')
    args = parser.parse_args()

    args_dict = args.__dict__
    print '##### args #####'
    for k in args_dict:
        print k, '=', args_dict[k]
    print '################'

    dbfaker = DBFaker(**args_dict)
    dbfaker.run()

