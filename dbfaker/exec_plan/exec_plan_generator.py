# coding: utf-8
import os
import re

from settings import ROOT_DIR
from util.number_util import NumberUtil


class ExecPlanGenerator:
    def __init__(self, template_data, process_count):
        self.template_data = template_data
        self.process_count = process_count
        self.dependency_graph = []

    def build_engine_init(self):
        engine_data = self.template_data['engine']
        engine_type = engine_data['type']
        if engine_type == "mysql":
            return 'create_engine("mysql+{lib}://{username}:{password}@{host}:{port}/{db}?charset=utf8mb4", echo=True, pool_size=1)'.format(
                lib=engine_data.get("lib", "pymysql"),
                username=engine_data["username"],
                password=engine_data["password"],
                host=engine_data["host"],
                port=engine_data["port"],
                db=engine_data["db"]
            )
        raise Exception("no support for %s" % engine_type)

    def build_table_def(self, tbl_name, tbl_data):
        column_list = []
        for col, col_attr in tbl_data['schema'].items():
            other_col_attr = []
            col_type = "String(255)"
            col_fk = ""
            col_pk = ""
            for attr, value in col_attr.items():
                if attr == "fake":
                    continue
                elif attr == 'pk':
                    col_pk = "primary_key=%s" % value
                elif attr == "fk":
                    col_fk = "ForeignKey('%s')" % value
                elif attr == "type":
                    col_type = value
                else:
                    other_col_attr.append("%s='%s'" % (attr, value))
            new_col_attr = [col_type, col_fk, col_pk] + other_col_attr
            column_list.append("Column('%s', %s)" % (col, ', '.join([ca for ca in new_col_attr if ca])))
        return '''{tbl_name} = Table('{tbl_name}', metadata, {column_list})
'''.format(tbl_name=tbl_name, column_list=', '.join(column_list))

    def generate_plan(self, template_data):
        table_list = []
        tbl_data_gen_clause_by_level = {tbl: {"clause": [], "level": 0} for tbl in template_data["tables"].keys()}
        for tbl_name, tbl_def in template_data['tables'].items():
            table_list.append(self.build_table_def(tbl_name, tbl_def))
            data_generator = DataGenerator(tbl_name, tbl_def, self.template_data, process_count=self.process_count)
            tbl_data_gen_clause_by_level[tbl_name]["clause"] = data_generator.generate().splitlines()
            for dep_tbl in data_generator.get_dependency_relation():
                tbl_data_gen_clause_by_level[dep_tbl]["level"] += 1

        data_generate = []
        for c in sorted(tbl_data_gen_clause_by_level.values(), key=lambda x: x["level"], reverse=True):
            data_generate += c["clause"]

        template_vars = dict(
            tables_def="\n".join(table_list),
            engine_init=self.build_engine_init(),
            data_generate="\n".join(data_generate)
        )

        with open(os.path.join(ROOT_DIR, 'exec_plan', 'exec_plan.template')) as f:
            return f.read().format(**template_vars)

class DataGenerator:
    def __init__(self, tbl, tbl_def, template_data, process_count=1):
        self.tbl = tbl
        self.tbl_def = tbl_def
        self.template_data = template_data
        self.process_count = process_count
        self.dependency_tables = set()
        self.tmp_vars = []
        self.tmp_col_vars = {}

    def generate(self):
        func_clause = self.generate_func()
        total_row_cnt = self.tbl_def['row_cnt']
        if self.process_count == 1:
            return '''
{func_clause}
gen_{tbl}({row_cnt})
'''.format(row_cnt=total_row_cnt, func_clause=func_clause, tbl=self.tbl)
        else:
            return '''
{func_clause}
gen_{tbl}_pool = Pool({process_count})
rs = gen_{tbl}_pool.map(gen_{tbl}, [{row_cnt_list}])
assert sum(rs) == {process_count}
'''.format(func_clause=func_clause, tbl=self.tbl, process_count=self.process_count, row_cnt_list=', '.join([str(s) for s in NumberUtil.split_group(total_row_cnt, self.process_count)]))

    def get_dependency_relation(self):
        return self.dependency_tables

    def generate_func(self):
        column_list = []
        for col, col_attr in self.tbl_def['schema'].items():
            fk = col_attr.get('fk', '')
            if fk:
                dep_tbl = fk.split(".")[0]
                dep_tbl_col = fk.split(".")[1]
                self.dependency_tables.add(dep_tbl)
                self.tmp_vars.append(
                    'tmp_{dep_tbl} = conn.execute(select([{dep_tbl}]).where({dep_tbl}.c.{dep_tbl_col} == fake.random_int(1, {dep_tbl_row_cnt})).limit(1)).fetchone()'.format(
                        dep_tbl=dep_tbl,
                        dep_tbl_col=dep_tbl_col,
                        dep_tbl_row_cnt=self.template_data['tables'][dep_tbl]['row_cnt']
                    )
                )

        for col, col_attr in self.tbl_def['schema'].items():
            tmp_col_var = self.make_temp_var(self.tbl, col)
            if tmp_col_var:
                column_list.append('%s=%s' % (col, tmp_col_var))
            fk = col_attr.get('fk', '')
            if fk:
                dep_tbl = fk.split(".")[0]
                dep_tbl_col = fk.split(".")[1]
                column_list.append('%s=%s' % (col, "tmp_{dep_tbl}['{dep_tbl_col}']".format(dep_tbl=dep_tbl, dep_tbl_col=dep_tbl_col)))

        self.tmp_vars += [v["clause"] for v in sorted(self.tmp_col_vars.values(), key=lambda x: x["order"], reverse=True)]
        return '''
def gen_{tbl}(row_cnt):
    engine = get_engine()
    with engine.connect() as conn:
        fake = faker.Faker('{locale}')
        fake.seed(random.random())
        {tbl}_list = []
        for i in range(row_cnt):
            {tmp_vars}
            {tbl}_list.append(dict(
                {column_list}
            ))
        conn.execute({tbl}.insert(), {tbl}_list)
    return 1
        '''.format(locale=self.template_data["locale"], tbl=self.tbl, tmp_vars='\n            '.join(self.tmp_vars),
                   column_list=',\n                '.join(column_list))

    def build_fake_func(self, fake_func, col):
        fake_func_clause = "fake.%s()" % fake_func[0]
        if len(fake_func) > 1:
            params = []
            for param_key, param_value in fake_func[1].items():
                tmp_value = param_value
                for r in self.extract_ref_info(param_value):
                    r_tbl_col = r.split(".")
                    if len(r_tbl_col) == 2:
                        ref_tbl = r_tbl_col[0]
                        ref_col = r_tbl_col[1]
                        if r_tbl_col[0] not in self.dependency_tables:
                            raise Exception(
                                "table[{ref_tbl}] is referred by the fake attribute of column[col] of table[{tbl}], but there is not a foreign key between table[{ref_tbl}] and table[{tbl}]".format(
                                    ref_tbl=ref_tbl,
                                    col=col,
                                    tbl=self.tbl
                                ))
                        tmp_value = tmp_value.replace("${%s}" % r, "tmp_%s['%s']" % (ref_tbl, ref_col))
                    elif len(r_tbl_col) == 1:
                        tmp_ref_col_var = self.make_temp_var(tbl=self.tbl, col=r_tbl_col[0])
                        if not tmp_ref_col_var:
                            raise Exception("column {col} has no fake attribute to generate data".format(col=r_tbl_col[0]))
                        tmp_value = tmp_value.replace("${%s}" % r, tmp_ref_col_var)
                    else:
                        raise Exception('invalid ref:%s' % r)
                params.append('%s=%s' % (param_key, tmp_value))
            fake_func_clause = "fake.%s(%s)" % (fake_func[0], ', '.join(params))
        return fake_func_clause.encode('utf-8')

    def extract_ref_info(self, s):
        return re.findall(r'\$\{([\w\.]+)\}', s)

    def make_temp_var(self, tbl, col):
        fake_func = self.tbl_def["schema"][col].get("fake", [])
        if not fake_func:
            return ""
        tmp_col_var = "tmp_{tbl}_{col}".format(tbl=tbl, col=col)
        if tmp_col_var in self.tmp_col_vars:
            self.tmp_col_vars[tmp_col_var]["order"] += 1
        else:
            self.tmp_col_vars[tmp_col_var] = {"clause": "{tmp_col_var} = {fake_func_clause}".format(
                tmp_col_var=tmp_col_var,
                fake_func_clause=self.build_fake_func(fake_func, col)), "order": 0}
        return tmp_col_var



