from dbfaker.util import yaml_util
import json

def test():
    data = yaml_util.parse('../../example/sample.yaml')
    print json.dumps(data)
    return data

if __name__ == "__main__":
    test()