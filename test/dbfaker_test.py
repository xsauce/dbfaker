from dbfaker.dbfaker import DBFaker

dbfaker = DBFaker(template='../example/sample.yaml', process_count=5)
dbfaker.run()