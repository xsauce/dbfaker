# coding: utf-8
from datetime import timedelta, date, datetime
import faker
from sqlalchemy import *
import random
from multiprocessing import Pool

metadata = MetaData()

company = Table('company', metadata, Column('established_at', Date), Column('id', Integer, primary_key=True), Column('name', String(255)), Column('address', String(255)))

hired = Table('hired', metadata, Column('job', String(255)), Column('user_id', Integer, ForeignKey('user.id')), Column('id', Integer, primary_key=True), Column('hired_at', Date), Column('company_id', Integer, ForeignKey('company.id')))

user = Table('user', metadata, Column('name', String(255)), Column('mail', String(255)), Column('gender', String(255)), Column('created_at', Date,), Column('id', Integer, primary_key=True), Column('birthdate', Date,))


def get_engine():
    return create_engine("mysql+pymysql://root:root123@localhost:3306/fake_sample?charset=utf8mb4", echo=True, pool_size=1)
engine = get_engine()
metadata.drop_all(engine)
metadata.create_all(engine)



def gen_company(row_cnt):
    engine = get_engine()
    with engine.connect() as conn:
        fake = faker.Faker('zh_CN')
        fake.seed(random.random())
        company_list = []
        for i in range(row_cnt):
            tmp_company_established_at = fake.date_between_dates()
            tmp_company_name = fake.company()
            tmp_company_address = fake.address()
            company_list.append(dict(
                established_at=tmp_company_established_at,
                name=tmp_company_name,
                address=tmp_company_address
            ))
        conn.execute(company.insert(), company_list)
    return 1
        
gen_company_pool = Pool(10)
rs = gen_company_pool.map(gen_company, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
assert sum(rs) == 10


def gen_user(row_cnt):
    engine = get_engine()
    with engine.connect() as conn:
        fake = faker.Faker('zh_CN')
        fake.seed(random.random())
        user_list = []
        for i in range(row_cnt):
            tmp_user_birthdate = fake.date_between_dates(date_end=date(1995, 12, 31), date_start=date(1960, 1, 1))
            tmp_user_created_at = fake.date_between_dates(date_start=tmp_user_birthdate + timedelta(days=1))
            tmp_user_name = fake.name()
            tmp_user_mail = fake.email()
            tmp_user_gender = fake.random_element(elements=('男', '女'))
            user_list.append(dict(
                name=tmp_user_name,
                mail=tmp_user_mail,
                gender=tmp_user_gender,
                created_at=tmp_user_created_at,
                birthdate=tmp_user_birthdate
            ))
        conn.execute(user.insert(), user_list)
    return 1
        
gen_user_pool = Pool(10)
rs = gen_user_pool.map(gen_user, [10, 10, 10, 10, 10, 10, 10, 10, 10, 10])
assert sum(rs) == 10


def gen_hired(row_cnt):
    engine = get_engine()
    with engine.connect() as conn:
        fake = faker.Faker('zh_CN')
        fake.seed(random.random())
        hired_list = []
        for i in range(row_cnt):
            tmp_user = conn.execute(select([user]).where(user.c.id == fake.random_int(1, 100)).limit(1)).fetchone()
            tmp_company = conn.execute(select([company]).where(company.c.id == fake.random_int(1, 10)).limit(1)).fetchone()
            tmp_hired_hired_at = fake.date_between_dates(date_start=max(tmp_company['established_at'], tmp_user['birthdate'] + timedelta(days=365 * 22)))
            tmp_hired_job = fake.job()
            hired_list.append(dict(
                job=tmp_hired_job,
                user_id=tmp_user['id'],
                hired_at=tmp_hired_hired_at,
                company_id=tmp_company['id']
            ))
        conn.execute(hired.insert(), hired_list)
    return 1
        
gen_hired_pool = Pool(10)
rs = gen_hired_pool.map(gen_hired, [20, 20, 20, 20, 20, 20, 20, 20, 20, 20])
assert sum(rs) == 10

