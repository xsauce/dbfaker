import random
from datetime import timedelta, date, datetime
import time

import faker
from sqlalchemy import *
import traceback


metadata = MetaData()

# Base = declarative_base()
# class User(Base):
#     __tablename__ = "user"
#     id = Column(Integer, primary_key=True)
#     name = Column(String(50))
#     address = Column(String(100))
#     mail = Column(String(100))
#     gender = Column(String(2))
#     birthdate = Column(Date)
#
# class Company(Base):
#     __tablename__ = "company"
#     id = Column(Integer, primary_key=True)
#     name = Column(String(50))
#     address = Column(String(100))
#     established_at = Column(Date)
#
# class Hired(Base):
#     __tablename__ = "hired"
#
#     id = Column(Integer, primary_key=True)
#     company_id = Column(Integer, ForeignKey('company.id'))
#     user_id = Column(Integer, ForeignKey('user.id'))
#     job = Column(String(50))
#     hired_at = Column(Date)

user = Table('user', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('address', String(100)),
    Column('mail', String(100)),
    Column('gender', String(2)),
    Column('birthdate', Date)
)

company = Table('company', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('address', String(100)),
    Column('established_at', Date)
)

hired = Table('hired', metadata,
    Column('id', Integer, primary_key=True),
    Column('company_id', Integer, ForeignKey('company.id')),
    Column('user_id', Integer, ForeignKey('user.id')),
    Column('job', String(50)),
    Column('hired_at', Date)
)


# def generate_sample(user_count=100, company_count=10, hired_count=200):
#     engine = create_engine("mysql+pymysql://root:root123@localhost/fake_sample?charset=utf8mb4", echo=True)
#     Base.metadata.drop_all(engine)
#     Base.metadata.create_all(engine)
#     fake = faker.Faker('zh_CN')
#     fake.seed(int(time.time()))
#     user_list = []
#     for i in range(user_count):
#         p = fake.simple_profile()
#         user_list.append(User(name=p['name'], gender=p['sex'], address=p['address'], mail=p['mail'], birthdate=fake.date_between_dates(date_start=date(1960, 1, 1), date_end=date(1995, 12, 31))))
#
#     with engine.connect() as conn:
#         try:
#             conn.execute(user.insert(), user_list)
#             conn.execute(company.insert(), [dict(name=fake.company(), address=fake.address(), established_at=fake.date()) for i in range(company_count)])
#
#             hired_list = []
#             for i in range(hired_count):
#                 tmp_company = conn.execute(select([company]).where(company.c.id == fake.random_int(1, company_count)).limit(1)).fetchone()
#                 tmp_user = conn.execute(select([user]).where(user.c.id == fake.random_int(1, user_count)).limit(1)).fetchone()
#                 hired_list.append(dict(
#                     company_id=tmp_company['id'],
#                     user_id=tmp_user['id'],
#                     job=fake.job(),
#                     hired_at=fake.date_between_dates(date_start=max(tmp_company['established_at'], tmp_user['birthdate'] + timedelta(days=365 * 22)))
#                 ))
#             conn.execute(hired.insert(), hired_list)
#         except:
#             traceback.print_exc()



def generate_sample(user_count=100, company_count=10, hired_count=200):
    engine = create_engine("mysql+pymysql://root:root123@localhost/fake_sample?charset=utf8mb4", echo=True)
    metadata.drop_all(engine)
    metadata.create_all(engine)
    fake = faker.Faker('zh_CN')
    fake.seed(random.random())
    user_list = []
    for i in range(user_count):
        p = fake.simple_profile()
        user_list.append(dict(name=p['name'], gender=p['sex'], address=p['address'], mail=p['mail'], birthdate=fake.date_between_dates(date_start=date(1960, 1, 1), date_end=date(1995, 12, 31))))

    with engine.connect() as conn:
        conn.execute(user.insert(), user_list)
        conn.execute(company.insert(), [dict(name=fake.company(), address=fake.address(), established_at=fake.date()) for i in range(company_count)])

        hired_list = []
        for i in range(hired_count):
            tmp_company = conn.execute(select([company]).where(company.c.id == fake.random_int(1, company_count)).limit(1)).fetchone()
            tmp_user = conn.execute(select([user]).where(user.c.id == fake.random_int(1, user_count)).limit(1)).fetchone()
            hired_list.append(dict(
                company_id=tmp_company['id'],
                user_id=tmp_user['id'],
                job=fake.job(),
                hired_at=fake.date_between_dates(date_start=max(tmp_company['established_at'], tmp_user['birthdate'] + timedelta(days=365 * 22)))
            ))
        conn.execute(hired.insert(), hired_list)


if __name__ == "__main__":
    st = time.time()
    user_count = 10000
    company_count = 10
    hired_count = 20000
    generate_sample(user_count, company_count, hired_count)
    et = time.time()
    print 'finished, cost:%s s' % (et - st)
