from faker import Faker

if __name__ == "__main__":

    fake = Faker('zh_CN')
    fake.seed(0)
    for i in range(2):
        print fake.name()
    fake.seed(1)
    for i in range(1):
        print fake.name()

    # print fake.address()