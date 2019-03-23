from faker.providers import BaseProvider

class CustomProvider(BaseProvider):
    lst = ['1', '2', '3']
    def pop_from_lst(self):
        return self.lst.pop()