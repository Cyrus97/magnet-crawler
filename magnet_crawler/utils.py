import random
import string


def get_random_id(len):
    str_list = [random.choice(string.digits + string.ascii_letters) for i in range(len)]
    random_str = ''.join(str_list)
    return random_str


if __name__ == '__main__':
    print(get_random_id(4))
