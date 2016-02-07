import random
import string

def random_string(length = 20, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(length))