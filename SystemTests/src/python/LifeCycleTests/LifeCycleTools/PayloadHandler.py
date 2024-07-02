import copy
import ujson

def increase_interval(start=0.0, step=0.2):
    value = start
    while True:
        yield value
        value += step

def split_list(this_list, split_size):
    for element in range(0, len(this_list), split_size):
        yield this_list[element:element+split_size]
        
class PayloadHandler(object):
    def __init__(self):
        self._new_payload = []
        self._initial_payload = None

    def get_payload(self):
        return self._initial_payload

    def load_payload(self, filename):
        with open(filename, 'r') as f:
            self._initial_payload = ujson.decode(f.read())

    def save_payload(self, filename):
        with open(filename, 'w') as f:
            f.write(ujson.encode(self._new_payload))
            
    def clone_payload(self):
        return copy.deepcopy(self._initial_payload)

    def append_payload(self, payload):
        self._new_payload.append(payload)

    payload = property(get_payload)
