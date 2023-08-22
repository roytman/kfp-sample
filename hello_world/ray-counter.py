import argparse
import os
import time

import ray
import requests


class ParseKwargs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
        for value in values:
            key, value = value.split("=")
            getattr(namespace, self.dest)[key] = value


parser = argparse.ArgumentParser()
parser.add_argument("-k", "--kwargs", nargs="*", action=ParseKwargs)
args = parser.parse_args()

numberOfIterations = int(args.kwargs["iterations"])
print(f"Requested number of iterations is: {numberOfIterations}")

print(f'Envarionment variable MY_VARIABLE has a value of {os.getenv("MY_VARIABLE")}')

ray.init()


@ray.remote
class Counter:
    def __init__(self):
        self.counter = 0

    def inc(self):
        print("incrementing the counter")
        # sleep for 2 sec
        time.sleep(2)
        # increment the counter
        self.counter += 1

    def get_counter(self):
        print("getting the counter")
        # sleep for 2 sec
        time.sleep(2)
        return self.counter


counter = Counter.remote()

for _ in range(numberOfIterations):
    ray.get(counter.inc.remote())
    print(ray.get(counter.get_counter.remote()))

print("Requests", requests.__version__)
