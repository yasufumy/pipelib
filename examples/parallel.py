import os
import time


def slow_map(x):
    print(f'[{os.getpid()}]: I\'m slow map func...')
    time.sleep(0.5)
    return x


def slow_filter(x):
    print(f'[{os.getpid()}]: I\'m slow filter func...')
    time.sleep(0.5)
    return True


def slow_flat_map(x):
    print(f'[{os.getpid()}]: I\'m slow map func...')
    time.sleep(0.5)
    return [x]


if __name__ == '__main__':
    from pipelib import Dataset
    data = Dataset(range(100))

    start = time.time()
    data.map(slow_map).filter(slow_filter).flat_map(slow_flat_map).all()
    normal_time = time.time() - start

    start = time.time()
    data.map_parallel(slow_map) \
        .filter_parallel(slow_filter) \
        .flat_map_parallel(slow_flat_map).all()
    faster_time = time.time() - start

    print(normal_time, faster_time)
