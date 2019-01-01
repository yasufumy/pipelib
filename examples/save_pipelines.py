import os
import os.path as osp
from collections import Counter

try:
    import cloudpickle
except ModuleNotFoundError:
    print('please install cloudpickle')

from pipelib import TextDataset


def build_vocab(tokens):
    counter = Counter(tokens)
    words, _ = zip(*counter.most_common())
    return dict(zip(words, range(len(words))))


def save(target, filepath):
    with open(filepath, 'wb') as f:
        cloudpickle.dump(target, f)


def load(filepath):
    with open(filepath, 'rb') as f:
        return cloudpickle.load(f)


if __name__ == '__main__':

    if not osp.exists('train.en'):
        os.system('curl -sO https://raw.githubusercontent.com/odashi/small_parallel_enja/master/train.en')
    if not osp.exists('test.en'):
        os.system('curl -sO https://raw.githubusercontent.com/odashi/small_parallel_enja/master/test.en')

    en_train = TextDataset('train.en')
    en_test = TextDataset('test.en')

    # build vocabulary
    token_to_index = build_vocab(en_train.concatenate(en_test).map(str.split).flat_map(lambda x: x))

    # process training data
    en_train = en_train.map(str.split).map(lambda x: [token_to_index[token] for token in x])
    print(en_train.take(3))

    # save and load pipelines applied to training data
    save(en_train._func, 'pipelines')
    pipelines = load('pipelines')

    # apply pipelines to test data
    en_test = en_test.apply(pipelines)
    print(en_test.take(3))
