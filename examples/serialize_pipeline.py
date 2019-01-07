import os
import os.path as osp
from collections import Counter

from pipelib import TextDataset
from pipelib.serializers import save_pipeline, load_pipeline


def build_vocab(tokens):
    counter = Counter(tokens)
    words, _ = zip(*counter.most_common())
    return dict(zip(words, range(len(words))))


if __name__ == '__main__':

    if not osp.exists('train.en'):
        os.system('curl -sO https://raw.githubusercontent.com/odashi/small_parallel_enja/master/train.en')
    if not osp.exists('test.en'):
        os.system('curl -sO https://raw.githubusercontent.com/odashi/small_parallel_enja/master/test.en')

    en_train = TextDataset('train.en')
    en_test = TextDataset('test.en')

    # build vocabulary
    token_to_index = build_vocab(en_train.concat(en_test).map(str.split).flat_map(lambda x: x))

    # process training data
    en_train = en_train.map(str.split).map(lambda x: [token_to_index[token] for token in x])
    print(en_train.take(3))

    # save and load pipelines applied to training data
    save_pipeline('pipeline', en_train)
    pipeline = load_pipeline('pipeline')

    # apply pipelines to test data
    en_test = en_test.apply(pipeline)
    print(en_test.take(3))
