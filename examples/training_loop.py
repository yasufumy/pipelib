import os
import os.path as osp
import time
from collections import Counter

from pipelib import TextDataset


def batch_transpose(batch):
    if isinstance(batch[0], tuple):
        batch = [x for x in zip(*batch)]
    return batch


def build_vocab(tokens):
    counter = Counter(tokens)
    words, _ = zip(*counter.most_common())
    token_to_index = dict(zip(words, range(len(words))))
    return token_to_index


def prepare_data(source_file, cache_file):
    if not osp.exists(cache_file):
        print('processing')
        # tokenize
        data = TextDataset(source_file).map(lambda x: x.split())
        # vocabulary
        vocab = build_vocab(data.flat_map(lambda x: x))
        # convert and save
        data = data.map(lambda x: [vocab[token] for token in x]).save(cache_file)
    else:
        print('loading')
        # load
        data = TextDataset.load(cache_file)

    return data


def do_something(batch):
    time.sleep(0.1)
    assert len(batch[0]) == len(batch[1])
    print(f'en batch: {len(batch[0])}', f'ja batch: {len(batch[1])}')


if __name__ == '__main__':

    # data preparation
    if not osp.exists('train.en'):
        os.system('curl -sO https://raw.githubusercontent.com/odashi/small_parallel_enja/master/test.en')
    if not osp.exists('train.ja'):
        os.system('curl -sO https://raw.githubusercontent.com/odashi/small_parallel_enja/master/test.ja')

    en = prepare_data('test.en', 'en.processed')
    ja = prepare_data('test.ja', 'ja.processed')

    epoch = 5
    batch_size = 64
    shuffle_size = 500
    en_ja = en.zip(ja) \
        .shuffle(shuffle_size) \
        .batch(batch_size) \
        .map(batch_transpose)

    print('start training')
    for _ in range(epoch):
        for batch in en_ja:
            do_something(batch)
