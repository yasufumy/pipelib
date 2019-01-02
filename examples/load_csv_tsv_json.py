import os
import os.path as osp

import csv
import json
from pipelib import TextDataset


if __name__ == '__main__':

    if not osp.exists('sample.csv'):
        os.system('curl -sO https://gist.githubusercontent.com/yasufumy/e4868a7ab91d20cd3be2e3669b6189cd/raw/553320aeae11221c90a3963c0e0855bceb0184f1/sample.csv')  # noqa
    if not osp.exists('sample.tsv'):
        os.system('curl -sO https://gist.githubusercontent.com/yasufumy/d5231ca019720bb17e80999d5b6e1408/raw/c811aaee38a039ad2ea089814a0b5c63a7abbe12/sample.tsv')  # noqa
    if not osp.exists('sample.json'):
        os.system('curl -sO https://gist.githubusercontent.com/yasufumy/f837b0a7047ff4736444c649f85c82dd/raw/2b63139431298ae9cf9fb2363bfe8afc9f4afd04/sample.json')  # noqa

    data_csv = TextDataset('sample.csv')
    print('CSV file')
    print(data_csv.first())
    print(data_csv.map(lambda x: next(csv.reader([x]))).first())

    data_tsv = TextDataset('sample.tsv')
    print('TSV file')
    print(data_tsv.first())
    print(data_tsv.map(lambda x: next(csv.reader([x], delimiter='\t'))).first())

    data_json = TextDataset('sample.json')
    print('JSON file')
    print(data_json.first())
    print(data_json.map(json.loads).first())
