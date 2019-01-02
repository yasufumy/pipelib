# pipelib: simple pipeline architecture dataset library
[![Build Status](https://travis-ci.org/yasufumy/pipelib.svg?branch=master)](https://travis-ci.org/yasufumy/pipelib)

pipelib is a simple pipeline architecture dataset library.

You can manipulate any kind of iterable data. Heavily inspired by [tf.data](https://www.tensorflow.org/api_docs/python/tf/data/Dataset), [pyspark.RDD](http://spark.apache.org/docs/2.1.0/api/python/pyspark.html#pyspark.RDD), [cgarciae/pypeln](https://github.com/cgarciae/pypeln), and [kennethreitz/tablib](https://github.com/kennethreitz/tablib)

## Overview

**_pipelib.Dataset()_**

A Dataset is a set of items which can be `dict`, `list`, or `tuple`. It implements `apply`, `map`, `filter`, `flat_map` to manipulate items, and these methods are only evaluated as needed (lazy evaluation). Lazy evaluation allows for more straightforward programming. Also it implements some eager evaluation methods to peek items.

**_pipelib.TextDataset()_**

A TextDataset is a special class for handling a text file. It loads a text line by line. In other words, the items are each line of text.

## Usage

Handle a simple and small data:

```py
from pipelib import Dataset

data = Dataset(range(20))
```

Take a look at the first item:

```py
>>> data.first()
0
```

Take the whole item:

```py
>>> data.all()
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
```

Take the first five items:

```py
>>> data.take(5)
[0, 1, 2, 3, 4]
```

Run for-loop:

```py
>>> for x in data:
...     print(x, end=', ')
0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 
```

Square only even numbers:

```py
>>> data.filter(lambda x: x % 2 == 0).map(lambda x: x ** 2).take(5)
[0, 4, 16, 36, 64]
```

Apply also works:

```py
def square_only_even(dataset):
    for x in dataset:
        if x % 2 == 0:
            yield x ** 2
```

```py
>>> data.apply(square_only_even).take(5)
[0, 4, 16, 36, 64]
```

## Text file

Download sample files:

```bash
$ wget https://raw.githubusercontent.com/wojzaremba/lstm/master/data/ptb.train.txt
$ wget https://gist.githubusercontent.com/yasufumy/e4868a7ab91d20cd3be2e3669b6189cd/raw/553320aeae11221c90a3963c0e0855bceb0184f1/sample.csv
$ wget https://gist.githubusercontent.com/yasufumy/d5231ca019720bb17e80999d5b6e1408/raw/c811aaee38a039ad2ea089814a0b5c63a7abbe12/sample.tsv
$ wget https://gist.githubusercontent.com/yasufumy/f837b0a7047ff4736444c649f85c82dd/raw/2b63139431298ae9cf9fb2363bfe8afc9f4afd04/sample.json
```

Prepare a TextDataset:

```py
import json
import csv

from pipelib import TextDataset

TXT = TextDataset('ptb.train.txt')
CSV = TextDataset('sample.csv')
TSV = TextDataset('sample.tsv')
JSON = TextDataset('sample.json')
```

Take a look at the first line:

```py
>>> TXT.first()
' aer banknote berlitz calloway centrust cluett fromstein gitano guterman hydro-quebec ipo kia memotec mlx nahb punts rake regatta rubens sim snack-food ssangyong swapo wachter '
```

For a CSV file:

```py
>>> CSV.map(lambda x: next(csv.reader([x]))).first()
['Frank', 'Riley', '10']
```

For a TSV file:

```py
>>> TSV.map(lambda x: next(csv.reader([x], delimiter='\t'))).first()
['Frank', 'Riley', '10']
```

For a line-delimited JSON file:

```py
>>> JSON.map(json.loads).first()
{'first': 'Frank', 'last': 'Riley', 'age': 10}
```

## Text Processing

Prepare Penn Tree Bank:

```py
from pipelib import TextDataset

data = TextDataset('ptb.train.txt')
```

Split by space and drop the items which include more than 10 words:

```py
>>> data.map(lambda x: x.strip().split()).filter(lambda x: len(x) < 10).take(5)
[['a', '<unk>', '<unk>', 'said', 'this', 'is', 'an', 'old', 'story'],
 ['there', 'is', 'no', 'asbestos', 'in', 'our', 'products', 'now'],
 ['it', 'has', 'no', 'bearing', 'on', 'our', 'work', 'force', 'today'],
 ['not', 'this', 'year'],
 ['champagne', 'and', '<unk>', 'followed']]
```

Build vocabulary:

```py
>>> from collections import Counter
>>> data = data.map(lambda x: x.strip().split()).filter(lambda x: len(x) < 10)
>>> words, _ = zip(*Counter(data.flat_map(lambda x: x)).most_common())
>>> token_to_index = dict(zip(words, range(len(words))))
>>> data.map(lambda x: [token_to_index[token] for token in x]).take(3)
[[5, 0, 0, 29, 22, 4, 48, 483, 629],
 [36, 4, 39, 2567, 6, 143, 314, 54],
 [7, 19, 39, 2568, 24, 143, 162, 1168, 153]]
```

## Installation

To install pipelib, simply:

```bash
$ pip install pipelib
```
