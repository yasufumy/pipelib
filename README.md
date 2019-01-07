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

Zip datasets:

```py
>>> other = Dataset(range(20)).map(lambda x: x * 2)
>>> data.map(lambda x: x / 2).zip(other).take(5)
[(0.0, 0), (0.5, 2), (1.0, 4), (1.5, 6), (2.0, 8)]
```

Concatenate datasets:

```py
>>> other = Dataset(range(3)).map(lambda x: 1 + x ** 2)
>>> other.concat(data).take(5)
[1, 2, 5, 0, 1]
```

## Text Processing

First of all, download Penn Tree Bank dataset:

```bash
$ curl -sO https://raw.githubusercontent.com/wojzaremba/lstm/master/data/ptb.train.txt
```

Load Penn Tree Bank dataset:

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
