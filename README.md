# pipelib: Minimal Data Pipeline Library for Python
[![Build Status](https://travis-ci.org/yasufumy/pipelib.svg?branch=master)](https://travis-ci.org/yasufumy/pipelib)

pipelib is a minimal data pipeline library. It supports parallel computation and lazy evaluation.

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

## Parallel Computation

Prepare a small data and a slow function:

```py
import time
import os

from pipelib import Dataset


def slow(x):
    print(f'[{os.getpid()}]: processing...')
    time.sleep(0.5)
    return x

data = Dataset(range(20))
```

Parallel computation for a slow function:

```py
>>> data.map_parallel(slow).take(5)
[26382]: processing...
[26383]: processing...
[26384]: processing...
[26382]: processing...
[26385]: processing...
[26383]: processing...
[26384]: processing...
[26382]: processing...
[26385]: processing...
[26383]: processing...
[26384]: processing...
[26382]: processing...
[26385]: processing...
[26383]: processing...
[26384]: processing...
[26382]: processing...
[26385]: processing...
[26383]: processing...
[26384]: processing...
[26382]: processing...
[0, 1, 2, 3, 4]
>>> data.map_parallel(slow, unordered=True).take(5)
[27493]: processing...
[27494]: processing...
[27493]: processing...
[27495]: processing...
[27494]: processing...
[27496]: processing...
[27493]: processing...
[27494]: processing...
[27495]: processing...
[27493]: processing...
[27496]: processing...
[27494]: processing...
[27495]: processing...
[27493]: processing...
[27496]: processing...
[27494]: processing...
[27495]: processing...
[27493]: processing...
[27496]: processing...
[27494]: processing...
[0, 1, 4, 5, 2]
```

It supports order and unorder computation.
Also, you can do parallel computation for `map`, `flat_map` and `filter`.

pipelib has an iterator to prefetch items from dataset in a worker thread.
You can use it as follows:

```py
>>> data = data.map_parallel(slow)  # not evaluated (lazy evaluation)
>>> it = data.get_prefetch_iterator(n_prefetch=5)  # evaluating in a worker thread
[39072]: processing...
[39073]: processing...
[39072]: processing...
[39074]: processing...
[39073]: processing...
[39072]: processing...
[39073]: processing...
[39072]: processing...
[39074]: processing...
[39073]: processing...
[39072]: processing...
[39074]: processing...
[39073]: processing...
[39072]: processing...
[39074]: processing...
[39073]: processing...
[39072]: processing...
[39074]: processing...
[39073]: processing...
>>> next(it)
0
```

## Installation

To install pipelib, simply:

```bash
$ pip install pipelib
```
