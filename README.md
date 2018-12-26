# pipes: simple pipeline architecture dataset library

datalib is a simple pipeline architecture dataset library.

You can manipulate any kind of iterable data. Heavily inspired by [tf.data](https://www.tensorflow.org/api_docs/python/tf/data/Dataset), [pyspark.RDD](http://spark.apache.org/docs/2.1.0/api/python/pyspark.html#pyspark.RDD), [cgarciae/pypeln](https://github.com/cgarciae/pypeln), and [kennethreitz/tablib](https://github.com/kennethreitz/tablib)

## Overview

**_pipes.Dataset()_**

　　　　A Dataset is a set of items which can be `dict`, `list`, or `tuple`. It implements `map`, `filter`, `flat_map` to manipulate items, and these methods are only evaluaed as needed (lazy evaluation). Lazy evaluation allows for more straightforward programming. Also it implements some eager evaluation methods to peek items.
    
**_pipes.TextDataset()_**

　　　　A TextDataset is a special class for handling a text file. It loads a text line by line. In other words, the items are each line of text.
  
## Usage

Handle a simple and small data:

```py
from pipes import Dataset

data = Dataset(range(20))
```

Take a look at the first item:

```py
>>> data.first()
0
```

Take the whole item:

```py
>>> data.collect()
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

Squre only even numbers:

```py
>>> data.filter(lambda x: x % 2 == 0).map(lambda x: x ** 2).take(5)
[0, 4, 16, 36, 64]
```

## Text file

Download a text file:

```bash
$ wget https://raw.githubusercontent.com/wojzaremba/lstm/master/data/ptb.train.txt
```

Prepare a TextDataset:

```py
from pipes import TextDataset

data = TextDataset('ptb.train.txt')
```

Take a look at the first item:

```
>>> data.first()
' aer banknote berlitz calloway centrust cluett fromstein gitano guterman hydro-quebec ipo kia memotec mlx nahb punts rake regatta rubens sim snack-food ssangyong swapo wachter '
```

Split by space and drop the items which include more than 10 words:

```
>>> data.map(lambda x: x.strip().split()).filter(lambda x: len(x) < 10).take(5)
[['a', '<unk>', '<unk>', 'said', 'this', 'is', 'an', 'old', 'story'],
 ['there', 'is', 'no', 'asbestos', 'in', 'our', 'products', 'now'],
 ['it', 'has', 'no', 'bearing', 'on', 'our', 'work', 'force', 'today'],
 ['not', 'this', 'year'],
 ['champagne', 'and', '<unk>', 'followed']]
```

## Installation

To install datalib, simply:

```bash
$ pip install pipes
```
