# datalib: simple pipeline architecture dataset library

datalib is a simple pipeline architecture dataset library.

Heavily inspired by [tf.data](https://www.tensorflow.org/api_docs/python/tf/data/Dataset) and [pyspark.RDD](http://spark.apache.org/docs/2.1.0/api/python/pyspark.html#pyspark.RDD)

## Overview

*datalib.Dataset()*
  A Dataset is a series of data. 
  
## Usage

```py
from datalib import Dataset

d = Dataset(range(100)) \
      .filter(lambda x: x % 2 == 0) \
      .map(lambda x: x ** 2)
```
