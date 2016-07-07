<!-- Spark API -->

# 目的
Sparkのよく使うAPIをメモしておくことで、久しぶりに開発するときでもサクサク使えるようにしたい。
とりあえずPython版をまとめる。

# Spark API チートシート（Python）

以下では次を前提とする

```py
from pyspark import SparkContext

sc = SparkContext()
```

## RDD

### RDDを作る（データの読み込み）

#### parallelize

```sc.parallelize(collection)``` リストやタプルからRDDを作る

```py
>>> a = [1, 2, 3]
>>> rdd = sc.parallelize(a)
```

#### textFile

```sc.textFile(file)``` ファイルを読み込む。ワイルドカードや世紀表現も使える。

```py
>>> rdd = sc.textFile("./words.txt")
```

### Action

#### take 
#### count
#### collect
#### saveAsTextFile

### Transformation

## Spark Streaming
## Dataframe
