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
>>> a = [1, 2, 3, 4, 5]
>>> rdd = sc.parallelize(a)
```

#### textFile

```sc.textFile(file)``` ファイルを読み込む。ワイルドカードや世紀表現も使える。

```py
>>> rdd = sc.textFile("./words.txt")
```

#### wholeTextFiles

```sc.wholeTextFiles(dierctory)``` ディレクトリの書くファイルの内容全体をそれぞれRDDの一つの要素に入力する

```py
>>> !ls
a.json b.json c.json
>>> rdd = sc.textWholeFiles("./")
```

### Action

Actionが実行されると初めてTransformationが順に実行される（遅延実行）

#### 要素を返すもの

##### collect

```collect()``` 全ての要素を返す

```py
>>> print(rdd.collect())
[1, 2, 3, 4, 5]
```

##### take

```take(n)``` 最初n個の要素を返す

```py
>>> print(rdd.take(3)
[1, 2, 3]
```

##### first

##### top

#### （統計）情報を返すもの

##### count

```count()``` 要素数を数えて返す

```py
>>> rdd.count()
10
```

##### mean
##### sum
##### variance
##### stdev

#### 保存するもの

##### saveAsTextFile

```saveAsTextFile(file)``` ファイルを保存する

```py
>>> rdd.saveAsTextFile("./a.txt")
```

### Transformation

Transformationはimmutableな新しいRDDを返す

#### filter/map/reduce
##### filter
##### map
##### flatMap
##### flatMapValues(PairRDD)
##### Reduce
##### reduceByKey(PairRDD)

#### ペアRDDに対する操作
##### keyBy(PairRDD)
##### keys
##### values

#### Join操作
##### leftOuterJoin
##### rightOuterJoin
##### fullOuterJoin

#### ソート操作
##### sortBy
##### sortbyKey(PairRDD)

#### 集合操作など
##### intersection
##### union
##### zip
##### distinct

#### サンプリング操作

##### sample
##### takeSample

#### デバッグ

##### toDebugString

## Dataframe

<!-- ## Spark Streaming -->
<!-- ## Mllib -->
