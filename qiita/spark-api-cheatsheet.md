<!-- Spark API -->

# 目的
Sparkのよく使うAPIを（主に自分用に）メモしておくことで、久しぶりに開発するときでもサクサク使えるようにしたい。とりあえずPython版をまとめておきます（Scalaも時間があれば加筆するかも）

**このチートシートはあくまでチートシート**なので（引数が省略してあったりします）、時間がある方はきちんと[公式APIドキュメント(Spark Python API Docs)](http://spark.apache.org/docs/latest/api/python/index.html)を見て下さい。

# Spark API チートシート（Python）

以下では次を前提とする

```py
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)
```

## RDD

### RDDを作る（データの読み込み）

##### parallelize

```sc.parallelize(collection)``` リストやタプルからRDDを作る

```py
>>> a = [1, 2, 3, 4, 5]
>>> rdd = sc.parallelize(a)
```

##### textFile

```sc.textFile(file)``` ファイルを読み込む。ワイルドカードや世紀表現も使える。

```py
>>> rdd = sc.textFile("./words.txt")
```

##### wholeTextFiles

```sc.wholeTextFiles(dierctory)``` ディレクトリの書くファイルの内容全体をそれぞれRDDの一つの要素に入力する

```py
# $ ls
# a.json b.json c.json
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

```first()``` 一番最初の要素を返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.first()
1
```

##### top

```top(n)``` 大きいものからn個要素を返す

```
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.first()
[3, 2]
```

#### （統計）量を返すもの

##### count

```count()``` 要素数を数えて返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.count()
3
```

##### mean

```mean()``` 平均を返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.mean()
3.0
```

##### sum

```sum()``` 合計を返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.sum()
6
```

##### variance

```variance()``` 分散を返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.variance()
0.6666666666666666
```

##### stdev

```stdev()``` 標準偏差を返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.stdev()
0.816496580927726
```

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

```filter(f)``` fが真となる要素だけを含むRDDを返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.filter(lambda x: x % 2 == 0).collect()
[2]
```

##### map

```map(f)``` 全ての要素にfを作用させたRDDを返す

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.map(lambda x: x * 2).collect()
[2, 4, 6]
```

##### flatMap

```flatMap(f)``` 全ての要素にfを作用させたあと、要素内のリストを展開したRDDを返す

```py
>>> rdd = sc.parallelize(["This is a pen", "This is an apple"])
>>> rdd.flatMap(lambda x: x.split()).collect()
['This', 'is', 'a', 'pen', 'This', 'is', 'an', 'apple']
```

##### Reduce

```reduce(f)``` 二つの要素にfを作用させ続けて一つの返り値を得る

```py
>>> rdd = sc.parallelize([1, 2, 3])
>>> rdd.reduce(lambda x, y: x + y)
6
```

ペアRDDをつくる

#### ペアRDDに対する操作

ペアRDDはPythonでいうTupleを要素にもつRDD。keyとvalueを扱うことができる。
作り方は```keyBy```を使うか```map```で要素数2のタプルを要素に返す。

##### keyBy(PairRDD)

```keyBy(f)``` 普通のRDDの要素にfを作用させ、その返り値をkeyに、元の要素をそのままvalueにしたRDDを返す

```py
>>> rdd = sc.parallelize(["Ken 27 180 83", "Bob 32 170 65", "Meg 29 165 45"])
>>> rdd.keyBy(lambda x: x.split()[0]).collect()
[('Ken', 'Ken 27 180 83'), ('Bob', 'Bob 32 170 65'), ('Meg', 'Meg 29 165 45')]
```

##### keys

```keys``` ペアRDDのkeyだけからなるRDDを返す

```py
>>> rdd = sc.parallelize([("Ken", 2), ("Bob", 3), ("Taka", 1), ("Ken", 3), ("Bob", 2)])
>>> rdd.keys().collect()
['Ken', 'Bob', 'Taka', 'Ken', 'Bob']
```

##### values

```values``` ペアRDDのvlaueだけからなるRDDを返す

```py
>>> rdd = sc.parallelize([("Ken", 2), ("Bob", 3), ("Taka", 1), ("Ken", 3), ("Bob", 2)])
>>> rdd.values().collect()
[2, 3, 1, 3, 2]
```

##### flatMapValues

```flatMapValues(f)``` PairRDDのvalueにflatMapを作用させてkeyを複製して所謂縦持ちにする

```py
>>> rdd = sc.parallelize([("Ken", "Yumi,Yukiko"), ("Bob", "Meg, Tomomi, Akira"), ("Taka", "Yuki")])
>>> rdd.flatMapValues(lambda x: x.split(","))
[('Ken', 'Yumi'),
 ('Ken', 'Yukiko'),
 ('Bob', 'Meg'),
 ('Bob', ' Tomomi'),
 ('Bob', ' Akira'),
 ('Taka', 'Yuki')]
```

##### reduceByKey

```reduceByKey(f)``` 同じkeyの要素でグループ化してvalueにreduceを作用させる

```py
>>> rdd = sc.parallelize([("Ken", 2), ("Bob", 3), ("Taka", 1), ("Ken", 3), ("Bob", 2)])
>>> rdd.reduceByKey(lambda x, y: x + y).collect()
```

##### countByKey

```countByKey()``` 同じkeyの値がいくつあるか数えてdictで返す

```py
>>> rdd = sc.parallelize([("Ken", 2), ("Bob", 3), ("Taka", 1), ("Ken", 3), ("Bob", 2)])
>>> rdd.countByKey()
defaultdict(<type 'int'>, {'Ken': 2, 'Bob': 2, 'Taka': 1})
```

##### sortByKey

```sortByKey``` ペアRDDをkeyでソートします

```py
>>> rdd = sc.parallelize([("cba", 2), ("abc", 3), ("bac", 1), ("bbb", 
>>> rdd.sortByKey().collect()
[('aaa', 2), ('abc', 3), ('bac', 1), ('bbb', 3), ('cba', 2)]
```

#### Join操作

##### leftOuterJoin
二つのRDDをleft outer joinして、valueに二つの要素のタプルをもつペアRDDを返す

```py
>>> rdd1 = sc.parallelize([("Ken", 1), ("Bob", 2), ("Meg", 3)])
>>> rdd2 = sc.parallelize([("Ken", 1), ("Kaz", 3)])
>>> rdd1.leftOuterJoin(rdd2).collect()
[('Bob', (2, None)), ('Meg', (3, None)), ('Ken', (1, 1))]
```

##### rightOuterJoin
二つのRDDをright outer joinして、valueに二つの要素のタプルをもつペアRDDを返す

```py
>>> rdd1 = sc.parallelize([("Ken", 1), ("Bob", 2), ("Meg", 3)])
>>> rdd2 = sc.parallelize([("Ken", 1), ("Kaz", 3)])
>>> rdd1.rightOuterJoin(rdd2).collect()
[('Ken', (1, 1)), ('Kaz', (3, None))]
```

##### fullOuterJoin
二つのRDDをfull outer joinして、valueに二つの要素のタプルをもつペアRDDを返す

```py
>>> rdd1 = sc.parallelize([("Ken", 1), ("Bob", 2), ("Meg", 3)])
>>> rdd2 = sc.parallelize([("Ken", 1), ("Kaz", 3)])
>>> rdd1.fullOuterJoin(rdd2).collect()
[('Bob', (2, None)), ('Meg', (3, None)), ('Ken', (1, 1)), ('Kaz', (None, 3))]
```

#### ソート操作

##### sortBy

```sortBy(f)``` fの返す値によってソートする

```py
>>> rdd = sc.parallelize([("cba", 2), ("abc", 3), ("bac", 1), ("bbb", 
>>> rdd.sortBy(lambda (x, y): x).collect() # sortByKeyと同じ
```

#### 集合操作など

##### intersection

```intersection(rdd)``` 二つのRDDのintersectionを返す

##### union

```union(rdd)``` 二つのRDDのunionを返す

##### zip
```zip(rdd)``` 引数のrddの各要素をvlaueにしたペアRDDを返す

```py
>>> rdd = sc.parallelize([("Ken", 2), ("Bob", 3), ("Taka", 1), ("Ken", 3), ("Bob", 2)])
>>> rdd.keys().zip(rdd.values())
[('Ken', 2), ('Bob', 3), ('Taka', 1), ('Ken', 3), ('Bob', 2)]
```

##### distinct
同じ要素を含まないRDDを返します

#### サンプリング操作

##### sample

```sample(bool, frac)``` サンプリングしたRDDを返す。第一引数で同じ要素の重複を許すか決める。

```py
>>> rdd = sc.parallelize([1, 2, 3, 4, 5])
>>> rdd.sample(True, 0.5).collect()
[1, 5, 5]
>>> rdd.sample(False, 0.5).collect()
[1, 3, 5]
```

##### takeSample

```takeSmaple(bool, size)``` 固定されたサイズのサンプルをリストで返す。第一引数で同じ要素の重複を許すか決める。


```py
>>> rdd = sc.parallelize([1, 2, 3, 4, 5])
>>> rdd.takeSample(True, 2)
[5, 5]
>>> rdd.takeSample(False, 2)
[3, 5]
```

#### デバッグ

##### toDebugString

```py
print(rdd.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).toDebugString())
(1) PythonRDD[190] at RDD at PythonRDD.scala:43 []
 |  MapPartitionsRDD[189] at mapPartitions at PythonRDD.scala:374 []
 |  ShuffledRDD[188] at partitionBy at null:-1 []
 +-(1) PairwiseRDD[187] at reduceByKey at <ipython-input-114-71f5cb742e13>:1 []
    |  PythonRDD[186] at reduceByKey at <ipython-input-114-71f5cb742e13>:1 []
    |  ParallelCollectionRDD[141] at parallelize at PythonRDD.scala:423 []
```

#### 永続化

##### persist

```persist()``` RDDをそのまま（デフォルトではメモリに）キャッシュする。メモリだけ、メモリが無理ならディスク、ディスクだけ、などの設定が出来る（```StorageLevel```で指定）

```py
>>> rdd.persist()
```

##### unpersist

```unpersist()``` RDDの永続化を解く。永続化レベルを変える時などに使う。

```py
>>> from pyspark import StorageLevel
>>> rdd.persist()
>>> rdd.unpersist()
>>> rdd.persist(StorageLevel.DISK_ONLY)
```

#### よくある例
随時追加していく予定

##### word count

```
>>> rdd.flatMap(lambda x: x.split())\
       .map(lambda x: (x, 1))\
       .reduceByKey(lambda x, y: x + y)\
       .take(5)
```

## DataFrame
特に構造化データを扱うときはこちらの方が便利。

### DataFrameをつくる（データの読み込み）

##### read.json

```read.json(file)``` jsonからデータを読み込む

```py
# $ cat a.json
# {"name":"Ken", "age":35}
# {"name":"Bob", "age":30, "weight":80}
# {"name":"Meg", "age":29, "weight":45}
df = sqlContext.read.json("a.json")
```

### DataFrameを表示する

RDDと同じ```collect```, ```take```の他に```show``` がある

##### show

```show(n)``` n行表示する（nはデフォルトで20）

```py
>>> df.show()
+---+----+------+
|age|name|weight|
+---+----+------+
| 35| Ken|  null|
| 30| Bob|    80|
| 29| Meg|    45|
+---+----+------+
```

### DataFrameの操作

##### select

```select(column)``` stringかColumnオブジェクトを渡してselectしたDataFrameを返す。カラムを列挙して複数列取得したり、演算することもできる。


```py
>>> df.select("age").show()
+---+
|age|
+---+
| 35|
| 30|
| 29|
+---+

# 次も同じ
>>> df.select(df.age).show() # Columnオブジェクトを渡す
>>> df.select(df["age"]).show() # Columnオブジェクトを渡す
```

```py
>>> df.select(df.name, df.age).show()
+----+---+
|name|age|
+----+---+
| Ken| 35|
| Bob| 30|
| Meg| 29|
+----+---+
```

###### DataframeのColumnオブジェクト

selectで渡すColumnオブジェクトのアクセエスの仕方として、Pythonでは次の2パターンが容易されている: 

```py
>>> df.age
Column<age>
>>> df["age"]
Column<age>
```

##### where/filter

```filter(condition)``` stringの条件に合う行だけからなるDataFrameを返す。 ```where```は```filter```のaliasである。


```py
>>> df.where(df.age >=30).show()
+---+----+------+
|age|name|weight|
+---+----+------+
| 35| Ken|  null|
| 30| Bob|    80|
+---+----+------+
```

##### sort

```sort(column)``` 指定されたカラムでソートされたDataFrameを返す

```py
>>> df.sort(df.age)
+---+----+------+
|age|name|weight|
+---+----+------+
| 29| Meg|    45|
| 30| Bob|    80|
| 35| Ken|  null|
+---+----+------+
```

##### limit

```limit(n)``` 先頭n行だけに制限したDataFrameを返す

```py
>>> df.limit(1).show()
+---+----+------+
|age|name|weight|
+---+----+------+
| 35| Ken|  null|
+---+----+------+
```

##### distinct

```distinct()``` distinctした結果の行だけからなるDataFrameを返す

```py
>>> df.distinct().count()
3
```

##### join

```join(dataframe, on, how)``` howのデフォルトはinnner

- on: カラム、もしくはカラムのリスト
- how: ```"inner"```, ```"outer"```, ```"left_outer"```, ```"right_outer"```, ```"leftsemi"``` のいずれか

### DataframeからRDDへ変換
DataFrameはRDD上に構築されているので、元となるRDDを取り出すことができる

```py
>>> print(df.rdd.collect())
[Row(age=35, name=u'Ken', weight=None),
 Row(age=30, name=u'Bob', weight=80),
 Row(age=29, name=u'Meg', weight=45)]
```

特定の列だけ取り出すには```Row```オブジェクトの対応する属性にアクセスする

```py
df.rdd.map(lambda row: (row.age, row.weight)).collect()
[(35, None), (30, 80), (29, 45)]
```


### Dataframeを保存する

##### toJson

```toJson()``` jsonの形でRDDに変換する。このあと```saveAsTextFile```を呼べばjson形式で保存できる。

```py
>>> df.toJSON().saveAsTextFile("b.json")
>>> df2 = sqlContext.read.json("/b.json")
+---+----+------+
|age|name|weight|
+---+----+------+
| 35| Ken|  null|
| 30| Bob|    80|
| 29| Meg|    45|
+---+----+------+
```

# 今後
Spark StreamingやMllib関連もこちらに追記していくかもしれない。

<!-- ## Spark Streaming -->
<!-- ## Mllib -->
