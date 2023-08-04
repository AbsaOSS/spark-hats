# spark-hats
[![Build](https://github.com/AbsaOSS/spark-hats/workflows/Build/badge.svg)](https://github.com/AbsaOSS/spark-hats/actions)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FAbsaOSS%2Fspark-hats.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FAbsaOSS%2Fspark-hats?ref=badge_shield)

Spark "**H**elpers for **A**rray **T**ransformation**s**"

This library extends Spark DataFrame API with helpers for transforming fields inside nested structures and arrays of
arbitrary levels of nesting.

## Usage

Reference the library

<table>
<tr><th>Scala 2.11</th><th>Scala 2.12</th><th>Scala 2.13</th></tr>
<tr>
<td align="center">
<a href = "https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.11"><img src = "https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.11/badge.svg" alt="Maven Central"></a><br>
</td>
<td align="center">
<a href = "https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.12"><img src = "https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.12/badge.svg" alt="Maven Central"></a><br>
</td>
<td align="center">
<a href = "https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.13"><img src = "https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.13/badge.svg" alt="Maven Central"></a><br>
</td>
</tr>
<tr>
<td>
<pre>groupId: za.co.absa<br>artifactId: spark-hats_2.11<br>version: 0.3.0</pre>
</td>
<td>
<pre>groupId: za.co.absa<br>artifactId: spark-hats_2.12<br>version: 0.3.0</pre>
</td>
<td>
<pre>groupId: za.co.absa<br>artifactId: spark-hats_2.13<br>version: 0.3.0</pre>
</td>
</tr>
</table>

Please, use the table below to determine what version of spark-hats to use for Spark compatibility.

| spark-hats version | Scala version | Spark version |
|:------------------:|:-------------:|:-------------:|
|       0.1.x        |  2.11, 2.12   |    2.4.3+     |
|       0.2.x        |  2.11, 2.12   |    2.4.3+     |
|       0.2.x        |     2.12      |    3.0.0+     |
|       0.3.x        |     2.11      |    2.4.3+     |
|       0.3.x        |  2.12, 2.13   |    3.2.1+     |

To use the extensions you need to add this import to your Spark application or shell:
```scala
import za.co.absa.spark.hats.Extensions._
```

### How to generate Code coverage report
```
sbt ++{matrix.scala} jacoco -DSPARK_VERSION={matrix.spark}
```
Code coverage will be generated on path:
```
{project-root}/spark-hats/target/scala-{scala_version}/jacoco/report/html
```


## Motivation

Here is a small example we will use to show you how `spark-hats` work. The important thing is that the dataframe
contains an array of struct fields.

```scala
scala> df.printSchema()
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- a: long (nullable = true)
 |    |    |-- b: string (nullable = true)
       
scala> df.show(false)
+---+------------------------------+
|id |my_array                      |
+---+------------------------------+
|1  |[[1, foo]]                    |
|2  |[[1, bar], [2, baz], [3, foz]]|
+---+------------------------------+
```

Now, say, we want to add a field `c` as part of the struct alongside `a` and `b` from the example above. The
expression for `c` is `c = a + 1`.

Here is the code you can use in Spark:
```scala
    val dfOut = df.select(col("id"), transform(col("my_array"), c => {
      struct(c.getField("a").as("a"),
        c.getField("b").as("b"),
        (c.getField("a") + 1).as("c"))
    }).as("my_array"))

```
(to use `transform()` in Scala API you need to add [spark-hofs](https://github.com/AbsaOSS/spark-hofs) as a dependency).

Here is how it looks when using `spark-hats` library. 
```scala
    val dfOut = df.nestedMapColumn("my_array.a","c", a => a + 1)
```

Both produce the following results:
```scala
scala> dfOut.printSchema
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- a: long (nullable = true)
 |    |    |-- b: string (nullable = true)
 |    |    |-- c: long (nullable = true)

scala> dfOut.show(false)
+---+---------------------------------------+
|id |my_array                               |
+---+---------------------------------------+
|1  |[[1, foo, 2]]                          |
|2  |[[1, bar, 2], [2, baz, 3], [3, foz, 4]]|
+---+---------------------------------------+
```

Imagine how the code will look like for more levels of array nesting.

## Methods

### Add a column
The `nestedWithColumn` method allows adding new fields inside nested structures and arrays.

The addition of a column API is provided in two flavors: the basic and the extended API. The basic API is simpler to
use, but the expressions it expects can only reference columns at the root of the schema. Here is an example of the basic add
column API:

```scala
scala> df.nestedWithColumn("my_array.c", lit("hello")).printSchema
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- a: long (nullable = true)
 |    |    |-- b: string (nullable = true)
 |    |    |-- c: string (nullable = false)

scala> df.nestedWithColumn("my_array.c", lit("hello")).show(false)
+---+---------------------------------------------------+
|id |my_array                                           |
+---+---------------------------------------------------+
|1  |[[1, foo, hello]]                                  |
|2  |[[1, bar, hello], [2, baz, hello], [3, foz, hello]]|
+---+---------------------------------------------------+
```

### Add column (extended)
The extended API method `nestedWithColumnExtended` works similarly to the basic one but allows the caller to reference
other array elements, possibly on different levels of nesting. The way it allows this is a little tricky.
The second parameter is changed from being a column to a *function that returns a column*. Moreover, this function has
an argument which is a function itself, the `getField()` function. The `getField()` function can be used in the
transformation to reference other columns in the dataframe by their fully qualified name.

In the following example, a transformation adds a new field `my_array.c` to the dataframe by concatenating a root
level column `id` with a nested field `my_array.b`:

```scala
scala> val dfOut = df.nestedWithColumnExtended("my_array.c", getField =>
         concat(getField("id").cast("string"), getField("my_array.b"))
       )

scala> dfOut.printSchema
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- a: long (nullable = true)
 |    |    |-- b: string (nullable = true)
 |    |    |-- c: string (nullable = true)

scala> dfOut.show(false)
+---+------------------------------------------------+
|id |my_array                                        |
+---+------------------------------------------------+
|1  |[[1, foo, 1foo]]                                |
|2  |[[1, bar, 2bar], [2, baz, 2baz], [3, foz, 2foz]]|
+---+------------------------------------------------+
```

* **Note.** You can still use `col` to reference root level columns. But if a column is inside an array (like
`my_array.b`), invoking `col("my_array.b")` will reference the whole array, not an individual element. The `getField()`
function that is passed to the transformation solves this by adding a generic way of addressing array elements on arbitrary
levels of nesting.

* **Advanced Note.** If there are several arrays in the schema, `getField()` allows to reference elements of an array
if it is one of the parents of the output column.


### Drop a column
The `nestedDropColumn` method allows dropping fields inside nested structures and arrays.


```scala
scala> df.nestedDropColumn("my_array.b").printSchema
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- a: long (nullable = true)

scala> df.nestedDropColumn("my_array.b").show(false)
+---+---------------+
|id |my_array       |
+---+---------------+
|1  |[[1]]          |
|2  |[[1], [2], [3]]|
+---+---------------+
```

### Map a column

The `nestedMapColumn` method applies a transformation on a nested field. If the input column is a primitive field the
method will add `outputColumnName` at the same level of nesting. If a struct column is expected you can use
`.getField(...)` method to operate on its children.

The output column name can omit the full path as the field will be created at the same level of nesting as the input column.

```scala
scala> df.nestedMapColumn(inputColumnName = "my_array.a", outputColumnName = "c", expression = a => a + 1).printSchema
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- a: long (nullable = true)
 |    |    |-- b: string (nullable = true)
 |    |    |-- c: long (nullable = true)

scala> df.nestedMapColumn(inputColumnName = "my_array.a", outputColumnName = "c", expression = a => a + 1).show(false)
+---+---------------------------------------+
|id |my_array                               |
+---+---------------------------------------+
|1  |[[1, foo, 2]]                          |
|2  |[[1, bar, 2], [2, baz, 3], [3, foz, 4]]|
+---+---------------------------------------+
```

## Other transformations

### Unstruct

Syntax: `df.nestedUnstruct("NestedStructColumnName")`.

Flattens one level of nesting when a struct is nested in another struct. For example,

```scala
scala> df.printSchema
root
|-- id: long (nullable = true)
|-- my_array: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- a: long (nullable = true)
|    |    |-- b: string (nullable = true)
|    |    |-- c: struct (containsNull = true)
|    |    |    |--nestedField1: string (nullable = true)
|    |    |    |--nestedField2: long (nullable = true)

scala> df.nestedUnstruct("my_array.c").printSchema
root
|-- id: long (nullable = true)
|-- my_array: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- a: long (nullable = true)
|    |    |-- b: string (nullable = true)
|    |    |-- nestedField1: string (nullable = true)
|    |    |-- nestedField2: long (nullable = true)
```

Note that the output schema doesn't have the `c` struct. All fields of `c` are now part of the parent struct. 

## Changelog
- #### 0.3.0 released 3 August 2023.
  - [#38](https://github.com/AbsaOSS/spark-hats/issues/38) Add scala 2.13 support.
  - [#33](https://github.com/AbsaOSS/spark-hats/issues/33) Update spark test to 3.2.1.
  - [#35](https://github.com/AbsaOSS/spark-hats/issues/35) Add code coverage support.

- #### 0.2.2 released 8 March 2021.
  - [#23](https://github.com/AbsaOSS/spark-hats/issues/23) Added `nestedUnstruct()` method that flattens one level of nesting for a given struct.

- #### 0.2.1 released 21 January 2020.
  - [#10](https://github.com/AbsaOSS/spark-hats/issues/10) Fixed error column aggregation when the input array is `null`.
  
- #### 0.2.0 released 16 January 2020.
  - [#5](https://github.com/AbsaOSS/spark-hats/issues/5) Added the extended nested transformation API that allows referencing arbitrary columns.


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FAbsaOSS%2Fspark-hats.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FAbsaOSS%2Fspark-hats?ref=badge_large)