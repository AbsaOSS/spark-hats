    Copyright 2018-2020 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

# spark-hats

Spark **H**elpers for **A**rray **T**ransformation**s**"
This library extends Spark DataFrame API with helpers for transforming fields inside nested structures and arrays of
arbitrary levels of nesting.

## Usage

Reference the library

### Scala 2.11
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.11)

```
groupId: za.co.absa
artifactId: spark-hats_2.11
version: 0.1.0
```

### Scala 2.12
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-hats_2.12)

```
groupId: za.co.absa
artifactId: spark-hats_2.12
version: 0.1.0
```

Please, use the table below to determine what version of spark-hats to use for Spark compatibility.

| spark-hats version | Scala version |  Spark version  |
|:------------------:|:-------------:|:---------------:|
|       0.1.x        |  2.11, 2.12   |     2.4.3+      |

Import the extensions into your scope.

```scala
import za.co.absa.spark.hats.Extensions._
```

## Methods

The usage of nested helper methods is shown on this simple example that contains an array of struct fields.

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

### Add a column
The **nestedWithColumn** method allows adding new fields inside nested structures and arrays.

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

### Drop a column
The **nestedDropColumn** method allows dropping fields inside nested structures and arrays.


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

The **nestedMapColumn** method applies a transformation on a nested field. If the input column is a primitive field the
method will add outputColumnName at the same level of nesting by executing the `expression` passing the source column
into it. If a struct column is expected you can use `.getField(...)` method to operate on its children.

The output column name can omit the full path as the field will be created at the same level of nesting as the input column.

```scala
scala> df.nestedMapColumn("my_array.a", "c", a => a + 1).printSchema
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- a: long (nullable = true)
 |    |    |-- b: string (nullable = true)
 |    |    |-- c: long (nullable = true)

scala> df.nestedMapColumn("my_array.a", "c", a => a + 1).show(false)
+---+---------------------------------------+
|id |my_array                               |
+---+---------------------------------------+
|1  |[[1, foo, 2]]                          |
|2  |[[1, bar, 2], [2, baz, 3], [3, foz, 4]]|
+---+---------------------------------------+
```

### Map a struct column

The **nestedMapStruct** method applies a transformation on a nested struct that can be inside an array. Given a struct
field the method creates a new child field of that struct as a transformation of struct fields. This is useful for
transformations such as concatenation of fields.

The lambda function receives a struct field as an argument. To access child fields use `.getField(fieldName)`. For example, 

```scala
scala> df.nestedMapStruct("my_array", "c", s => concat(s.getField("b"), s.getField("a").cast("string")) ).printSchema
root
 |-- id: long (nullable = true)
 |-- my_array: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- a: long (nullable = true)
 |    |    |-- b: string (nullable = true)
 |    |    |-- c: string (nullable = true)

scala> df.nestedMapStruct("my_array", "c", s => concat(s.getField("b"), s.getField("a").cast("string")) ).show(false)
+---+------------------------------------------------+
|id |my_array                                        |
+---+------------------------------------------------+
|1  |[[1, foo, foo1]]                                |
|2  |[[1, bar, bar1], [2, baz, baz2], [3, foz, foz3]]|
+---+------------------------------------------------+
```

The function can be generically used for applying transformation to a schema root since schema root is also a struct.
To use root of the schema as the input struct pass "" as the input column name.
In this case `null` will be passed to the lambda function.

To generically handle root and nested cases a null check should be done inside the lambda function:
```scala
val dfOut = df.nestedMapStruct(columnPath, "combinedField", s => {
if (s == null) {
  // The columns are at the root level
  concat(col("city"), col("street"))
} else {
  // The columns are inside nested structs/arrays
  concat(s.getField("city"), s.getField("street"))
}
})
```

