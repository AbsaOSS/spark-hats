/*
 * Copyright 2018-2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.hats.transformations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import za.co.absa.spark.hats.SparkTestBase
import za.co.absa.spark.hats.transformations.samples.DeepArraySamples._

// Examples for constructing dataframes containing arrays of various levels of nesting

// The case classes were declared at the package level so it can be used to create Spark DataSets
// It is declared package private so the names won't pollute public/exported namespace


class DeepArrayTransformationSuite extends FunSuite with SparkTestBase {
  // scalastyle:off line.size.limit
  // scalastyle:off null

  import spark.implicits._
  import za.co.absa.spark.hats.Extensions._

  private val log = LoggerFactory.getLogger(this.getClass)

  test("Test uppercase of a plain field") {
    val df = spark.sparkContext.parallelize(plainSampleN).toDF

    val dfOut = df.nestedMapColumn("city", "conformedCity", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- city: string (nullable = true)
        | |-- street: string (nullable = true)
        | |-- conformedCity: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"city":"Olomuc","street":"Vodickova","conformedCity":"OLOMUC"}
        |{"city":"Ostrava","street":"Vlavska","conformedCity":"OSTRAVA"}
        |{"city":"Plzen","street":"Kralova","conformedCity":"PLZEN"}""".stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test uppercase of two level of struct nesting") {
    // Struct of struct
    val df = spark.sparkContext.parallelize(structOfStructSampleN).toDF

    val dfOut = df.nestedMapColumn("employee.address.city", "conformedCity", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: struct (nullable = false)
        | |    |-- name: string (nullable = true)
        | |    |-- address: struct (nullable = false)
        | |    |    |-- city: string (nullable = true)
        | |    |    |-- street: string (nullable = true)
        | |    |    |-- conformedCity: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"employee":{"name":"Martin","address":{"city":"Olomuc","street":"Vodickova","conformedCity":"OLOMUC"}}}
        |{"id":1,"employee":{"name":"Petr","address":{"city":"Ostrava","street":"Vlavska","conformedCity":"OSTRAVA"}}}
        |{"id":1,"employee":{"name":"Vojta","address":{"city":"Plzen","street":"Kralova","conformedCity":"PLZEN"}}}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test uppercase of arrays of primitives") {
    // Array of primitives
    val df = spark.sparkContext.parallelize(arraysOfPrimitivesSampleN).toDF

    val dfOut = df.nestedMapColumn("words", "conformedWords", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- words: array (nullable = true)
        | |    |-- element: string (containsNull = true)
        | |-- conformedWords: array (nullable = true)
        | |    |-- element: string (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"words":["Gizmo","Blurp","Buzinga"],"conformedWords":["GIZMO","BLURP","BUZINGA"]}
        |{"id":1,"words":["Quirk","Zap","Mmrnmhrm"],"conformedWords":["QUIRK","ZAP","MMRNMHRM"]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }


  test("Test uppercase of arrays of arrays of primitives") {
    // Array of arrays of primitives
    val df = spark.sparkContext.parallelize(arraysOfArraysOfPrimitivesSampleN).toDF

    val dfOut = df.nestedMapColumn("matrix", "conformedMatrix", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- matrix: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: string (containsNull = true)
        | |-- conformedMatrix: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: string (containsNull = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"matrix":[["Tree","Table"],["Map","Duck"]],"conformedMatrix":[["TREE","TABLE"],["MAP","DUCK"]]}
        |{"id":2,"matrix":[["Apple","Machine"],["List","Duck"]],"conformedMatrix":[["APPLE","MACHINE"],["LIST","DUCK"]]}
        |{"id":3,"matrix":[["Computer","Snake"],["Sun","Star"]],"conformedMatrix":[["COMPUTER","SNAKE"],["SUN","STAR"]]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test uppercase of a field inside an array of structs") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStructsSampleN).toDF

    val dfOut = df.nestedMapColumn("person.firstName", "conformedName", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- firstName: string (nullable = true)
        | |    |    |-- lastName: string (nullable = true)
        | |    |    |-- conformedName: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[{"firstName":"John","lastName":"Smith","conformedName":"JOHN"},{"firstName":"Jack","lastName":"Brown","conformedName":"JACK"}]}
        |{"id":1,"person":[{"firstName":"Merry","lastName":"Cook","conformedName":"MERRY"},{"firstName":"Jane","lastName":"Clark","conformedName":"JANE"}]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test uppercase of a field inside an array of arrays of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfArraysOfStructSampleN).toDF

    val dfOut = df.nestedMapColumn("person.lastName", "conformedName", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: struct (containsNull = false)
        | |    |    |    |-- firstName: string (nullable = true)
        | |    |    |    |-- lastName: string (nullable = true)
        | |    |    |    |-- conformedName: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[[{"firstName":"Mona Lisa","lastName":"Harddrive","conformedName":"HARDDRIVE"}],[{"firstName":"Lenny","lastName":"Linux","conformedName":"LINUX"},{"firstName":"Dot","lastName":"Not","conformedName":"NOT"}]]}
        |{"id":1,"person":[[{"firstName":"Eddie","lastName":"Larrison","conformedName":"LARRISON"}],[{"firstName":"Scarlett","lastName":"Johanson","conformedName":"JOHANSON"},{"firstName":"William","lastName":"Windows","conformedName":"WINDOWS"}]]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test uppercase of a field inside an array of struct containing an array of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStrtuctsDeepSampleN).toDF

    val dfOut = df.nestedMapColumn("legs.conditions.conif", "conformedField", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- legid: integer (nullable = true)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |-- conif: string (nullable = true)
        | |    |    |    |    |-- conthen: string (nullable = true)
        | |    |    |    |    |-- amount: double (nullable = true)
        | |    |    |    |    |-- conformedField: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults = "{\"id\":1,\"legs\":[{\"legid\":100,\"conditions\":[{\"conif\":\"if bid>10\",\"conthen\":\"buy\"," +
      "\"amount\":100.0,\"conformedField\":\"IF BID>10\"},{\"conif\":\"if sell<5\",\"conthen\":\"sell\",\"amount\":150.0," +
      "\"conformedField\":\"IF SELL<5\"},{\"conif\":\"if sell<1\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedField\"" +
      ":\"IF SELL<1\"}]},{\"legid\":101,\"conditions\":[{\"conif\":\"if bid<50\",\"conthen\":\"sell\",\"amount\":200.0," +
      "\"conformedField\":\"IF BID<50\"},{\"conif\":\"if sell>30\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedField\"" +
      ":\"IF SELL>30\"},{\"conif\":\"if sell>25\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedField\":\"IF SELL>25\"}]}]}" +
      "\n{\"id\":2,\"legs\":[{\"legid\":102,\"conditions\":[{\"conif\":\"if bid>11\",\"conthen\":\"buy\",\"amount\":100.0," +
      "\"conformedField\":\"IF BID>11\"},{\"conif\":\"if sell<6\",\"conthen\":\"sell\",\"amount\":150.0,\"conformedField\":" +
      "\"IF SELL<6\"},{\"conif\":\"if sell<2\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedField\":\"IF SELL<2\"}]}," +
      "{\"legid\":103,\"conditions\":[{\"conif\":\"if bid<51\",\"conthen\":\"sell\",\"amount\":200.0,\"conformedField\":" +
      "\"IF BID<51\"},{\"conif\":\"if sell>31\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedField\":\"IF SELL>31\"}," +
      "{\"conif\":\"if sell>26\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedField\":\"IF SELL>26\"}]}]}\n{\"id\":3," +
      "\"legs\":[{\"legid\":104,\"conditions\":[{\"conif\":\"if bid>12\",\"conthen\":\"buy\",\"amount\":100.0," +
      "\"conformedField\":\"IF BID>12\"},{\"conif\":\"if sell<7\",\"conthen\":\"sell\",\"amount\":150.0,\"conformedField\":" +
      "\"IF SELL<7\"},{\"conif\":\"if sell<3\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedField\":\"IF SELL<3\"}]}," +
      "{\"legid\":105,\"conditions\":[{\"conif\":\"if bid<52\",\"conthen\":\"sell\",\"amount\":200.0,\"conformedField\":" +
      "\"IF BID<52\"},{\"conif\":\"if sell>32\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedField\":\"IF SELL>32\"}," +
      "{\"conif\":\"if sell>27\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedField\":\"IF SELL>27\"}]}]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test lit() of a plain field") {
    val df = spark.sparkContext.parallelize(plainSampleN).toDF

    val dfOut = df.nestedWithColumn("planet", lit("Earth"))

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- city: string (nullable = true)
        | |-- street: string (nullable = true)
        | |-- planet: string (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"city":"Olomuc","street":"Vodickova","planet":"Earth"}
        |{"city":"Ostrava","street":"Vlavska","planet":"Earth"}
        |{"city":"Plzen","street":"Kralova","planet":"Earth"}""".stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test lit() of two level of struct nesting") {
    // Struct of struct
    val df = spark.sparkContext.parallelize(structOfStructSampleN).toDF

    val dfOut = df.nestedWithColumn("employee.address.conformedType", lit("City"))

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: struct (nullable = false)
        | |    |-- name: string (nullable = true)
        | |    |-- address: struct (nullable = false)
        | |    |    |-- city: string (nullable = true)
        | |    |    |-- street: string (nullable = true)
        | |    |    |-- conformedType: string (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"employee":{"name":"Martin","address":{"city":"Olomuc","street":"Vodickova","conformedType":"City"}}}
        |{"id":1,"employee":{"name":"Petr","address":{"city":"Ostrava","street":"Vlavska","conformedType":"City"}}}
        |{"id":1,"employee":{"name":"Vojta","address":{"city":"Plzen","street":"Kralova","conformedType":"City"}}}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test lit() inside an array of structs") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStructsSampleN).toDF

    val dfOut = df.nestedWithColumn("person.conformedType", lit("Person"))

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- firstName: string (nullable = true)
        | |    |    |-- lastName: string (nullable = true)
        | |    |    |-- conformedType: string (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[{"firstName":"John","lastName":"Smith","conformedType":"Person"},{"firstName":"Jack","lastName":"Brown","conformedType":"Person"}]}
        |{"id":1,"person":[{"firstName":"Merry","lastName":"Cook","conformedType":"Person"},{"firstName":"Jane","lastName":"Clark","conformedType":"Person"}]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test lit() of a field inside an array of structs") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStructsSampleN).toDF

    val dfOut = df.nestedWithColumn("person.department", lit("IT"))

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- firstName: string (nullable = true)
        | |    |    |-- lastName: string (nullable = true)
        | |    |    |-- department: string (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[{"firstName":"John","lastName":"Smith","department":"IT"},{"firstName":"Jack","lastName":"Brown","department":"IT"}]}
        |{"id":1,"person":[{"firstName":"Merry","lastName":"Cook","department":"IT"},{"firstName":"Jane","lastName":"Clark","department":"IT"}]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }


  test("Test lit() inside an array of struct containing an array of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStrtuctsDeepSampleN).toDF

    val dfOut = df.nestedWithColumn("legs.conditions.conformedSystem", lit("Trading"))

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- legid: integer (nullable = true)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |-- conif: string (nullable = true)
        | |    |    |    |    |-- conthen: string (nullable = true)
        | |    |    |    |    |-- amount: double (nullable = true)
        | |    |    |    |    |-- conformedSystem: string (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults = "{\"id\":1,\"legs\":[{\"legid\":100,\"conditions\":[{\"conif\":\"if bid>10\",\"conthen\":\"buy\"," +
      "\"amount\":100.0,\"conformedSystem\":\"Trading\"},{\"conif\":\"if sell<5\",\"conthen\":\"sell\",\"amount\":150.0," +
      "\"conformedSystem\":\"Trading\"},{\"conif\":\"if sell<1\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedSystem\"" +
      ":\"Trading\"}]},{\"legid\":101,\"conditions\":[{\"conif\":\"if bid<50\",\"conthen\":\"sell\",\"amount\":200.0" +
      ",\"conformedSystem\":\"Trading\"},{\"conif\":\"if sell>30\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedSystem\"" +
      ":\"Trading\"},{\"conif\":\"if sell>25\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedSystem\":\"Trading\"}]}]}" +
      "\n{\"id\":2,\"legs\":[{\"legid\":102,\"conditions\":[{\"conif\":\"if bid>11\",\"conthen\":\"buy\",\"amount\":100.0" +
      ",\"conformedSystem\":\"Trading\"},{\"conif\":\"if sell<6\",\"conthen\":\"sell\",\"amount\":150.0,\"conformedSystem\"" +
      ":\"Trading\"},{\"conif\":\"if sell<2\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedSystem\":\"Trading\"}]}," +
      "{\"legid\":103,\"conditions\":[{\"conif\":\"if bid<51\",\"conthen\":\"sell\",\"amount\":200.0,\"conformedSystem\":" +
      "\"Trading\"},{\"conif\":\"if sell>31\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedSystem\":\"Trading\"}," +
      "{\"conif\":\"if sell>26\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedSystem\":\"Trading\"}]}]}\n{\"id\":3," +
      "\"legs\":[{\"legid\":104,\"conditions\":[{\"conif\":\"if bid>12\",\"conthen\":\"buy\",\"amount\":100.0," +
      "\"conformedSystem\":\"Trading\"},{\"conif\":\"if sell<7\",\"conthen\":\"sell\",\"amount\":150.0,\"conformedSystem\"" +
      ":\"Trading\"},{\"conif\":\"if sell<3\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedSystem\":\"Trading\"}]}," +
      "{\"legid\":105,\"conditions\":[{\"conif\":\"if bid<52\",\"conthen\":\"sell\",\"amount\":200.0,\"conformedSystem\":" +
      "\"Trading\"},{\"conif\":\"if sell>32\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedSystem\":\"Trading\"},{\"" +
      "conif\":\"if sell>27\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedSystem\":\"Trading\"}]}]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  // Dropping columns

  test("Test drop of a plain field") {
    val df = spark.sparkContext.parallelize(plainSampleN).toDF

    val dfOut = df.nestedDropColumn("street")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- city: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"city":"Olomuc"}
        |{"city":"Ostrava"}
        |{"city":"Plzen"}""".stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test drop of two level of struct nesting") {
    // Struct of struct
    val df = spark.sparkContext.parallelize(structOfStructSampleN).toDF

    val dfOut = df.nestedDropColumn("employee.address.city")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: struct (nullable = false)
        | |    |-- name: string (nullable = true)
        | |    |-- address: struct (nullable = false)
        | |    |    |-- street: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"employee":{"name":"Martin","address":{"street":"Vodickova"}}}
        |{"id":1,"employee":{"name":"Petr","address":{"street":"Vlavska"}}}
        |{"id":1,"employee":{"name":"Vojta","address":{"street":"Kralova"}}}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test drop of arrays of primitives") {
    // Array of primitives
    val df = spark.sparkContext.parallelize(arraysOfPrimitivesSampleN).toDF

    val dfOut = df.nestedDropColumn("words")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1}
        |{"id":1}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }


  test("Test drop of arrays of arrays of primitives") {
    // Array of arrays of primitives
    val df = spark.sparkContext.parallelize(arraysOfArraysOfPrimitivesSampleN).toDF

    val dfOut = df.nestedDropColumn("matrix")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1}
        |{"id":2}
        |{"id":3}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test drop of a field inside an array of structs") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStructsSampleN).toDF

    val dfOut = df.nestedDropColumn("person.lastName")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- firstName: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[{"firstName":"John"},{"firstName":"Jack"}]}
        |{"id":1,"person":[{"firstName":"Merry"},{"firstName":"Jane"}]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test drop of a field inside an array of arrays of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfArraysOfStructSampleN).toDF

    val dfOut = df.nestedDropColumn("person.lastName")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: struct (containsNull = false)
        | |    |    |    |-- firstName: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[[{"firstName":"Mona Lisa"}],[{"firstName":"Lenny"},{"firstName":"Dot"}]]}
        |{"id":1,"person":[[{"firstName":"Eddie"}],[{"firstName":"Scarlett"},{"firstName":"William"}]]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test drop of a field inside an array of struct containing an array of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStrtuctsDeepSampleN).toDF

    val dfOut = df.nestedDropColumn("legs.conditions.conif")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- legid: integer (nullable = true)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |-- conthen: string (nullable = true)
        | |    |    |    |    |-- amount: double (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults = "{\"id\":1,\"legs\":[{\"legid\":100,\"conditions\":[{\"conthen\":\"buy\",\"amount\":100.0}," +
      "{\"conthen\":\"sell\",\"amount\":150.0},{\"conthen\":\"sell\",\"amount\":1000.0}]},{\"legid\":101,\"conditions\":" +
      "[{\"conthen\":\"sell\",\"amount\":200.0},{\"conthen\":\"buy\",\"amount\":175.0},{\"conthen\":\"buy\",\"amount\":" +
      "225.0}]}]}\n{\"id\":2,\"legs\":[{\"legid\":102,\"conditions\":[{\"conthen\":\"buy\",\"amount\":100.0},{\"conthen\":" +
      "\"sell\",\"amount\":150.0},{\"conthen\":\"sell\",\"amount\":1000.0}]},{\"legid\":103,\"conditions\":[{\"conthen\":" +
      "\"sell\",\"amount\":200.0},{\"conthen\":\"buy\",\"amount\":175.0},{\"conthen\":\"buy\",\"amount\":225.0}]}]}\n{" +
      "\"id\":3,\"legs\":[{\"legid\":104,\"conditions\":[{\"conthen\":\"buy\",\"amount\":100.0},{\"conthen\":\"sell\"," +
      "\"amount\":150.0},{\"conthen\":\"sell\",\"amount\":1000.0}]},{\"legid\":105,\"conditions\":[{\"conthen\":\"sell\"," +
      "\"amount\":200.0},{\"conthen\":\"buy\",\"amount\":175.0},{\"conthen\":\"buy\",\"amount\":225.0}]}]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test drop of a struct field inside an array of struct containing an array of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arrayOfstructOfStructSampleN).toDF

    val dfOut = df.nestedDropColumn("employee.address")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- name: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"employee":[{"name":"Martin"},{"name":"Stephan"}]}
        |{"id":2,"employee":[{"name":"Petr"},{"name":"Michal"}]}
        |{"id":3,"employee":[{"name":"Vojta"}]}""".stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test drop of an array field inside an array of struct containing an array of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStrtuctsDeepSampleN).toDF

    val dfOut = df.nestedDropColumn("legs.conditions")

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- legid: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"legs":[{"legid":100},{"legid":101}]}
        |{"id":2,"legs":[{"legid":102},{"legid":103}]}
        |{"id":3,"legs":[{"legid":104},{"legid":105}]}""".stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test concat of a plain field") {
    val df = spark.sparkContext.parallelize(plainSampleN).toDF

    val dfOut = df.nestedMapColumn("", "combinedCity", c => {
      if (c == null) {
        concat(col("city"), col("street"))
      } else {
        concat(c.getField("city"), c.getField("street"))
      }
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- city: string (nullable = true)
        | |-- street: string (nullable = true)
        | |-- combinedCity: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"city":"Olomuc","street":"Vodickova","combinedCity":"OlomucVodickova"}
        |{"city":"Ostrava","street":"Vlavska","combinedCity":"OstravaVlavska"}
        |{"city":"Plzen","street":"Kralova","combinedCity":"PlzenKralova"}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test concat of two level of struct nesting") {
    // Struct of struct
    val df = spark.sparkContext.parallelize(structOfStructSampleN).toDF

    val dfOut = df.nestedMapStruct("employee.address", "combinedCity", c => {
      if (c == null) {
        concat(col("city"), col("street"))
      } else {
        concat(c.getField("city"), c.getField("street"))
      }
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: struct (nullable = false)
        | |    |-- name: string (nullable = true)
        | |    |-- address: struct (nullable = false)
        | |    |    |-- city: string (nullable = true)
        | |    |    |-- street: string (nullable = true)
        | |    |    |-- combinedCity: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"employee":{"name":"Martin","address":{"city":"Olomuc","street":"Vodickova","combinedCity":"OlomucVodickova"}}}
        |{"id":1,"employee":{"name":"Petr","address":{"city":"Ostrava","street":"Vlavska","combinedCity":"OstravaVlavska"}}}
        |{"id":1,"employee":{"name":"Vojta","address":{"city":"Plzen","street":"Kralova","combinedCity":"PlzenKralova"}}}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test concat of a field inside an array of structs") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStructsSampleN).toDF

    val dfOut = df.nestedMapStruct("person", "combinedName", c => {
      concat(c.getField("firstName"), lit(" "), c.getField("lastName"))
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- firstName: string (nullable = true)
        | |    |    |-- lastName: string (nullable = true)
        | |    |    |-- combinedName: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[{"firstName":"John","lastName":"Smith","combinedName":"John Smith"},{"firstName":"Jack","lastName":"Brown","combinedName":"Jack Brown"}]}
        |{"id":1,"person":[{"firstName":"Merry","lastName":"Cook","combinedName":"Merry Cook"},{"firstName":"Jane","lastName":"Clark","combinedName":"Jane Clark"}]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test concat of a field inside an array of arrays of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfArraysOfStructSampleN).toDF

    val dfOut = df.nestedMapStruct("person", "combinedName", c => {
      concat(c.getField("firstName"), lit(" "), c.getField("lastName"))
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- person: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: struct (containsNull = false)
        | |    |    |    |-- firstName: string (nullable = true)
        | |    |    |    |-- lastName: string (nullable = true)
        | |    |    |    |-- combinedName: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"person":[[{"firstName":"Mona Lisa","lastName":"Harddrive","combinedName":"Mona Lisa Harddrive"}],[{"firstName":"Lenny","lastName":"Linux","combinedName":"Lenny Linux"},{"firstName":"Dot","lastName":"Not","combinedName":"Dot Not"}]]}
        |{"id":1,"person":[[{"firstName":"Eddie","lastName":"Larrison","combinedName":"Eddie Larrison"}],[{"firstName":"Scarlett","lastName":"Johanson","combinedName":"Scarlett Johanson"},{"firstName":"William","lastName":"Windows","combinedName":"William Windows"}]]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test concat of a field inside an array of struct containing an array of struct") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStrtuctsDeepSampleN).toDF

    val dfOut = df.nestedMapStruct("legs.conditions", "combinedField", c => {
      concat(c.getField("conif"), lit(" "), c.getField("conthen"), lit(" ("), c.getField("amount").cast(StringType), lit(")"))
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- legid: integer (nullable = true)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |-- conif: string (nullable = true)
        | |    |    |    |    |-- conthen: string (nullable = true)
        | |    |    |    |    |-- amount: double (nullable = true)
        | |    |    |    |    |-- combinedField: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"legs":[{"legid":100,"conditions":[{"conif":"if bid>10","conthen":"buy","amount":100.0,"combinedField":"if bid>10 buy (100.0)"},{"conif":"if sell<5","conthen":"sell","amount":150.0,"combinedField":"if sell<5 sell (150.0)"},{"conif":"if sell<1","conthen":"sell","amount":1000.0,"combinedField":"if sell<1 sell (1000.0)"}]},{"legid":101,"conditions":[{"conif":"if bid<50","conthen":"sell","amount":200.0,"combinedField":"if bid<50 sell (200.0)"},{"conif":"if sell>30","conthen":"buy","amount":175.0,"combinedField":"if sell>30 buy (175.0)"},{"conif":"if sell>25","conthen":"buy","amount":225.0,"combinedField":"if sell>25 buy (225.0)"}]}]}
        |{"id":2,"legs":[{"legid":102,"conditions":[{"conif":"if bid>11","conthen":"buy","amount":100.0,"combinedField":"if bid>11 buy (100.0)"},{"conif":"if sell<6","conthen":"sell","amount":150.0,"combinedField":"if sell<6 sell (150.0)"},{"conif":"if sell<2","conthen":"sell","amount":1000.0,"combinedField":"if sell<2 sell (1000.0)"}]},{"legid":103,"conditions":[{"conif":"if bid<51","conthen":"sell","amount":200.0,"combinedField":"if bid<51 sell (200.0)"},{"conif":"if sell>31","conthen":"buy","amount":175.0,"combinedField":"if sell>31 buy (175.0)"},{"conif":"if sell>26","conthen":"buy","amount":225.0,"combinedField":"if sell>26 buy (225.0)"}]}]}
        |{"id":3,"legs":[{"legid":104,"conditions":[{"conif":"if bid>12","conthen":"buy","amount":100.0,"combinedField":"if bid>12 buy (100.0)"},{"conif":"if sell<7","conthen":"sell","amount":150.0,"combinedField":"if sell<7 sell (150.0)"},{"conif":"if sell<3","conthen":"sell","amount":1000.0,"combinedField":"if sell<3 sell (1000.0)"}]},{"legid":105,"conditions":[{"conif":"if bid<52","conthen":"sell","amount":200.0,"combinedField":"if bid<52 sell (200.0)"},{"conif":"if sell>32","conthen":"buy","amount":175.0,"combinedField":"if sell>32 buy (175.0)"},{"conif":"if sell>27","conthen":"buy","amount":225.0,"combinedField":"if sell>27 buy (225.0)"}]}]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test deep mapping a field into a field with the same name") {
    // Array of struct
    val df = spark.sparkContext.parallelize(arraysOfStrtuctsDeepSampleN).toDF

    val dfOut = df.nestedMapColumn("legs.legid", "legs.legid", _ => {
      lit("1")
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- legid: string (nullable = false)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- conif: string (nullable = true)
        | |    |    |    |    |-- conthen: string (nullable = true)
        | |    |    |    |    |-- amount: double (nullable = false)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """{"id":1,"legs":[{"legid":"1","conditions":[{"conif":"if bid>10","conthen":"buy","amount":100.0},{"conif":"if sell<5","conthen":"sell","amount":150.0},{"conif":"if sell<1","conthen":"sell","amount":1000.0}]},{"legid":"1","conditions":[{"conif":"if bid<50","conthen":"sell","amount":200.0},{"conif":"if sell>30","conthen":"buy","amount":175.0},{"conif":"if sell>25","conthen":"buy","amount":225.0}]}]}
        |{"id":2,"legs":[{"legid":"1","conditions":[{"conif":"if bid>11","conthen":"buy","amount":100.0},{"conif":"if sell<6","conthen":"sell","amount":150.0},{"conif":"if sell<2","conthen":"sell","amount":1000.0}]},{"legid":"1","conditions":[{"conif":"if bid<51","conthen":"sell","amount":200.0},{"conif":"if sell>31","conthen":"buy","amount":175.0},{"conif":"if sell>26","conthen":"buy","amount":225.0}]}]}
        |{"id":3,"legs":[{"legid":"1","conditions":[{"conif":"if bid>12","conthen":"buy","amount":100.0},{"conif":"if sell<7","conthen":"sell","amount":150.0},{"conif":"if sell<3","conthen":"sell","amount":1000.0}]},{"legid":"1","conditions":[{"conif":"if bid<52","conthen":"sell","amount":200.0},{"conif":"if sell>32","conthen":"buy","amount":175.0},{"conif":"if sell>27","conthen":"buy","amount":225.0}]}]}"""
        .stripMargin.replace("\r\n", "\n")

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  private def assertSchema(actualSchema: String, expectedSchema: String): Unit = {
    if (actualSchema != expectedSchema) {
      log.error("EXPECTED:")
      log.error(expectedSchema)
      log.error("ACTUAL:")
      log.error(actualSchema)
      fail("Actual conformed schema does not match the expected schema (see above).")
    }
  }

  private def assertResults(actualResults: String, expectedResults: String): Unit = {
    if (actualResults != expectedResults) {
      log.error("EXPECTED:")
      log.error(expectedResults)
      log.error("ACTUAL:")
      log.error(actualResults)
      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }
}

