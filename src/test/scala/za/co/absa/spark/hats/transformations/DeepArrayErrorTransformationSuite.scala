/*
 * Copyright 2020 ABSA Group Limited
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import za.co.absa.spark.hats.SparkTestBase
import za.co.absa.spark.hats.transformations.samples.DeepArraySamples._
import za.co.absa.spark.hats.transformations.samples.SampleErrorUDFs
import za.co.absa.spark.hats.utils.JsonUtils

class DeepArrayErrorTransformationSuite extends FunSuite with SparkTestBase {
  // scalastyle:off line.size.limit
  // scalastyle:off null

  import spark.implicits._
  import za.co.absa.spark.hats.Extensions._
  implicit val _: SampleErrorUDFs = new SampleErrorUDFs

  private val log = LoggerFactory.getLogger(this.getClass)

  test("Test casting of a plain field with error column") {
    val df = spark.sparkContext.parallelize(plainSampleE).toDF

    val expectedSchema =
      """root
        | |-- city: string (nullable = true)
        | |-- street: string (nullable = true)
        | |-- buildingNum: integer (nullable = false)
        | |-- zip: string (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        | |-- intZip: integer (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "city" : "Olomuc",
        |  "street" : "Vodickova",
        |  "buildingNum" : 12,
        |  "zip" : "12000",
        |  "errors" : [ ],
        |  "intZip" : 12000
        |}, {
        |  "city" : "Ostrava",
        |  "street" : "Vlavska",
        |  "buildingNum" : 110,
        |  "zip" : "1455a",
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intZip",
        |    "rawValues" : [ "1455a" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "city" : "Plzen",
        |  "street" : "Kralova",
        |  "buildingNum" : 71,
        |  "zip" : "b881",
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intZip",
        |    "rawValues" : [ "b881" ],
        |    "mappings" : [ ]
        |  } ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "zip", "intZip", expectedSchema, expectedResults)
  }

  test("Test casting of a struct of struct field with error column") {
    val df = spark.sparkContext.parallelize(structOfStructSampleE).toDF

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: struct (nullable = false)
        | |    |-- name: string (nullable = true)
        | |    |-- address: struct (nullable = false)
        | |    |    |-- city: string (nullable = true)
        | |    |    |-- street: string (nullable = true)
        | |    |    |-- buildingNum: integer (nullable = true)
        | |    |    |-- zip: string (nullable = true)
        | |    |    |-- intZip: integer (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "employee" : {
        |    "name" : "Martin",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 12,
        |      "zip" : "12000",
        |      "intZip" : 12000
        |    }
        |  },
        |  "errors" : [ ]
        |}, {
        |  "id" : 1,
        |  "employee" : {
        |    "name" : "Petr",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 110,
        |      "zip" : "1455a"
        |    }
        |  },
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "1455a" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 1,
        |  "employee" : {
        |    "name" : "Vojta",
        |    "address" : {
        |      "city" : "Plzen",
        |      "street" : "Kralova",
        |      "buildingNum" : 71,
        |      "zip" : "b881"
        |    }
        |  },
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "b881" ],
        |    "mappings" : [ ]
        |  } ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "employee.address.zip", "employee.address.intZip",
      expectedSchema, expectedResults)
  }

  test("Test casting of an array of struct of struct with error column") {
    val df = spark.sparkContext.parallelize(arrayOfStructOfStructErrSampleE).toDF

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- name: string (nullable = true)
        | |    |    |-- address: struct (nullable = false)
        | |    |    |    |-- city: string (nullable = true)
        | |    |    |    |-- street: string (nullable = true)
        | |    |    |    |-- buildingNum: integer (nullable = true)
        | |    |    |    |-- zip: string (nullable = true)
        | |    |    |    |-- intZip: integer (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "employee" : [ {
        |    "name" : "Martin",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 732,
        |      "zip" : "73200",
        |      "intZip" : 73200
        |    }
        |  }, {
        |    "name" : "Stephan",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 77,
        |      "zip" : "77-333"
        |    }
        |  } ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "77-333" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 2,
        |  "employee" : [ {
        |    "name" : "Petr",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 25,
        |      "zip" : "a9991"
        |    }
        |  }, {
        |    "name" : "Michal",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 334,
        |      "zip" : "552-aa1"
        |    }
        |  } ],
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "a9991" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "552-aa1" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 3,
        |  "employee" : [ {
        |    "name" : "Vojta",
        |    "address" : {
        |      "city" : "Plzen",
        |      "street" : "Kralova",
        |      "buildingNum" : 33,
        |      "zip" : "993",
        |      "intZip" : 993
        |    }
        |  } ],
        |  "errors" : [ ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "employee.address.zip", "employee.address.intZip",
      expectedSchema, expectedResults)
  }

  test("Test casting of an array of primitives") {
    val df = spark.sparkContext.parallelize(arraysOfPrimitivesSampleE).toDF

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- nums: array (nullable = true)
        | |    |-- element: string (containsNull = true)
        | |-- intNums: array (nullable = true)
        | |    |-- element: integer (containsNull = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "nums" : [ "7755", "a212", "222-111" ],
        |  "intNums" : [ 7755, null, null ],
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intNums",
        |    "rawValues" : [ "a212" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intNums",
        |    "rawValues" : [ "222-111" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 1,
        |  "nums" : [ "223a", "223a", "775" ],
        |  "intNums" : [ null, null, 775 ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intNums",
        |    "rawValues" : [ "223a" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 1,
        |  "nums" : [ "5", "-100", "9999999" ],
        |  "intNums" : [ 5, -100, 9999999 ],
        |  "errors" : [ ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "nums", "intNums", expectedSchema, expectedResults)
  }

  test("Test casting of an array of array of primitives") {
    val df = spark.sparkContext.parallelize(arraysOfArraysOfPrimitivesSampleE).toDF

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- matrix: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: string (containsNull = true)
        | |-- intMatrix: array (nullable = true)
        | |    |-- element: array (containsNull = true)
        | |    |    |-- element: integer (containsNull = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "matrix" : [ [ "10", "11b" ], [ "11b", "12" ] ],
        |  "intMatrix" : [ [ 10, null ], [ null, 12 ] ],
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intMatrix",
        |    "rawValues" : [ "11b" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 2,
        |  "matrix" : [ [ "20f", "300" ], [ "1000", "10-10" ] ],
        |  "intMatrix" : [ [ null, 300 ], [ 1000, null ] ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intMatrix",
        |    "rawValues" : [ "20f" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "intMatrix",
        |    "rawValues" : [ "10-10" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 3,
        |  "matrix" : [ [ "775", "223" ], [ "100", "0" ] ],
        |  "intMatrix" : [ [ 775, 223 ], [ 100, 0 ] ],
        |  "errors" : [ ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "matrix", "intMatrix", expectedSchema, expectedResults)
  }

  test("Test casting of an array of struct of array of struct with error column") {
    val df = spark.sparkContext.parallelize(arraysOfStrtuctsDeepSampleE).toDF

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
        | |    |    |    |    |-- intConditionVal: integer (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "legs" : [ {
        |    "legid" : 100,
        |    "conditions" : [ {
        |      "conif" : "if bid>10",
        |      "conthen" : "100",
        |      "amount" : 100.0,
        |      "intConditionVal" : 100
        |    }, {
        |      "conif" : "if sell<5",
        |      "conthen" : "300a",
        |      "amount" : 150.0
        |    }, {
        |      "conif" : "if sell<1",
        |      "conthen" : "1000",
        |      "amount" : 1000.0,
        |      "intConditionVal" : 1000
        |    } ]
        |  }, {
        |    "legid" : 101,
        |    "conditions" : [ {
        |      "conif" : "if bid<50",
        |      "conthen" : "200",
        |      "amount" : 200.0,
        |      "intConditionVal" : 200
        |    }, {
        |      "conif" : "if sell>30",
        |      "conthen" : "175b",
        |      "amount" : 175.0
        |    }, {
        |      "conif" : "if sell>25",
        |      "conthen" : "225-225",
        |      "amount" : 225.0
        |    } ]
        |  } ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "300a" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "175b" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "225-225" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 2,
        |  "legs" : [ {
        |    "legid" : 102,
        |    "conditions" : [ {
        |      "conif" : "if bid>11",
        |      "conthen" : "100",
        |      "amount" : 100.0,
        |      "intConditionVal" : 100
        |    }, {
        |      "conif" : "if sell<6",
        |      "conthen" : "150",
        |      "amount" : 150.0,
        |      "intConditionVal" : 150
        |    }, {
        |      "conif" : "if sell<2",
        |      "conthen" : "1000",
        |      "amount" : 1000.0,
        |      "intConditionVal" : 1000
        |    } ]
        |  }, {
        |    "legid" : 103,
        |    "conditions" : [ {
        |      "conif" : "if bid<51",
        |      "conthen" : "200",
        |      "amount" : 200.0,
        |      "intConditionVal" : 200
        |    }, {
        |      "conif" : "if sell>31",
        |      "conthen" : "175",
        |      "amount" : 175.0,
        |      "intConditionVal" : 175
        |    }, {
        |      "conif" : "if sell>26",
        |      "conthen" : "225",
        |      "amount" : 225.0,
        |      "intConditionVal" : 225
        |    } ]
        |  } ],
        |  "errors" : [ ]
        |}, {
        |  "id" : 3,
        |  "legs" : [ {
        |    "legid" : 104,
        |    "conditions" : [ {
        |      "conif" : "if bid>12",
        |      "conthen" : "1OO",
        |      "amount" : 100.0
        |    }, {
        |      "conif" : "if sell<7",
        |      "conthen" : "150x",
        |      "amount" : 150.0
        |    }, {
        |      "conif" : "if sell<3",
        |      "conthen" : "-1000-",
        |      "amount" : 1000.0
        |    } ]
        |  }, {
        |    "legid" : 105,
        |    "conditions" : [ {
        |      "conif" : "if bid<52",
        |      "conthen" : "2OO",
        |      "amount" : 200.0
        |    }, {
        |      "conif" : "if sell>32",
        |      "conthen" : "f175",
        |      "amount" : 175.0
        |    }, {
        |      "conif" : "if sell>27",
        |      "conthen" : "225_",
        |      "amount" : 225.0
        |    } ]
        |  } ],
        |  "errors" : [ {
        |    "errType" : "myErrorType",
        |    "errCode" : "E-1",
        |    "errMsg" : "Testing This stuff",
        |    "errCol" : "whatEvColumn",
        |    "rawValues" : [ "some value" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "1OO" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "150x" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "-1000-" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "2OO" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "f175" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.intConditionVal",
        |    "rawValues" : [ "225_" ],
        |    "mappings" : [ ]
        |  } ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "legs.conditions.conthen", "legs.conditions.intConditionVal",
      expectedSchema, expectedResults)
  }

  test("Test casting of an array of struct of struct WITHOUT error column") {
    val df = spark.sparkContext.parallelize(arrayOfStructOfStruvtNoErrSampleE).toDF

    val expectedSchema =
      """root
        | |-- id: integer (nullable = false)
        | |-- employee: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- name: string (nullable = true)
        | |    |    |-- address: struct (nullable = false)
        | |    |    |    |-- city: string (nullable = true)
        | |    |    |    |-- street: string (nullable = true)
        | |    |    |    |-- buildingNum: integer (nullable = true)
        | |    |    |    |-- zip: string (nullable = true)
        | |    |    |    |-- intZip: integer (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "employee" : [ {
        |    "name" : "Martin",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 732,
        |      "zip" : "73200",
        |      "intZip" : 73200
        |    }
        |  }, {
        |    "name" : "Stephan",
        |    "address" : {
        |      "city" : "Olomuc",
        |      "street" : "Vodickova",
        |      "buildingNum" : 77,
        |      "zip" : "77-333"
        |    }
        |  } ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "77-333" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 2,
        |  "employee" : [ {
        |    "name" : "Petr",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 25,
        |      "zip" : "a9991"
        |    }
        |  }, {
        |    "name" : "Michal",
        |    "address" : {
        |      "city" : "Ostrava",
        |      "street" : "Vlavska",
        |      "buildingNum" : 334,
        |      "zip" : "552-aa1"
        |    }
        |  } ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "a9991" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "employee.address.intZip",
        |    "rawValues" : [ "552-aa1" ],
        |    "mappings" : [ ]
        |  } ]
        |}, {
        |  "id" : 3,
        |  "employee" : [ {
        |    "name" : "Vojta",
        |    "address" : {
        |      "city" : "Plzen",
        |      "street" : "Kralova",
        |      "buildingNum" : 33,
        |      "zip" : "993",
        |      "intZip" : 993
        |    }
        |  } ],
        |  "errors" : [ ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "employee.address.zip", "employee.address.intZip",
      expectedSchema, expectedResults)
  }

  test ("Test multiple levels of nesting") {

    val sample = """[{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}]}]"""

    val df = JsonUtils.getDataFrameFromJson(spark, Seq(sample))

    val expectedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |-- amount: long (nullable = true)
        | |    |    |    |    |-- checks: array (nullable = true)
        | |    |    |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |    |    |-- checkNums: array (nullable = true)
        | |    |    |    |    |    |    |    |-- element: string (containsNull = true)
        | |    |    |    |    |    |    |-- optimizedNums: array (nullable = true)
        | |    |    |    |    |    |    |    |-- element: integer (containsNull = true)
        | |    |    |-- legid: long (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")
    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "legs" : [ {
        |    "conditions" : [ {
        |      "amount" : 100,
        |      "checks" : [ {
        |        "checkNums" : [ "1", "2", "3b", "4", "5c", "6" ],
        |        "optimizedNums" : [ 1, 2, null, 4, null, 6 ]
        |      } ]
        |    } ],
        |    "legid" : 100
        |  } ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.checks.optimizedNums",
        |    "rawValues" : [ "3b" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.checks.optimizedNums",
        |    "rawValues" : [ "5c" ],
        |    "mappings" : [ ]
        |  } ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    processCastExample(df, "legs.conditions.checks.checkNums", "legs.conditions.checks.optimizedNums",
      expectedSchema, expectedResults)
  }

  test ("Test combining fields on multiple levels of nesting") {

    val sample = """[{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"amount":100}]}]}]"""

    val df = JsonUtils.getDataFrameFromJson(spark, Seq(sample))

    val expectedSchema =
      """root
        | |-- id: long (nullable = true)
        | |-- legs: array (nullable = true)
        | |    |-- element: struct (containsNull = false)
        | |    |    |-- conditions: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |-- amount: long (nullable = true)
        | |    |    |    |    |-- checks: array (nullable = true)
        | |    |    |    |    |    |-- element: struct (containsNull = false)
        | |    |    |    |    |    |    |-- checkNums: array (nullable = true)
        | |    |    |    |    |    |    |    |-- element: string (containsNull = true)
        | |    |    |    |    |    |    |-- optimizedNums: array (nullable = true)
        | |    |    |    |    |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- legid: long (nullable = true)
        | |-- errors: array (nullable = true)
        | |    |-- element: struct (containsNull = true)
        | |    |    |-- errType: string (nullable = true)
        | |    |    |-- errCode: string (nullable = true)
        | |    |    |-- errMsg: string (nullable = true)
        | |    |    |-- errCol: string (nullable = true)
        | |    |    |-- rawValues: array (nullable = true)
        | |    |    |    |-- element: string (containsNull = true)
        | |    |    |-- mappings: array (nullable = true)
        | |    |    |    |-- element: struct (containsNull = true)
        | |    |    |    |    |-- mappingTableColumn: string (nullable = true)
        | |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)
        |""".stripMargin.replace("\r\n", "\n")

    val expectedResults =
      """[ {
        |  "id" : 1,
        |  "legs" : [ {
        |    "conditions" : [ {
        |      "amount" : 100,
        |      "checks" : [ {
        |        "checkNums" : [ "1", "2", "3b", "4", "5c", "6" ],
        |        "optimizedNums" : [ "1_100_1", "2_100_1", "3b_100_1", "4_100_1", "5c_100_1", "6_100_1" ]
        |      } ]
        |    } ],
        |    "legid" : 100
        |  } ],
        |  "errors" : [ {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.checks.optimizedNums",
        |    "rawValues" : [ "3b" ],
        |    "mappings" : [ ]
        |  }, {
        |    "errType" : "confCastError",
        |    "errCode" : "E00003",
        |    "errMsg" : "Conformance Error - Null returned by casting conformance rule",
        |    "errCol" : "legs.conditions.checks.optimizedNums",
        |    "rawValues" : [ "5c" ],
        |    "mappings" : [ ]
        |  } ]
        |} ]"""
        .stripMargin.replace("\r\n", "\n")

    val inputColumn = "legs.conditions.checks.checkNums"
    val outputColumn = "legs.conditions.checks.optimizedNums"
    val dfOut = NestedArrayTransformations.nestedExtendedWithColumnAndErrorMap(df, inputColumn, outputColumn, "errors",
      (_, gf) => {
        concat(gf(inputColumn),
          lit("_"),
          gf("legs.conditions.amount").cast(StringType),
          lit("_"),
          gf("id"))
      }, (c, gf) => {
        when(c.isNotNull.and(c.cast(IntegerType).isNull)
          .and(gf("legs.conditions.amount") === 100)
          .and(gf("legs.legid") === 100)
          .and(gf("id") === 1),
          callUDF("confCastErr", lit(outputColumn), gf(inputColumn).cast(StringType)))
          .otherwise(null)
      })

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.toJSON.collect)

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test ("Test deep array transformations unhappy paths") {
    val df = spark.sparkContext.parallelize(Seq(1,2,3,4,5)).toDF()

    assert(intercept[IllegalArgumentException] {
      NestedArrayTransformations.nestedWithColumnAndErrorMap(df, "value", "value2", "err.errors", c => c, e => e)
    }.getMessage contains "Error columns should be at the root schema level")

    assert(intercept[IllegalArgumentException] {
      NestedArrayTransformations.nestedWithColumnMap(df, "value.foo", "value.foo2", c => c)
    }.getMessage contains "Field 'value' is not a struct type or an array")

    assert(intercept[IllegalArgumentException] {
      NestedArrayTransformations.nestedWithColumnMap(df, "value", "", _ => lit("foo")).printSchema()
    }.getMessage contains "Output field cannot be empty")

    assert(intercept[IllegalArgumentException] {
      df.nestedWithColumn("value", lit("foo")).printSchema()
    }.getMessage contains "The column 'value' already exists")

  }

  test("Test array_distinct() from Spark API (didn't work in 2.4.0, fixed in 2.4.1)"){
    val sourceData =
      """{
        |  "id": 3,
        |  "MyLiteral": "abcdef",
        |  "MyUpperLiteral": "ABCDEF",
        |  "errCol": [
        |    {
        |      "errType": "confMapError",
        |      "errCode": "E00001",
        |      "errMsg": "Conformance Error - Null produced by mapping conformance rule",
        |      "errCol": "legs.conditions.conformed_country",
        |      "rawValues": [
        |        "SWE"
        |      ],
        |      "mappings": [
        |        {
        |          "mappingTableColumn": "country_code",
        |          "mappedDatasetColumn": "legs.conditions.country"
        |        }
        |      ]
        |    },
        |    {
        |      "errType": "confMapError",
        |      "errCode": "E00001",
        |      "errMsg": "Conformance Error - Null produced by mapping conformance rule",
        |      "errCol": "legs.conditions.conformed_country",
        |      "rawValues": [
        |        "SWE"
        |      ],
        |      "mappings": [
        |        {
        |          "mappingTableColumn": "country_code",
        |          "mappedDatasetColumn": "legs.conditions.country"
        |        }
        |      ]
        |    },
        |    {
        |      "errType": "confMapError",
        |      "errCode": "E00001",
        |      "errMsg": "Conformance Error - Null produced by mapping conformance rule",
        |      "errCol": "legs.conditions.conformed_currency",
        |      "rawValues": [
        |        "Dummy"
        |      ],
        |      "mappings": [
        |        {
        |          "mappingTableColumn": "currency_code",
        |          "mappedDatasetColumn": "legs.conditions.currency"
        |        }
        |      ]
        |    }
        |  ],
        |  "legs": [
        |    {
        |      "conditions": [
        |        {
        |          "checks": [],
        |          "country": "SWE",
        |          "currency": "SWK",
        |          "product": "Stock",
        |          "conformed_currency": "SEK",
        |          "conformed_product": "STK"
        |        }
        |      ],
        |      "legid": 300
        |    },
        |    {
        |      "conditions": [
        |        {
        |          "checks": [],
        |          "country": "SA",
        |          "currency": "Dummy",
        |          "product": "Bond",
        |          "conformed_country": "South Africa",
        |          "conformed_currency": "Unknown",
        |          "conformed_product": "BND"
        |        }
        |      ],
        |      "legid": 301
        |    }
        |  ]
        |}""".stripMargin

    val expectedDistinct =
      """{
        |  "MyLiteral" : "abcdef",
        |  "errCol" : [ {
        |    "errCode" : "E00001",
        |    "errCol" : "legs.conditions.conformed_country",
        |    "errMsg" : "Conformance Error - Null produced by mapping conformance rule",
        |    "errType" : "confMapError",
        |    "mappings" : [ {
        |      "mappedDatasetColumn" : "legs.conditions.country",
        |      "mappingTableColumn" : "country_code"
        |    } ],
        |    "rawValues" : [ "SWE" ]
        |  }, {
        |    "errCode" : "E00001",
        |    "errCol" : "legs.conditions.conformed_currency",
        |    "errMsg" : "Conformance Error - Null produced by mapping conformance rule",
        |    "errType" : "confMapError",
        |    "mappings" : [ {
        |      "mappedDatasetColumn" : "legs.conditions.currency",
        |      "mappingTableColumn" : "currency_code"
        |    } ],
        |    "rawValues" : [ "Dummy" ]
        |  } ]
        |}""".stripMargin.replace("\r\n", "\n")

    val df = JsonUtils.getDataFrameFromJson(spark, Seq(sourceData))

    val dfDistinct = df.select(col("MyLiteral"), array_distinct(col("errCol")).as("errCol"))

    val actualDistinct = JsonUtils.prettyJSON(dfDistinct.toJSON.take(1)(0))

    assert(actualDistinct == expectedDistinct)
  }

  private def processCastExample(df: DataFrame, inputColumn: String, outputColumn: String, expectedSchema: String,
                                 expectedResults: String): Unit = {
    val dfOut = NestedArrayTransformations.nestedWithColumnAndErrorMap(df, inputColumn, outputColumn, "errors",
      c => {
        c.cast(IntegerType)
      }, c => {
        when(c.isNotNull.and(c.cast(IntegerType).isNull),
          callUDF("confCastErr", lit(outputColumn), c.cast(StringType)))
          .otherwise(null)
      })

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.toJSON.collect)

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

