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

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import za.co.absa.spark.hats.SparkTestBase
import za.co.absa.spark.hats.transformations.samples.{ErrorMessage, NestedTestCaseFactory, SampleErrorUDFs}
import za.co.absa.spark.hats.utils.JsonUtils

class ExtendedTransformationsSuite extends FunSuite with SparkTestBase {
  implicit val _: SampleErrorUDFs = new SampleErrorUDFs

  private val log = LoggerFactory.getLogger(this.getClass)
  private val nestedTestCaseFactory = new NestedTestCaseFactory()

  test("Test extended array transformations work on root level fields") {
    val expectedSchema = getResourceString("/test_data/nested/nested1Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested1Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructMap(df, "", "id_str", (_, gf) =>
      concat(gf("id"), lit(" "), gf("key1").cast(StringType), lit(" "), gf("key2"))
    )

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations work on an inner struct level fields") {
    val expectedSchema = getResourceString("/test_data/nested/nested2Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested2Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructMap(df, "struct2", "skey2", (c, gf) =>
      concat(gf("key1"), lit(" "), gf("struct2.inner1.key5").cast(StringType), lit(" "),
        c.getField("inner1").getField("key6"))
    ).select("key1", "struct2")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations work on a double nested inner struct level fields") {
    val expectedSchema = getResourceString("/test_data/nested/nested3Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested3Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructMap(df, "struct2.inner1", "skey2", (c, gf) =>
      concat(gf("key1"), lit(" "), gf("struct2.inner1.key5").cast(StringType), lit(" "), c.getField("key6"))
    ).select("key1", "struct2")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations work on a nested struct in an array") {
    val expectedSchema = getResourceString("/test_data/nested/nested4Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested4Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructMap(df, "array1", "skey3", (c, gf) =>
      concat(gf("key1"), lit(" "), gf("array1.key7").cast(StringType), lit(" "), c.getField("key8"))
    ).select("key1", "array1")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations work on a nested struct in an array of an array") {
    val expectedSchema = getResourceString("/test_data/nested/nested5Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested5Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructMap(df, "array2.inner2", "out", (c, gf) =>
      concat(gf("key1"),
        lit(" "),
        gf("array2.key2").cast(StringType),
        lit(" "),
        gf("array2.inner2.key9"),
        lit(" "),
        c.getField("key10"))
    ).select("key1", "array2")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations work if a nested struct in an array is accessed") {
    val expectedSchema = getResourceString("/test_data/nested/nested6Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested6Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructMap(df, "array2.inner2", "out", (c, gf) =>
      concat(c.getField("key10"),
        lit(" "),
        gf("array2.inner2.struct3.k1").cast(StringType))
    ).select("array2")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations work for a nested struct in an array is accessed") {
    val expectedSchema = getResourceString("/test_data/nested/nested7Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested7Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructMap(df, "array2.inner2.struct3", "out", (c, gf) =>
      concat(c.getField("k1"),
        lit(" "),
        gf("array2.inner2.key10").cast(StringType))
    ).select("array2")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations with error column work if a nested struct in an array is accessed") {
    val expectedSchema = getResourceString("/test_data/nested/nested8Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested8Results.json")

    val df = nestedTestCaseFactory.getTestCase

    val dfOut = NestedArrayTransformations.nestedExtendedStructAndErrorMap(df, "array2.inner2", "out", "errCol", (c, gf) =>
      concat(c.getField("key10"),
        lit(" "),
        gf("array2.inner2.struct3.k1").cast(StringType))
      ,
      (_, gf) => {
        when(gf("array2.inner2.struct3.k1") =!= 1,
          callUDF("confCastErr", lit("k1!==1"), gf("array2.inner2.struct3.k1").cast(StringType))
        ).otherwise(null)
      }
    ).select("array2", "errCol")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations with error column that has existing errors") {
    val expectedSchema = getResourceString("/test_data/nested/nested9Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested9Results.json")

    val df = nestedTestCaseFactory
      .getTestCase
      .withColumn("errCol", array(typedLit(ErrorMessage("Initial", "000", "ErrMsg", "id", Seq(), Seq()))))

    val dfOut = NestedArrayTransformations.nestedExtendedStructAndErrorMap(df, "array2.inner2", "out", "errCol", (c, gf) =>
      concat(c.getField("key10"),
        lit(" "),
        gf("array2.inner2.struct3.k1").cast(StringType))
      ,
      (_, gf) => {
        when(gf("array2.inner2.struct3.k1") =!= 1,
          callUDF("confCastErr", lit("k1!==1"), gf("array2.inner2.struct3.k1").cast(StringType))
        ).otherwise(null)
      }
    ).select("array2", "errCol")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test extended array transformations with error column for an array inside a double nested struct") {
    val expectedSchema = getResourceString("/test_data/nested/nested10Schema.txt")
    val expectedResults = getResourceString("/test_data/nested/nested10Results.json")

    val df = nestedTestCaseFactory
      .getTestCase
      .withColumn("errCol", array(typedLit(ErrorMessage("Initial", "000", "ErrMsg", "id", Seq(), Seq()))))

    val dfOut = NestedArrayTransformations.nestedExtendedStructAndErrorMap(df, "struct3.inner3.array3",
      "struct3.inner3.array3.out", "errCol", (c, gf) =>
      concat(c.getField("a1"),
        lit(" "),
        gf("struct3.inner3.array3.a2").cast(StringType))
      ,
      (_, gf) => {
        when(gf("struct3.inner3.array3.a1") =!= 3,
          callUDF("confCastErr", lit("a1!==3"), gf("struct3.inner3.array3.a1").cast(StringType))
        ).otherwise(null)
      }
    ).select("struct3", "errCol")

    val actualSchema = dfOut.schema.treeString
    val actualResults = JsonUtils.prettySparkJSON(dfOut.orderBy("id").toJSON.collect())

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  private def getResourceString(name: String): String =
    IOUtils.toString(getClass.getResourceAsStream(name), "UTF-8")

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
    if (!expectedResults.startsWith(actualResults)) {
      log.error("EXPECTED:")
      log.error(expectedResults)
      log.error("ACTUAL:")
      log.error(actualResults)
      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }
}
