/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.hats.transformations.samples

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

class NestedMapTestCaseFactory(implicit spark: SparkSession) {

  private val testCaseSchema = new StructType()
    .add("name", StringType)
    .add("addresses", ArrayType(new StructType()
      .add("city", StringType)
      .add("state", StringType)))
    .add("properties", MapType(StringType, StringType))

  private val tstCaseData = Seq(
    Row("John", List(Row("Newark", "NY"), Row("Brooklyn", "NY")), Map("hair" -> "black", "eyes" -> "brown", "height" -> "178")),
    Row("Kate", List(Row("San Jose", "CA"), Row("Sandiago", "CA")), Map("hair" -> "brown", "eyes" -> "black", "height" -> "165")),
    Row("William", List(Row("Las Vegas", "NV")), Map("hair" -> "red", "eye" -> "gray", "height" -> "185")),
    Row("Sarah", null, Map("hair" -> "blond", "eyes" -> "red", "height" -> "162")),
    Row("Michael", List(Row("Sacramento", "CA"), Row("San Diego", "CA")), Map("white" -> "black", "eyes" -> "black", "height" -> "180"))
  )

  def getTestCase: DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(tstCaseData),
      testCaseSchema
    ).orderBy("name")
  }

}
