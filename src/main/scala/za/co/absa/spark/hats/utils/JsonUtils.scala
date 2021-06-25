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

package za.co.absa.spark.hats.utils

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonUtils {

  /**
    * Formats a JSON string so it looks pretty.
    *
    * @param jsonIn A JSON string
    * @return A pretty formatted JSON string
    */
  def prettyJSON(jsonIn: String): String = {
    val mapper = new ObjectMapper()

    val jsonUnindented = mapper.readValue(jsonIn, classOf[Any])
    val indented = mapper.writerWithDefaultPrettyPrinter.writeValueAsString(jsonUnindented)
    indented.replace("\r\n", "\n")
  }

  /**
    * Formats a Spark-generated JSON strings that are returned by
    * applying `.toJSON.collect()` to a DataFrame.
    *
    * @param jsons A list of JSON documents
    * @return A pretty formatted JSON string
    */
  def prettySparkJSON(jsons: Seq[String]): String = {
    //val properJson = "[" + "}\n".r.replaceAllIn(jsonIn, "},\n") + "]"
    val singleJSON = jsons.mkString("[", ",", "]")
    prettyJSON(singleJSON)
  }

  /**
    * Creates a Spark DataFrame from a JSON document(s).
    *
    * @param json A json string to convert to a DataFrame
    * @return A data frame
    */
  def getDataFrameFromJson(spark: SparkSession, json: Seq[String]): DataFrame = {
    import spark.implicits._
    spark.read.json(json.toDS)
  }
}
