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

import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.util.Random

object SchemaUtils {

  /**
    * For an array of arrays of arrays, ... get the final element type at the bottom of the array
    *
    * @param arrayType An array data type from a Spark dataframe schema
    * @return A non-array data type at the bottom of array nesting
    */
  @tailrec
  def getDeepestArrayType(arrayType: ArrayType): DataType = {
    arrayType.elementType match {
      case a: ArrayType => getDeepestArrayType(a)
      case b => b
    }
  }

  /**
    * Generate a unique column name
    *
    * @param prefix A prefix to use for the column name
    * @param schema An optional schema to validate if the column already exists (a very low probability)
    * @return A name that can be used as a unique column name
    */
  def getUniqueName(prefix: String, schema: Option[StructType]): String = {
    schema match {
      case None =>
        s"${prefix}_${Random.nextLong().abs}"
      case Some(sch) =>
        var exists = true
        var columnName = ""
        while (exists) {
          columnName = s"${prefix}_${Random.nextLong().abs}"
          exists = sch.fields.exists(_.name.compareToIgnoreCase(columnName) == 0)
        }
        columnName
    }
  }

}
