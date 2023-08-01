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

package za.co.absa.spark.hats

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{transform => sparkTransform}

/**
  * This is a wrapper for high order functions depending on Scala version
  */
object HofsWrapper {
  /**
    * Applies the function `f` to every element in the `array`. The method is an equivalent to the `map` function
    * from functional programming.
    *
    * @param array       A column of arrays
    * @param f           A function transforming individual elements of the array
    * @param elementName The name of the lambda variable. The value is used in Spark execution plans.
    * @return A column of arrays with transformed elements
    */
  def transform(
                 array: Column,
                 f: Column => Column,
                 elementName: String): Column = {
    sparkTransform(array, f)
  }
}
