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

package za.co.absa.spark.hats.transformations

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import za.co.absa.spark.hats.transformations.NestedArrayTransformations.splitByDeepestParent

/**
  * The class provides a storage for array transformation context for a transformation of a dataframe field.
  * The context contains all arrays in the path of the field and their corresponding array element lambda variables
  * provided by 'transform()' function of Spark SQL.
  */
private[transformations]
class ArrayContext(val arrayPaths: Seq[String] = Array[String](),
                   val lambdaVars: Seq[Column] = Array[Column]()) {

  /**
    * Returns a new context by appending the current context with a new array/lambda combination.
    *
    * @param arrayPath A fully-qualified array field name.
    * @param lambdaVar A lambda variable of the array element provided by 'transform()' function of Spark SQL.
    * @return A column that corresponds to the field name.
    */
  def withArraysUpdated(arrayPath: String, lambdaVar: Column): ArrayContext = {
    new ArrayContext(arrayPaths :+ arrayPath, lambdaVars :+ lambdaVar)
  }

  /**
    * Returns an instance of Column that corresponds to the input field's level of array nesting.
    *
    * @param fieldName A fully-qualified field name.
    * @return A column that corresponds to the field name.
    */
  def getField(fieldName: String): Column = {
    val (parentArray, childField) = splitByDeepestParent(fieldName, arrayPaths)
    if (parentArray.isEmpty) {
      col(childField)
    } else {
      val i = arrayPaths.indexOf(parentArray)
      if (fieldName == arrayPaths(i)) {
        // If the array itself is specified - return the array
        lambdaVars(i)
      } else {
        // If a field inside an array is specified - return the field
        // by using '.getField()' on each child (which could be a nested struct)
        childField.split('.')
          .foldLeft(lambdaVars(i))((parent, column) => parent.getField(column))
      }
    }
  }
}
