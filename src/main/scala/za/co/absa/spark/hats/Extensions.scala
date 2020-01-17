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

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import za.co.absa.spark.hats.transformations.NestedArrayTransformations

/**
  * The object is a container of extension methods for Spark DataFrames.
  */
object Extensions {

  type TransformFunction = Column => Column

  type ExtendedTransformFunction = (String => Column) => Column

  /**
    * The class represents an extension wrapper for an [[org.apache.spark.sql.DataFrame]].
    *
    * @param dataset A data frame to be extended with methods contained in this class.
    */
  implicit class DataFrameExtension(dataset: Dataset[Row]) {

    /**
      * Add a column that can be inside nested structs, arrays and its combinations.
      *
      * @param newColumnName A column name to be created
      * @param expression    A new column value
      * @return A dataframe with a new field that contains transformed values.
      */
    def nestedWithColumn(newColumnName: String,
                         expression: Column): Dataset[Row] = {
      NestedArrayTransformations.nestedAddColumn(dataset, newColumnName, expression)
    }

    /**
      * Add a column that can be inside nested structs, arrays and its combinations.
      *
      * @param newColumnName A column name to be created
      * @param expression    A new column value
      * @return A dataframe with a new field that contains transformed values.
      */
    def nestedWithColumnExtended(newColumnName: String,
                                 expression: ExtendedTransformFunction): Dataset[Row] = {
      NestedArrayTransformations.nestedAddColumnExtended(dataset, newColumnName,
        (_, getFieldFunction) => expression(getFieldFunction))
    }

    /**
      * Drop a column from inside a nested structs, arrays and its combinations
      *
      * @param columnToDrop A column name to be dropped
      * @return A dataframe with a new field that contains transformed values.
      */
    def nestedDropColumn(columnToDrop: String): DataFrame = {
      NestedArrayTransformations.nestedDropColumn(dataset, columnToDrop)
    }

    /**
      * Map transformation for columns that can be inside nested structs, arrays and its combinations.
      *
      * If the input column is a primitive field the method will add outputColumnName at the same level of nesting
      * by executing the `expression` passing the source column into it. If a struct column is expected you can
      * use `.getField(...)` method to operate on its children.
      *
      * The output column name can omit the full path as the field will be created at the same level of nesting as the input column.
      *
      * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
      * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
      * @param expression       A function that applies a transformation to a column as a Spark expression.
      * @return A dataframe with a new field that contains transformed values.
      */
    def nestedMapColumn(inputColumnName: String,
                        outputColumnName: String,
                        expression: TransformFunction): DataFrame = {
      NestedArrayTransformations.nestedWithColumnMap(dataset, inputColumnName, outputColumnName, expression)
    }

  }

}
