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
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import za.co.absa.spark.hofs._
import za.co.absa.spark.hats.utils.SchemaUtils

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

object NestedArrayTransformations {

  type TransformFunction = Column => Column

  /**
    * Map transformation for columns that can be inside nested structs, arrays and its combinations.
    *
    * If the input column is a primitive field the method will add outputColumnName at the same level of nesting
    * by executing the `expression` passing the source column into it. If a struct column is expected you can
    * use `.getField(...)` method to operate on its children.
    *
    * The output column name can omit the full path as the field will be created at the same level of nesting as the input column.
    *
    * @param df               Dataframe to be transformed
    * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
    * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
    * @param expression       A function that applies a transformation to a column as a Spark expression.
    * @return A dataframe with a new field that contains transformed values.
    */
  private[hats] def nestedWithColumnMap(df: DataFrame,
                                        inputColumnName: String,
                                        outputColumnName: String,
                                        expression: TransformFunction): DataFrame = {
    nestedWithColumnMapHelper(df, inputColumnName, outputColumnName, Some(expression))._1
  }

  /**
    * A nested map that also appends errors to the error column (see [[NestedArrayTransformations.nestedWithColumnMap]] above)
    *
    * @param df               Dataframe to be transformed
    * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
    * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
    * @param expression       A function that applies a transformation to a column as a Spark expression.
    * @param errorCondition   A function that takes an input column and returns an expression for an error column.
    * @return A dataframe with a new field that contains transformed values.
    */
  def nestedWithColumnAndErrorMap(df: DataFrame,
                                  inputColumnName: String,
                                  outputColumnName: String,
                                  errorColumnName: String,
                                  expression: TransformFunction,
                                  errorCondition: TransformFunction
                                 ): DataFrame = {

    if (errorColumnName.contains('.')) {
      throw new IllegalArgumentException(s"Error columns should be at the root schema level. " +
        s"Value '$errorColumnName' is not valid.")
    }

    val (dfOut: DataFrame, deepErrorColumn: String) =
      nestedWithColumnMapHelper(df, inputColumnName, outputColumnName, Some(expression), Some(errorCondition))

    gatherErrors(dfOut, deepErrorColumn, errorColumnName)
  }

  /**
    * A nested struct map. Given a struct field the method will create a new child field of that struct as a
    * transformation of struct fields. This is useful for transformations such as concatenation of fields.
    *
    * To use root of the schema as the input struct pass "" as the `inputStructField`.
    * In this case `null` will be passed to the lambda function.
    *
    * Here is an example demonstrating how to handle both root and nested cases:
    *
    * {{{
    * val dfOut = nestedStructMap(df, columnPath, "combinedField", c => {
    * if (c==null) {
    *   // The columns are at the root level
    *   concat(col("city"), col("street"))
    * } else {
    *   // The columns are inside nested structs/arrays
    *   concat(c.getField("city"), c.getField("street"))
    * }
    * })
    * }}}
    *
    * @param df               An input DataFrame
    * @param inputStructField A struct column name for which to apply the transformation
    * @param outputChildField The output column name that will be added as a child of the source struct.
    * @param expression       A function that applies a transformation to a column as a Spark expression
    * @return A dataframe with a new field that contains transformed values.
    */
  private[hats] def nestedStructMap(df: DataFrame,
                                    inputStructField: String,
                                    outputChildField: String,
                                    expression: TransformFunction
                                   ): DataFrame = {
    val updatedStructField = if (inputStructField.nonEmpty) inputStructField + ".*" else ""
    nestedWithColumnMap(df, updatedStructField, outputChildField, expression)
  }

  /**
    * A nested struct map with error column support. Given a struct field the method will create a new child field of that
    * struct as a transformation of struct fields and will update the error column according to a specified transformation.
    * This is useful for transformations that require combining several fields of a struct in an array.
    *
    * To use root of the schema as the input struct pass "" as the `inputStructField`.
    * In this case `null` will be passed to the lambda function.
    *
    * Here is an example demonstrating how to handle both root and nested cases:
    *
    * {{{
    * val dfOut = nestedStructAndErrorMap(df, columnPath, "combinedField", c => {
    * // Struct transformation
    * if (c==null) {
    *   // The columns are at the root level
    *   concat(col("city"), col("street"))
    * } else {
    *   // The columns are inside nested structs/arrays
    *   concat(c.getField("city"), c.getField("street"))
    * }
    * }, c => {
    * // Error column transformation
    * if (c==null) {
    *   // The columns are at the root level
    *   if (isError(col("city")) ErrorCaseClsss("Some error") else null
    * } else {
    *   // The columns are inside nested structs/arrays
    *   if (isError(c.getField("city")) ErrorCaseClsss("Some error") else null
    * }
    * })
    * }}}
    *
    * @param df               An input DataFrame
    * @param inputStructField A struct column name for which to apply the transformation
    * @param outputChildField The output column name that will be added as a child of the source struct.
    * @param errorColumnName  An error column name
    * @param expression       A function that applies a transformation to a column as a Spark expression
    * @param errorCondition   A function that should check error conditions and return an error column in case such conditions are met
    * @return A dataframe with a new field that contains transformed values.
    */
  def nestedStructAndErrorMap(df: DataFrame,
                              inputStructField: String,
                              outputChildField: String,
                              errorColumnName: String,
                              expression: TransformFunction,
                              errorCondition: TransformFunction
                             ): DataFrame = {
    val updatedStructField = if (inputStructField.nonEmpty) inputStructField + ".*" else ""
    nestedWithColumnAndErrorMap(df, updatedStructField, outputChildField, errorColumnName, expression, errorCondition)
  }

  /**
    * Add a column that can be inside nested structs, arrays and its combinations
    *
    * @param df            Dataframe to be transformed
    * @param newColumnName A column name to be created
    * @param expression    A new column value
    * @return A dataframe with a new field that contains transformed values.
    */
  private[hats] def nestedAddColumn(df: DataFrame,
                                    newColumnName: String,
                                    expression: Column): DataFrame = {
    try {
      nestedWithColumnMapHelper(df, newColumnName, "", Some(_ => expression), None)._1
    } catch {
      case e: IllegalArgumentException if e.getMessage.contains("Output field cannot be empty") =>
        throw new IllegalArgumentException(s"The column '$newColumnName' already exists.", e)
      case NonFatal(e) => throw e
    }

  }

  /**
    * Drop a column from inside a nested structs, arrays and its combinations
    *
    * @param df           Dataframe to be transformed
    * @param columnToDrop A column name to be dropped
    * @return A dataframe with a new field that contains transformed values.
    */
  private[hats] def nestedDropColumn(df: DataFrame,
                                     columnToDrop: String): DataFrame = {
    nestedWithColumnMapHelper(df, columnToDrop, "")._1
  }

  // scalastyle:off method.length
  // scalastyle:off null
  /**
    * This is a helper function for all mapping transformations.
    *
    * It combines many operations inside one traversal. It is used to
    * - Map a column
    * - Map a column with an additional creation of an error column
    * - Add/remove column
    *
    * It is probably too complicated to use by itself. Use facade functions instead.
    *
    * If the input column is a primitive field the method will add outputColumnName at the same level of nesting
    * by executing the `expression` passing the source column into it. If a struct column is expected you can
    * use `.getField(...)` method to operate on its children.
    *
    * The output column name can omit the full path as the field will be created at the same level of nesting as the input column.
    *
    * If the input column ends with '*', e.g. `shop.manager.*`, the struct itself will be passed as the lambda parameter,
    * but the new column will be placed inside the struct. This behavior is used in [[NestedArrayTransformations.nestedStructMap]].
    *
    * If the input column does not exist, the column will be created passing null as a column parameter to the expression.
    * This behavior is used in [[NestedArrayTransformations.nestedAddColumn]].
    *
    * If null is passed as an expression the input column will be dropped. This behavior is used in
    * [[NestedArrayTransformations.nestedDropColumn]].
    *
    * @param df               Dataframe to be transformed
    * @param inputColumnName  A column name for which to apply the transformation, e.g. `company.employee.firstName`.
    * @param outputColumnName The output column name. The path is optional, e.g. you can use `conformedName` instead of `company.employee.conformedName`.
    * @param expression       A function that applies a transformation to a column as a Spark expression.
    * @param errorCondition   A function that should check error conditions and return an error column in case such conditions are met
    * @return A pair consisting of a dataframe with a new field that contains transformed values and a string containing the error column name.
    */
  private[hats] def nestedWithColumnMapHelper(df: DataFrame,
                                              inputColumnName: String,
                                              outputColumnName: String,
                                              expression: Option[TransformFunction] = None,
                                              errorCondition: Option[TransformFunction] = None
                                             ): (DataFrame, String) = {
    // The name of the field is the last token of outputColumnName
    val outputFieldName = outputColumnName.split('.').last
    var errorColumnName = ""
    val replaceExistingColumn = inputColumnName == outputColumnName

    // Sequential lambda name generator
    var lambdaIndex = 1

    def getLambdaName: String = {
      val name = s"v$lambdaIndex"
      lambdaIndex += 1
      name
    }

    def ensureOutputColumnNonEmpty(): Unit = {
      if (outputColumnName.isEmpty) {
        throw new IllegalArgumentException(
          s"Output field cannot be empty when transforming an existing field '$inputColumnName'"
        )
      }
    }

    def addErrorColumn(schema: Option[StructType], column: Column): Option[Column] = {
      errorCondition.map(errorCond => {
        errorColumnName = SchemaUtils.getUniqueName("errorList", schema)
        val errorColumn = array(errorCond(column)).as(errorColumnName)
        if (inputColumnName.contains('.')) {
          val parent = inputColumnName.split('.').dropRight(1).mkString(".")
          errorColumnName = s"$parent.$errorColumnName"
        }
        errorColumn
      })
    }

    // Handle the case when the input column is inside a nested struct
    def mapStruct(schema: StructType, path: Seq[String], parentColumn: Option[Column] = None): Seq[Column] = {
      val mappedFields = new ListBuffer[Column]()

      def handleStructLevelMap(): Unit = {
        expression match {
          case None =>
            throw new IllegalArgumentException("An expression must be specified if an asterix is used inside" +
              s"input field name ($inputColumnName).")
          case Some(exp) =>
            val parentField = parentColumn.orNull
            ensureOutputColumnNonEmpty()
            mappedFields += exp(parentField).as(outputFieldName)
            addErrorColumn(Some(schema), parentField).foreach(mappedFields += _)
        }
      }

      def handleNewFieldRequest(newFieldName: String): Unit = {
        expression match {
          case None =>
            throw new IllegalArgumentException("An expression must be specified if addition of a new field is " +
              s"requested ($inputColumnName).")
          case Some(exp) =>
            mappedFields += exp(null).as(newFieldName)
        }
      }

      def handleInputFieldDoesNotExist(fieldName: String): Unit = {
        if (fieldName == "*") {
          // If a star is specified as the last field name => manipulation on a struct itself is requested
          handleStructLevelMap()
        } else {
          // Field not found => an addition of a new field is requested
          val fieldToAdd = if (fieldName.isEmpty) outputFieldName else fieldName
          handleNewFieldRequest(fieldToAdd)
        }
      }

      def handleMatchedLeaf(field: StructField, curColumn: Column): Seq[Column] = {
        expression match {
          case None =>
            // Drops the column if the expression is not specified
            Nil
          case Some(exp) =>
            field.dataType match {
              case dt: ArrayType =>
                mapArray(dt, path, parentColumn)
              case _ =>
                ensureOutputColumnNonEmpty()
                val newColumn = exp(curColumn).as(outputFieldName)
                addErrorColumn(Some(schema), curColumn).foreach(mappedFields += _)
                if (replaceExistingColumn) {
                  Seq(newColumn)
                } else {
                  mappedFields += newColumn
                  Seq(curColumn)
                }
            }
        }
      }

      def handleMatchedNonLeaf(field: StructField, curColumn: Column): Seq[Column] = {
        // Non-leaf columns need to be further processed recursively
        field.dataType match {
          case dt: StructType => Seq(struct(mapStruct(dt, path.tail, Some(curColumn)): _*).as(field.name))
          case dt: ArrayType => mapArray(dt, path, parentColumn)
          case _ => throw new IllegalArgumentException(s"Field '${field.name}' is not a struct type or an array.")
        }
      }

      def handleMatchedField(field: StructField, curColumn: Column, isLeaf: Boolean): Seq[Column] = {
        if (isLeaf) {
          handleMatchedLeaf(field, curColumn)
        } else {
          handleMatchedNonLeaf(field, curColumn)
        }
      }

      val fieldName = path.head
      val isLeaf = isLeafElement(path)
      var fieldFound = false

      val newColumns = schema.fields.flatMap(field => {
        // This is the original column (struct field) we want to process
        val curColumn = parentColumn match {
          case None => new Column(field.name)
          case Some(col) => col.getField(field.name).as(field.name)
        }

        if (field.name.compareToIgnoreCase(fieldName) != 0) {
          // Copy unrelated fields as they were
          Seq(curColumn)
        } else {
          // We have found a match
          fieldFound = true
          handleMatchedField(field, curColumn, isLeaf)
        }
      })

      if (isLeaf) {
        if (inputColumnName == "") {
          // A transformation is requested on the root level of the schema as a struct field (nestedStructAndErrorMap(...))
          handleStructLevelMap()
        } else if (!fieldFound) {
          handleInputFieldDoesNotExist(fieldName)
        }
      }

      newColumns ++ mappedFields
    }

    // Handle arrays (including arrays of arrays) of primitives
    // The output column will also be an array, not an additional element of the existing array
    def mapNestedArrayOfPrimitives(schema: ArrayType, expr: TransformFunction,
                                   doFlatten: Boolean = false): TransformFunction = {
      val lambdaName = getLambdaName
      val elemType = schema.elementType

      elemType match {
        case _: StructType =>
          throw new IllegalArgumentException(s"Unexpected usage of mapNestedArrayOfPrimitives() on structs.")
        case dt: ArrayType =>
          val innerArray = mapNestedArrayOfPrimitives(dt, expr, doFlatten)
          if (doFlatten) {
            x => flatten(transform(x, innerArray, lambdaName))
          } else {
            x => transform(x, innerArray, lambdaName)
          }
        case dt =>
          x => transform(x, InnerX => expr(InnerX), lambdaName)
      }
    }

    // Handle the case when the input column is inside a nested array
    def mapArray(schema: ArrayType, path: Seq[String], parentColumn: Option[Column] = None,
                 isParentArray: Boolean = false): Seq[Column] = {
      val isLeaf = isLeafElement(path)
      val lambdaName = getLambdaName
      val fieldName = path.head
      val mappedFields = new ListBuffer[Column]()

      val curColumn = parentColumn match {
        case None => new Column(fieldName)
        case Some(col) if !isParentArray => col.getField(fieldName).as(fieldName)
        case Some(col) if isParentArray => col
      }

      // For an error column created by transforming arrays of primitives the error column will be created at
      // the same level as the array. The error column will contain errors from all elements of the processed array
      def handleErrorColumnOfArraysOfPrimitives(errorExpression: Option[TransformFunction],
                                                doFlatten: Boolean): Unit = {
        errorExpression.map(errorExpr => {
          errorColumnName = SchemaUtils.getUniqueName("errorList", None)
          val errorColumn = if (doFlatten) {
            flatten(transform(curColumn, x => errorExpr(x), lambdaName)).as(errorColumnName)
          } else {
            transform(curColumn, x => errorExpr(x), lambdaName).as(errorColumnName)
          }
          if (inputColumnName.contains('.')) {
            val parent = inputColumnName.split('.').dropRight(1).mkString(".")
            errorColumnName = s"$parent.$errorColumnName"
          }
          mappedFields += errorColumn
        })
      }

      // Handles primitive data types as well as nested arrays of primitives
      def handlePrimitive(dt: DataType, transformExpression: Option[TransformFunction],
                          errorExpression: Option[TransformFunction],
                          doFlatten: Boolean = false): Column = {
        if (isLeaf) {
          transformExpression match {
            case None =>
              // Drops the column
              null
            case Some(exp) =>
              // Retain the original column
              ensureOutputColumnNonEmpty()
              mappedFields += transform(curColumn, x => exp(x), lambdaName).as(outputFieldName)

              // Handle error column for arrays of primitives
              handleErrorColumnOfArraysOfPrimitives(errorExpression, doFlatten)
              curColumn
          }
        } else {
          // This is the case when the caller is requested to map a field that is a child of a primitive.
          // For instance, the caller is requested to map 'person.firstName.foo' when 'person.firstName'
          // is an instance of StringType.
          throw new IllegalArgumentException(s"Field $fieldName is not a struct or an array of struct type.")
        }
      }

      def handleNestedArray(dt: ArrayType): Column = {
        // This is the case when the input field is a several nested arrays of arrays of...
        // Each level of array nesting needs to be dealt with using transform()
        val deepestType = SchemaUtils.getDeepestArrayType(dt)
        deepestType match {
          case _: StructType =>
            // If at the bottom of the array nesting is a struct we need to add the output column
            // as a field of that struct
            // Example: if 'persons' is an array of array of structs having firstName and lastName,
            //          fields, then 'conformedFirstName' needs to be a new field inside the struct
            val innerArray = (x: Column) => mapArray(dt, path, Some(x), isParentArray = true)
            transform(curColumn, c => innerArray(c).head, lambdaName).as(fieldName)
          case _ =>
            // If at the bottom of the array nesting is a primitive we need to add the new column
            // as an array of its own
            // Example: if 'persons' is an array of array of string the output field,
            //          say, 'conformedPersons' needs also to be an array of array of string.
            val errorExpression = errorCondition.map(errorCond => {
              mapNestedArrayOfPrimitives(dt, errorCond, doFlatten = true)
            })

            val doFlatten = errorCondition.nonEmpty
            handlePrimitive(dt, Some(mapNestedArrayOfPrimitives(dt, expression.get)),
              errorExpression, doFlatten)
        }
      }

      def handleNestedStruct(dt: StructType) = {
        // If the leaf array element is struct we need to create the output field inside the struct itself.
        // This is done by specifying "*" as a leaf field.
        // If this struct is not a leaf element we just recursively call mapStruct() with child portion of the path.
        val innerPath = if (isLeaf) Seq("*") else path.tail
        val innerStruct = (x: Column) => struct(mapStruct(dt, innerPath, Some(x)): _*)
        transform(curColumn, innerStruct, lambdaName).as(fieldName)
      }

      val elemType = schema.elementType
      val newColumn = elemType match {
        case dt: StructType =>
          handleNestedStruct(dt)
        case dt: ArrayType =>
          handleNestedArray(dt)
        case dt =>
          // This handles an array of primitives, e.g. arrays of strings etc.
          val transformExpression = expression.map(expr => (x: Column) => expr(x))
          val errorExpression = errorCondition.map(cond => (x: Column) => cond(x))
          handlePrimitive(dt, transformExpression, errorExpression)
      }
      if (newColumn == null) {
        mappedFields
      } else {
        Seq(newColumn) ++ mappedFields
      }
    }

    val schema = df.schema
    val path = inputColumnName.split('.')
    (df.select(mapStruct(schema, path): _*), errorColumnName) // ;-]
  }

  /**
    * Gathers errors from a nested error column into a global error column for the dataframe
    *
    * @param df                A dataframe containing error columns.
    * @param nestedErrorColumn A column name that can be nested deeply inside the dataframe.
    * @param globalErrorColumn An error column name at the root shema level. This column should be at the root level.
    *                          It will be created automatically if it does not exist.
    * @return A dataframe with a new field that contains the list of errors.
    */
  def gatherErrors(df: DataFrame,
                   nestedErrorColumn: String,
                   globalErrorColumn: String): DataFrame = {

    def flattenNestedArrays(schema: StructType, inputColumnName: String): Column = {
      def handleNestedStruct(schema: StructType, columnPath: Seq[String], parentPath: Option[Column],
                             inputColumn: Column, arrayLevel: Int): Column = {
        val curCol = parentPath match {
          case None => col(columnPath.head)
          case Some(parentCol) => parentCol.getField(columnPath.head)
        }

        if (isLeafElement(columnPath)) {
          curCol
        } else {
          schema.apply(columnPath.head).dataType match {
            case st: StructType =>
              handleNestedStruct(st, columnPath.tail, Some(curCol), inputColumn, arrayLevel)
            case ar: ArrayType =>
              handleNestedArray(ar, columnPath, curCol, inputColumn, arrayLevel + 1)
            case _ =>
              curCol
          }
        }
      }

      def handleNestedArray(arr: ArrayType, columnPath: Seq[String], parentPath: Column, inputColumn: Column,
                            arrayLevel: Int): Column = {
        arr.elementType match {
          case st: StructType =>
            if (columnPath.isEmpty) {
              flatten(parentPath)
            } else {
              if (arrayLevel > 1) {
                // If the nested struct is inside of 2 dimensional array or an array of struct with an array of struct
                // need to flatten it before using .getField(), otherwise concatenation won't be able to figure out the
                // path ot the field.
                // E.g. If a schema looks like this:
                //   root
                //    |-- legs: array
                //    |    |-- element: struct
                //    |    |    |-- conditions: array
                //    |    |    |    |-- element: struct
                //    |    |    |    |    |-- errors: array<struct>
                //
                // To combine root level errors with nested ones (legs.conditions.errors) the flattening needs
                // to be like this:
                //    flatten(flatten($"legs.conditions").getField("errors"))
                //    [ this won't work: flatten(flatten($"legs.conditions.errors")) ]
                //    [ this won't work: flatten(flatten($"legs").getField("conditions")).getField("errors")) ]
                //
                handleNestedStruct(st, columnPath.tail, Some(flatten(parentPath)), inputColumn, arrayLevel)
              } else {
                flatten(handleNestedStruct(st, columnPath.tail, Some(parentPath), inputColumn, arrayLevel + 1))
              }
            }
          case ar: ArrayType =>
            flatten(handleNestedArray(ar, columnPath, parentPath, inputColumn, arrayLevel + 1))
          case _ =>
            flatten(parentPath)
        }
      }

      val path = nestedErrorColumn.split('.')
      handleNestedStruct(df.schema, path, None, col(inputColumnName), 0)
    }

    if (globalErrorColumn.contains('.')) {
      throw new IllegalArgumentException(s"Global error columns should be at the root schema level. " +
        s"Value '$globalErrorColumn' is not valid.")
    }

    val tmpCol = SchemaUtils.getUniqueName("tmp", Some(df.schema))
    val flattenedColumn = flattenNestedArrays(df.schema, nestedErrorColumn)

    val dfOutput =
      if (df.schema.fields.exists(_.name == globalErrorColumn)) {
        // 1. Rename original error column to a temporary name
        // 2. Add a new column with the original name by appending new errors to the existing ones

        // This preserves the position of the error column ([arguably] less efficient)
        addColumnAfter(df.withColumnRenamed(globalErrorColumn, tmpCol),
          tmpCol, globalErrorColumn, callUDF("arrayDistinctErrors", concat(col(tmpCol), flattenedColumn)))
          .drop(col(tmpCol))

        // This moves the error column to the end ([arguably] more efficient than preserving the position
        // of the error column)
        //df.withColumnRenamed(globalErrorColumn, tmpCol)
        //  .withColumn(globalErrorColumn, callUDF("arrayDistinctErrors", concat(col(tmpCol), flattenedColumn)))
        //  .drop(col(tmpCol))

      } else {
        // The root level error column does not exist. Adding it as a concatenation of nested errors
        df.withColumn(globalErrorColumn, callUDF("arrayDistinctErrors", flattenedColumn))
      }

    nestedDropColumn(dfOutput, nestedErrorColumn)
  }

  // scalastyle:on method.length
  // scalastyle:on null

  /** Adds a column similar to df.withColumn(), but you can specify the position of the new column by specifying
    * a column name after which to add the new column */
  private def addColumnAfter(df: DataFrame, afterColumn: String, columnName: String, expr: Column): DataFrame = {
    df.select(df.columns.flatMap(c => {
      if (c == afterColumn) {
        Seq(
          col(c),
          expr.as(columnName)
        )
      } else {
        Seq(col(c))
      }
    }): _*)

  }

  /** Checks if a path is a leaf element
    * Basically it is just a slightly more efficient version of path.length == 1
    *
    * @param path A path to an element of a struct (e.g. company.employee.firstName)
    * @return true if a path consists only of 1 element meaning it is the leaf element of the input column path
    */
  private def isLeafElement(path: Seq[String]): Boolean = path.lengthCompare(2) < 0
}
