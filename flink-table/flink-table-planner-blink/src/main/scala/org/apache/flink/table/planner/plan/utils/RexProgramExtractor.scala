/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.utils

import java.sql.{Date, Time, Timestamp}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.{SqlFunction, SqlPostfixOperator}
import org.apache.calcite.util.{DateString, TimeString, TimestampString}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.FunctionCatalog
import org.apache.flink.table.dataformat.DataFormatConverters.{LocalDateTimeConverter, LocalTimeConverter}
import org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedCall
import org.apache.flink.table.expressions._
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions._
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils.unixTimestampToLocalDateTime
import org.apache.flink.table.types.logical.LogicalTypeRoot.{TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIME_WITHOUT_TIME_ZONE}
import org.apache.flink.util.Preconditions
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object RexProgramExtractor {

  lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Extracts the indices of input fields which accessed by the RexProgram.
    *
    * @param rexProgram The RexProgram to analyze
    * @return The indices of accessed input fields
    */
  def extractRefInputFields(rexProgram: RexProgram): Array[Int] = {
    val visitor = new InputRefVisitor

    // extract referenced input fields from projections
    rexProgram.getProjectList.foreach(
      exp => rexProgram.expandLocalRef(exp).accept(visitor))

    // extract referenced input fields from condition
    val condition = rexProgram.getCondition
    if (condition != null) {
      rexProgram.expandLocalRef(condition).accept(visitor)
    }

    visitor.getFields
  }

  /**
    * Extract condition from RexProgram and convert it into independent CNF expressions.
    *
    * @param rexProgram The RexProgram to analyze
    * @return converted expressions as well as RexNodes which cannot be translated
    */
  def extractConjunctiveConditions(
      rexProgram: RexProgram,
      rexBuilder: RexBuilder,
      catalog: FunctionCatalog): (Array[Expression], Array[RexNode]) = {

    rexProgram.getCondition match {
      case condition: RexLocalRef =>
        val expanded = rexProgram.expandLocalRef(condition)
        // converts the expanded expression to conjunctive normal form,
        // like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
        val cnf = RexUtil.toCnf(rexBuilder, expanded)
        // converts the cnf condition to a list of AND conditions
        val conjunctions = RelOptUtil.conjunctions(cnf)

        val convertedExpressions = new mutable.ArrayBuffer[Expression]
        val unconvertedRexNodes = new mutable.ArrayBuffer[RexNode]
        val inputNames = rexProgram.getInputRowType.getFieldNames.asScala.toArray
        val converter = new RexNodeToExpressionConverterV2(inputNames, catalog)

        conjunctions.asScala.foreach(rex => {
          rex.accept(converter) match {
            case Some(expression) => convertedExpressions += expression
            case None => unconvertedRexNodes += rex
          }
        })
        (convertedExpressions.toArray, unconvertedRexNodes.toArray)

      case _ => (Array.empty, Array.empty)
    }
  }

  /**
    * Extracts the name of nested input fields accessed by the RexProgram and returns the
    * prefix of the accesses.
    *
    * @param rexProgram The RexProgram to analyze
    * @return The full names of accessed input fields. e.g. field.subfield
    */
  def extractRefNestedInputFields(
      rexProgram: RexProgram, usedFields: Array[Int]): Array[Array[String]] = {

    val visitor = new RefFieldAccessorVisitor(usedFields)
    rexProgram.getProjectList.foreach(exp => rexProgram.expandLocalRef(exp).accept(visitor))

    val condition = rexProgram.getCondition
    if (condition != null) {
      rexProgram.expandLocalRef(condition).accept(visitor)
    }
    visitor.getProjectedFields
  }
}

/**
  * An RexVisitor to convert RexNode to Expression.
  *
  * @param inputNames      The input names of the relation node
  * @param functionCatalog The function catalog
  */
class RexNodeToExpressionConverterV2(
    inputNames: Array[String],
    functionCatalog: FunctionCatalog)
    extends RexVisitor[Option[Expression]] {

  override def visitInputRef(inputRef: RexInputRef): Option[Expression] = {
    Preconditions.checkArgument(inputRef.getIndex < inputNames.length)
    Some(PlannerResolvedFieldReference(
      inputNames(inputRef.getIndex),
      FlinkTypeFactory.toTypeInfo(inputRef.getType)
    ))
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): Option[Expression] =
    visitInputRef(rexTableInputRef)

  override def visitLocalRef(localRef: RexLocalRef): Option[Expression] = {
    throw new TableException("Bug: RexLocalRef should have been expanded")
  }

  override def visitLiteral(literal: RexLiteral): Option[Expression] = {
    val literalType = FlinkTypeFactory.toTypeInfo(literal.getType)

    val literalValue = literalType match {

      case _@SqlTimeTypeInfo.DATE =>
        val rexValue = literal.getValueAs(classOf[DateString])
        Date.valueOf(rexValue.toString)

      case _@SqlTimeTypeInfo.TIME =>
        val rexValue = literal.getValueAs(classOf[TimeString])
        Time.valueOf(rexValue.toString(0))

      case _@SqlTimeTypeInfo.TIMESTAMP =>
        val rexValue = literal.getValueAs(classOf[TimestampString])
        Timestamp.valueOf(rexValue.toString(3))

      case _@BasicTypeInfo.BYTE_TYPE_INFO =>
        // convert from BigDecimal to Byte
        literal.getValueAs(classOf[java.lang.Byte])

      case _@BasicTypeInfo.SHORT_TYPE_INFO =>
        // convert from BigDecimal to Short
        literal.getValueAs(classOf[java.lang.Short])

      case _@BasicTypeInfo.INT_TYPE_INFO =>
        // convert from BigDecimal to Integer
        literal.getValueAs(classOf[java.lang.Integer])

      case _@BasicTypeInfo.LONG_TYPE_INFO =>
        // convert from BigDecimal to Long
        literal.getValueAs(classOf[java.lang.Long])

      case _@BasicTypeInfo.FLOAT_TYPE_INFO =>
        // convert from BigDecimal to Float
        literal.getValueAs(classOf[java.lang.Float])

      case _@BasicTypeInfo.DOUBLE_TYPE_INFO =>
        // convert from BigDecimal to Double
        literal.getValueAs(classOf[java.lang.Double])

      case _@BasicTypeInfo.STRING_TYPE_INFO =>
        // convert from NlsString to String
        literal.getValueAs(classOf[java.lang.String])

      case _@BasicTypeInfo.BOOLEAN_TYPE_INFO =>
        // convert to Boolean
        literal.getValueAs(classOf[java.lang.Boolean])

      case _@BasicTypeInfo.BIG_DEC_TYPE_INFO =>
        // convert to BigDecimal
        literal.getValueAs(classOf[java.math.BigDecimal])

      case _ =>
        // Literal type is not supported.
        RexProgramExtractor.LOG.debug(
          "Literal {} of SQL type {} is not supported and cannot be converted. " +
            "Please reach out to the community if you think this type should be supported.",
          Array(literal, literal.getType): _*)
        return None
    }

    Some(Literal(literalValue, literalType))
  }

  override def visitCall(call: RexCall): Option[Expression] = {
    val operands = call.getOperands.map(
      operand => operand.accept(this).orNull
    )

    // return null if we cannot translate all the operands of the call
    if (operands.contains(null)) {
      None
    } else {
        // TODO we cast to planner expression as a temporary solution to keep the old interfaces
        call.getOperator match {
          case SqlStdOperatorTable.OR =>
            Option(operands.reduceLeft { (l, r) =>
              Or(l.asInstanceOf[PlannerExpression], r.asInstanceOf[PlannerExpression])
            })
          case SqlStdOperatorTable.AND =>
            Option(operands.reduceLeft { (l, r) =>
              And(l.asInstanceOf[PlannerExpression], r.asInstanceOf[PlannerExpression])
            })
          case function: SqlFunction =>
            lookupFunction(replace(function.getName), operands)
          case postfix: SqlPostfixOperator =>
            lookupFunction(replace(postfix.getName), operands)
          case operator@_ =>
            lookupFunction(replace(s"${operator.getKind}"), operands)
      }
    }
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Option[Expression] = None

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): Option[Expression] = None

  override def visitRangeRef(rangeRef: RexRangeRef): Option[Expression] = None

  override def visitSubQuery(subQuery: RexSubQuery): Option[Expression] = None

  override def visitDynamicParam(dynamicParam: RexDynamicParam): Option[Expression] = None

  override def visitOver(over: RexOver): Option[Expression] = None

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): Option[Expression] = None

  private def lookupFunction(name: String, operands: Seq[Expression]): Option[Expression] = {
    // TODO we assume only planner expression as a temporary solution to keep the old interfaces
    val expressionBridge = new ExpressionBridge[PlannerExpression](
      functionCatalog,
      PlannerExpressionConverter.INSTANCE)
    JavaScalaConversionUtil.toScala(functionCatalog.lookupFunction(name))
      .flatMap(result =>
        Try(expressionBridge.bridge(
          unresolvedCall(result.getFunctionDefinition, operands: _*))).toOption
      )
  }

  private def replace(str: String): String = {
    str.replaceAll("\\s|_", "")
  }
}
