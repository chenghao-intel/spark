/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.types._

/**
 * This is from org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode
 * Just a hint for the UDAF developers which stage we are about to process,
 * However, we probably don't want the developers knows so many details, here
 * is just for keep consistent with Hive (when integrated with Hive), need to
 * figure out if we have work around for that soon.
 */
@deprecated
trait Mode

/**
 * PARTIAL1: from original data to partial aggregation data: iterate() and
 * terminatePartial() will be called.
 */
@deprecated
case object PARTIAL1 extends Mode

/**
 * PARTIAL2: from partial aggregation data to partial aggregation data:
 * merge() and terminatePartial() will be called.
 */
@deprecated
case object PARTIAL2 extends Mode
/**
 * FINAL: from partial aggregation to full aggregation: merge() and
 * terminate() will be called.
 */
@deprecated
case object FINAL extends Mode
/**
 * COMPLETE: from original data directly to full aggregation: iterate() and
 * terminate() will be called.
 */
@deprecated
case object COMPLETE extends Mode

/**
 * Aggregation Function Interface
 * It's created by AggregateExpression
 */
trait AggregateFunction {
  self: Product =>

  // Initialize (reinitialize) the aggregation buffer
  def reset(buf: MutableRow): Unit

  // Expect the aggregate function fills the aggregation buffer when
  // fed with each value in the group
  def iterate(arguments: Any, buf: MutableRow): Unit

  // Merge 2 aggregation buffer, and write back to the later one
  def merge(value: Row, buf: MutableRow): Unit

  // Semantically we probably don't need this, however, we need it when
  // integrating with Hive UDAF(GenericUDAF)
  @deprecated
  def terminatePartial(buf: MutableRow): Unit = {}

  // Output the final result by feeding the aggregation buffer
  def terminate(input: Row): Any

  // AggregateExpression associated with this AggregateFunction
  def base: AggregateExpression

  def eval(row: Row): Any = base.eval(row)
}

trait AggregateExpression extends Expression {
  self: Product =>
  type EvaluatedType = Any

  var mode: Mode = COMPLETE

  def initial(m: Mode): Unit = {
    this.mode = m
  }

  // Create the AggregateFunction, by specified the schema (as BoundReference)
  def newInstance(buffers: Seq[BoundReference]): AggregateFunction = null
  // Aggregation Buffer data types
  def bufferDataType: Seq[DataType] = Nil
  // Is it a distinct aggregate expression?
  def distinct: Boolean
  // Is it a distinct like aggregate expression (e.g. Min/Max is distinctLike, while avg is not)
  def distinctLike: Boolean = false  // TODO we are not using this hint yet, remove it for now?

  def nullable = true

  override def eval(input: Row): EvaluatedType = children.map(_.eval(input))
}

abstract class UnaryAggregateExpression extends UnaryExpression with AggregateExpression {
  self: Product =>

  override def eval(input: Row): EvaluatedType = child.eval(input)
}

case class Min(
    child: Expression,
    distinct: Boolean = false,
    override val distinctLike: Boolean = true)
  extends UnaryAggregateExpression {

  override def toString = s"MIN($child)"
  override def dataType = child.dataType
  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def newInstance(buffers: Seq[BoundReference]) = MinFunction(buffers(0), this)
}

case class Average(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = false

  override def dataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType(precision + 4, scale + 4)  // Add 4 digits after decimal point, like Hive
    case DecimalType.Unlimited =>
      DecimalType.Unlimited
    case _ =>
      DoubleType
  }

  override def toString = s"AVG($child)"
  override def bufferDataType: Seq[DataType] = LongType :: dataType :: Nil

  override def newInstance(buffers: Seq[BoundReference]) =
    AverageFunction(buffers(0), buffers(1), this)
}

case class Max(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"MAX($child)"

  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def newInstance(buffers: Seq[BoundReference]) = MaxFunction(buffers(0), this)
}

case class Count(child: Expression)
  extends UnaryAggregateExpression {
  def distinct: Boolean = false
  override def nullable = false
  override def dataType = LongType
  override def toString = s"COUNT($child)"

  override def bufferDataType: Seq[DataType] = LongType :: Nil
  override def newInstance(buffers: Seq[BoundReference]) = CountFunction(buffers(0), this)
}

case class CountDistinct(children: Seq[Expression])
  extends AggregateExpression {
  def distinct: Boolean = true
  override def nullable = false
  override def dataType = LongType
  override def toString = s"COUNT($children)"

  override def bufferDataType: Seq[DataType] = LongType :: Nil
  override def newInstance(buffers: Seq[BoundReference]) = CountDistinctFunction(buffers(0), this)
}

case class Sum(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = true
  override def dataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType(precision + 10, scale)  // Add 10 digits left of decimal point, like Hive
    case DecimalType.Unlimited =>
      DecimalType.Unlimited
    case _ =>
      child.dataType
  }

  override def toString = s"SUM($child)"

  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def newInstance(buffers: Seq[BoundReference]) = SumFunction(buffers(0), this)
}

case class First(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"FIRST($child)"

  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def newInstance(buffers: Seq[BoundReference]) = FirstFunction(buffers(0), this)
}

case class Last(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = true
  override def dataType = child.dataType
  override def toString = s"LAST($child)"

  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def newInstance(buffers: Seq[BoundReference]) = LastFunction(buffers(0), this)
}

case class MinFunction(aggr: BoundReference, base: Min) extends AggregateFunction {
  val arg: MutableLiteral = MutableLiteral(null, base.dataType)
  val buffer: MutableLiteral = MutableLiteral(null, base.dataType)
  val cmp = LessThan(arg, buffer)

  override def reset(buf: MutableRow): Unit = {
    buf.update(aggr.ordinal, null)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      arg.value = argument
      buffer.value = buf(aggr.ordinal)
      if (buf.isNullAt(aggr.ordinal) || cmp.eval(null) == true) {
        buf.update(aggr.ordinal, argument)
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (!value.isNullAt(aggr.ordinal)) {
      arg.value = value(aggr.ordinal)
      buffer.value = rowBuf(aggr.ordinal)
      if (rowBuf.isNullAt(aggr.ordinal) || cmp.eval(null) == true) {
        rowBuf.update(aggr.ordinal, arg.value)
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class AverageFunction(count: BoundReference, sum: BoundReference, base: Average)
  extends AggregateFunction {
  // for iterate
  val arg = MutableLiteral(null, base.child.dataType)
  val cast = if (arg.dataType != base.dataType) Cast(arg, base.dataType) else arg
  val add = Add(cast, sum)

  // for merge
  val argInMerge = MutableLiteral(null, base.dataType)
  val addInMerge = Add(argInMerge, sum)

  // for terminate
  val divide = Divide(sum, Cast(count, base.dataType))

  override def reset(buf: MutableRow): Unit = {
    buf.update(count.ordinal, 0L)
    buf.update(sum.ordinal, null)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      arg.value = argument
      buf.update(count.ordinal, buf.getLong(count.ordinal) + 1)
      if (buf.isNullAt(sum.ordinal)) {
        buf.update(sum.ordinal, cast.eval())
      } else {
        buf.update(sum.ordinal, add.eval(buf))
      }
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (!value.isNullAt(sum.ordinal)) {
      buf.setLong(count.ordinal, value.getLong(count.ordinal) + buf.getLong(count.ordinal))
      if (buf.isNullAt(sum.ordinal)) {
        buf.update(sum.ordinal, value(sum.ordinal))
      } else {
        argInMerge.value = value(sum.ordinal)
        buf.update(sum.ordinal, addInMerge.eval(buf))
      }
    }
  }

  override def terminate(row: Row): Any = if (count.eval(row) == 0) null else divide.eval(row)
}

case class MaxFunction(aggr: BoundReference, base: Max) extends AggregateFunction {
  val arg: MutableLiteral = MutableLiteral(null, base.dataType)
  val buffer: MutableLiteral = MutableLiteral(null, base.dataType)
  val cmp = GreaterThan(arg, buffer)

  override def reset(buf: MutableRow): Unit = {
    buf.update(aggr.ordinal, null)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      arg.value = argument
      buffer.value = buf(aggr.ordinal)
      if (buf.isNullAt(aggr.ordinal) || cmp.eval(null) == true) {
        buf.update(aggr.ordinal, argument)
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (!value.isNullAt(aggr.ordinal)) {
      arg.value = value(aggr.ordinal)
      buffer.value = rowBuf(aggr.ordinal)
      if (rowBuf.isNullAt(aggr.ordinal) || cmp.eval(null) == true) {
        rowBuf.update(aggr.ordinal, arg.value)
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class CountFunction(aggr: BoundReference, base: Count)
    extends AggregateFunction {
  override def reset(buf: MutableRow): Unit = {
    buf.update(aggr.ordinal, 0L)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      if (buf.isNullAt(aggr.ordinal)) {
        buf.setLong(aggr.ordinal, 1L)
      } else {
        buf.update(aggr.ordinal, buf.getLong(aggr.ordinal) + 1L)
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (value.isNullAt(aggr.ordinal)) {
      // do nothing
    } else if (rowBuf.isNullAt(aggr.ordinal)) {
      rowBuf(aggr.ordinal) = value(aggr.ordinal)
    } else {
      rowBuf.update(aggr.ordinal, value.getLong(aggr.ordinal) + rowBuf.getLong(aggr.ordinal))
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class CountDistinctFunction(aggr: BoundReference, base: CountDistinct)
  extends AggregateFunction {
  override def reset(buf: MutableRow): Unit = {
    buf.update(aggr.ordinal, 0L)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (!argument.asInstanceOf[Seq[_]].exists(_ == null)) {
      // CountDistinct supports multiple expression, and ONLY IF
      // none of its expressions value equals null
      if (buf.isNullAt(aggr.ordinal)) {
        buf.setLong(aggr.ordinal, 1L)
      } else {
        buf.update(aggr.ordinal, buf.getLong(aggr.ordinal) + 1L)
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (value.isNullAt(aggr.ordinal)) {
      // do nothing
    } else if (rowBuf.isNullAt(aggr.ordinal)) {
      rowBuf(aggr.ordinal) = value(aggr.ordinal)
    } else {
      rowBuf.update(aggr.ordinal, value.getLong(aggr.ordinal) + rowBuf.getLong(aggr.ordinal))
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class SumFunction(aggr: BoundReference, base: Sum) extends AggregateFunction {
  val arg: MutableLiteral = MutableLiteral(null, base.dataType)
  val sum = Add(arg, aggr)

  override def reset(buf: MutableRow): Unit = {
    buf.update(aggr.ordinal, null)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      if (buf.isNullAt(aggr.ordinal)) {
        buf.update(aggr.ordinal, argument)
      } else {
        arg.value = argument
        buf.update(aggr.ordinal, sum.eval(buf))
      }
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (!value.isNullAt(aggr.ordinal)) {
      arg.value = value(aggr.ordinal)
      if (buf.isNullAt(aggr.ordinal)) {
        buf.update(aggr.ordinal, arg.value)
      } else {
        buf.update(aggr.ordinal, sum.eval(buf))
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class FirstFunction(aggr: BoundReference, base: First) extends AggregateFunction {
  override def reset(buf: MutableRow): Unit = {
    buf.update(aggr.ordinal, null)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (buf.isNullAt(aggr.ordinal)) {
      if (argument != null) {
        buf.update(aggr.ordinal, argument)
      }
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (buf.isNullAt(aggr.ordinal)) {
      if (!value.isNullAt(aggr.ordinal)) {
        buf.update(aggr.ordinal, value(aggr.ordinal))
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class LastFunction(aggr: BoundReference, base: AggregateExpression) extends AggregateFunction {
  override def reset(buf: MutableRow): Unit = {
    buf.update(aggr.ordinal, null)
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      buf.update(aggr.ordinal, argument)
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (!value.isNullAt(aggr.ordinal)) {
      buf.update(aggr.ordinal, value(aggr.ordinal))
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}
