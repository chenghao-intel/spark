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

import org.apache.spark.sql.types._


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
 * All of the function will be called within Spark executors.
 */
trait AggregateFunction {
  self: Product =>

  // Specify the BoundReference for Aggregate Buffer
  def initialBoundReference(buffers: Seq[BoundReference]): Unit

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
}

trait AggregateExpression extends Expression with AggregateFunction {
  self: Product =>
  type EvaluatedType = Any

  var mode: Mode = COMPLETE

  def initial(m: Mode): Unit = {
    this.mode = m
  }

  // Aggregation Buffer data types
  def bufferDataType: Seq[DataType] = Nil
  // Is it a distinct aggregate expression?
  def distinct: Boolean
  // Is it a distinct like aggregate expression (e.g. Min/Max is distinctLike, while avg is not)
  def distinctLike: Boolean = false

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

  override def dataType = child.dataType
  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def toString = s"MIN($child)"

  /* The below code will be called in executors, be sure to make the instance transientable */
  @transient var arg: MutableLiteral = _
  @transient var buffer: MutableLiteral = _
  @transient var cmp: LessThan = _
  @transient var aggr: BoundReference = _

  /* Initialization on executors */
  override def initialBoundReference(buffers: Seq[BoundReference]): Unit = {
    aggr = buffers(0)
    arg = MutableLiteral(null, dataType)
    buffer = MutableLiteral(null, dataType)
    cmp = LessThan(arg, buffer)
  }

  override def reset(buf: MutableRow): Unit = {
    buf(aggr) = null
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      arg.value = argument
      buffer.value = buf(aggr)
      if (buf.isNullAt(aggr) || cmp.eval(null) == true) {
        buf(aggr) = argument
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (!value.isNullAt(aggr)) {
      arg.value = value(aggr)
      buffer.value = rowBuf(aggr)
      if (rowBuf.isNullAt(aggr) || cmp.eval(null) == true) {
        rowBuf(aggr) = arg.value
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
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

  override def bufferDataType: Seq[DataType] = LongType :: dataType :: Nil
  override def toString = s"AVG($child)"

  /* The below code will be called in executors, be sure to mark the instance as transient */
  @transient var count: BoundReference = _
  @transient var sum: BoundReference = _

  // for iterate
  @transient var arg: MutableLiteral = _
  @transient var cast: Expression = _
  @transient var add: Add = _

  // for merge
  @transient var argInMerge: MutableLiteral = _
  @transient var addInMerge: Add = _

  // for terminate
  @transient var divide: Divide = _

  /* Initialization on executors */
  override def initialBoundReference(buffers: Seq[BoundReference]): Unit = {
    count = buffers(0)
    sum = buffers(1)

    arg = MutableLiteral(null, child.dataType)
    cast = if (arg.dataType != dataType) Cast(arg, dataType) else arg
    add = Add(cast, sum)

    argInMerge = MutableLiteral(null, dataType)
    addInMerge = Add(argInMerge, sum)

    divide = Divide(sum, Cast(count, dataType))
  }

  override def reset(buf: MutableRow): Unit = {
    buf(count) = 0L
    buf(sum) = null
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      arg.value = argument
      buf(count) = buf.getLong(count) + 1
      if (buf.isNullAt(sum)) {
        buf(sum) = cast.eval()
      } else {
        buf(sum) = add.eval(buf)
      }
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (!value.isNullAt(sum)) {
      buf(count) = value.getLong(count) + buf.getLong(count)
      if (buf.isNullAt(sum)) {
        buf(sum) = value(sum)
      } else {
        argInMerge.value = value(sum)
        buf(sum) = addInMerge.eval(buf)
      }
    }
  }

  override def terminate(row: Row): Any = if (count.eval(row) == 0) null else divide.eval(row)
}

case class Max(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = true
  override def dataType = child.dataType
  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def toString = s"MAX($child)"

  /* The below code will be called in executors, be sure to mark the instance as transient */
  @transient var aggr: BoundReference = _
  @transient var arg: MutableLiteral = _
  @transient var buffer: MutableLiteral = _
  @transient var cmp: GreaterThan = _

  override def initialBoundReference(buffers: Seq[BoundReference]) = {
    aggr = buffers(0)
    arg = MutableLiteral(null, dataType)
    buffer = MutableLiteral(null, dataType)
    cmp = GreaterThan(arg, buffer)
  }

  override def reset(buf: MutableRow): Unit = {
    buf(aggr) = null
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      arg.value = argument
      buffer.value = buf(aggr)
      if (buf.isNullAt(aggr) || cmp.eval(null) == true) {
        buf(aggr) = argument
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (!value.isNullAt(aggr)) {
      arg.value = value(aggr)
      buffer.value = rowBuf(aggr)
      if (rowBuf.isNullAt(aggr) || cmp.eval(null) == true) {
        rowBuf(aggr) = arg.value
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class Count(child: Expression)
  extends UnaryAggregateExpression {
  def distinct: Boolean = false
  override def nullable = false
  override def dataType = LongType
  override def bufferDataType: Seq[DataType] = LongType :: Nil
  override def toString = s"COUNT($child)"

  /* The below code will be called in executors, be sure to mark the instance as transient */
  @transient var aggr: BoundReference = _

  override def initialBoundReference(buffers: Seq[BoundReference]) = {
    aggr = buffers(0)
  }

  override def reset(buf: MutableRow): Unit = {
    buf(aggr) = 0L
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      if (buf.isNullAt(aggr)) {
        buf(aggr) = 1L
      } else {
        buf(aggr) = buf.getLong(aggr) + 1L
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (value.isNullAt(aggr)) {
      // do nothing
    } else if (rowBuf.isNullAt(aggr)) {
      rowBuf(aggr) = value(aggr)
    } else {
      rowBuf(aggr) = value.getLong(aggr) + rowBuf.getLong(aggr)
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class CountDistinct(children: Seq[Expression])
  extends AggregateExpression {
  def distinct: Boolean = true
  override def nullable = false
  override def dataType = LongType
  override def toString = s"COUNT($children)"
  override def bufferDataType: Seq[DataType] = LongType :: Nil

  /* The below code will be called in executors, be sure to mark the instance as transient */
  @transient var aggr: BoundReference = _
  override def initialBoundReference(buffers: Seq[BoundReference]) = {
    aggr = buffers(0)
  }

  override def reset(buf: MutableRow): Unit = {
    buf(aggr) = 0L
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (!argument.asInstanceOf[Seq[_]].exists(_ == null)) {
      // CountDistinct supports multiple expression, and ONLY IF
      // none of its expressions value equals null
      if (buf.isNullAt(aggr)) {
        buf(aggr) = 1L
      } else {
        buf(aggr) = buf.getLong(aggr) + 1L
      }
    }
  }

  override def merge(value: Row, rowBuf: MutableRow): Unit = {
    if (value.isNullAt(aggr)) {
      // do nothing
    } else if (rowBuf.isNullAt(aggr)) {
      rowBuf(aggr) = value(aggr)
    } else {
      rowBuf(aggr) = value.getLong(aggr) + rowBuf.getLong(aggr)
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
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

  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def toString = s"SUM($child)"

  /* The below code will be called in executors, be sure to mark the instance as transient */
  @transient var aggr: BoundReference = _
  @transient var arg: MutableLiteral = _
  @transient var sum: Add = _

  override def initialBoundReference(buffers: Seq[BoundReference]) = {
    aggr = buffers(0)
    arg = MutableLiteral(null, dataType)
    sum = Add(arg, aggr)
  }

  override def reset(buf: MutableRow): Unit = {
    buf(aggr) = null
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      if (buf.isNullAt(aggr)) {
        buf(aggr) = argument
      } else {
        arg.value = argument
        buf(aggr) = sum.eval(buf)
      }
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (!value.isNullAt(aggr)) {
      arg.value = value(aggr)
      if (buf.isNullAt(aggr)) {
        buf(aggr) = arg.value
      } else {
        buf(aggr) = sum.eval(buf)
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class First(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = true
  override def dataType = child.dataType
  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def toString = s"FIRST($child)"

  /* The below code will be called in executors, be sure to mark the instance as transient */
  @transient var aggr: BoundReference = _

  override def initialBoundReference(buffers: Seq[BoundReference]) = {
    aggr = buffers(0)
  }

  override def reset(buf: MutableRow): Unit = {
    buf(aggr) = null
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (buf.isNullAt(aggr)) {
      if (argument != null) {
        buf(aggr) = argument
      }
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (buf.isNullAt(aggr)) {
      if (!value.isNullAt(aggr)) {
        buf(aggr) = value(aggr)
      }
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}

case class Last(child: Expression, distinct: Boolean = false)
  extends UnaryAggregateExpression {
  override def nullable = true
  override def dataType = child.dataType
  override def bufferDataType: Seq[DataType] = dataType :: Nil
  override def toString = s"LAST($child)"

  /* The below code will be called in executors, be sure to mark the instance as transient */
  @transient var aggr: BoundReference = _

  override def initialBoundReference(buffers: Seq[BoundReference]) = {
    aggr = buffers(0)
  }

  override def reset(buf: MutableRow): Unit = {
    buf(aggr) = null
  }

  override def iterate(argument: Any, buf: MutableRow): Unit = {
    if (argument != null) {
      buf(aggr) = argument
    }
  }

  override def merge(value: Row, buf: MutableRow): Unit = {
    if (!value.isNullAt(aggr)) {
      buf(aggr) = value(aggr)
    }
  }

  override def terminate(row: Row): Any = aggr.eval(row)
}
