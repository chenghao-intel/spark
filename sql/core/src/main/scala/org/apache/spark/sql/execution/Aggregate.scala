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

package org.apache.spark.sql.execution

import scala.collection._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._

/**
 * An aggregate that needs to be computed for each row in a group.
 *
 * @param aggregate AggregateExpression, associated with the function
 * @param function Function of this aggregate used to process the aggregation.
 * @param substitution A MutableLiteral used to refer to the result of this aggregate in the final
 *                        output.
 */
sealed case class AggregateFunctionBind(
    aggregate: AggregateExpression,
    function: AggregateFunction,
    substitution: MutableLiteral)

sealed class KeyBufferSeens(
    var key: Row, //
    var buffer: MutableRow,
    var seens: Array[mutable.HashSet[Any]] = null) {
  def this() {
    this(null, null)
  }

  def withKey(row: Row): KeyBufferSeens = {
    this.key = row
    this
  }

  def withBuffer(row: MutableRow): KeyBufferSeens = {
    this.buffer = row
    this
  }

  def withSeens(seens: Array[mutable.HashSet[Any]]): KeyBufferSeens = {
    this.seens = seens
    this
  }
}

sealed trait Aggregate {
  self: Product =>
  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  val childOutput = child.output
  val isGlobalAggregation = groupingExpressions.isEmpty

  def computedAggregates: Seq[AggregateExpression] = {
    boundProjection.flatMap { expr =>
      expr.collect {
        case ae: AggregateExpression => ae
      }
    }
  }

  // This is a hack, instead of relying on the BindReferences for the aggregation
  // buffer schema in PostShuffle, we have a strong protocols which represented as the
  // BoundReferences in PostShuffle for aggregation buffer.
  @transient lazy val bufferSchema: Seq[AttributeReference] =
    computedAggregates.zipWithIndex.flatMap { case (ca, idx) =>
      ca.bufferDataType.zipWithIndex.map { case (dt, i) =>
        AttributeReference(s"aggr.${idx}_$i", dt)() }
    }

  // The tuples of aggregate expressions with information
  // (AggregateExpression, Aggregate Function, Placeholder of AggregateExpression result)
  @transient lazy val aggregateFunctionBinds: Seq[AggregateFunctionBind] = {
    var pos = 0
    computedAggregates.map { ae =>
      ae.initial(mode)

      // we connect all of the aggregation buffers in a single Row,
      // and "BIND" the attribute references in a Hack way.
      val bufferDataTypes = ae.bufferDataType
      val f = ae.newInstance(for (i <- 0 until bufferDataTypes.length) yield {
        BoundReference(pos + i, bufferDataTypes(i), true)
      })
      pos += bufferDataTypes.length

      AggregateFunctionBind(ae, f, MutableLiteral(null, ae.dataType))
    }
  }

  def groupByProjection = if (groupingExpressions.isEmpty) {
    InterpretedMutableProjection(Nil)
  } else {
    new InterpretedMutableProjection(groupingExpressions, childOutput)
  }

  // Indicate which stage we are running into
  def mode: Mode
  // This is provided by SparkPlan
  def child: SparkPlan
  // Group By Key Expressions
  def groupingExpressions: Seq[Expression]
  // Bounded Projection
  def boundProjection: Seq[NamedExpression]
}

sealed trait PreShuffle extends Aggregate {
  self: Product =>

  def boundProjection: Seq[NamedExpression] = projection.map {
    case a: Attribute => // Attribte will be converted into BoundReference
      Alias(
        BindReferences.bindReference(a: Expression, childOutput), a.name)(a.exprId, a.qualifiers)
    case a: NamedExpression => BindReferences.bindReference(a, childOutput)
  }

  // The expression list for output, this is the unbound expressions
  def projection: Seq[NamedExpression]
}

sealed trait PostShuffle extends Aggregate {
  self: Product =>
  /**
   * Substituted version of boundProjection expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  @transient lazy val finalExpressions = {
    val resultMap = aggregateFunctionBinds.map { ae => ae.aggregate -> ae.substitution }.toMap
    boundProjection.map { agg =>
      agg.transform {
        case e: AggregateExpression if resultMap.contains(e) => resultMap(e)
      }
    }
  }.map(e => {BindReferences.bindReference(e: Expression, childOutput)})

  @transient lazy val finalProjection = new InterpretedMutableProjection(finalExpressions)

  def aggregateFunctionBinds: Seq[AggregateFunctionBind]

  def appendNullRow(
      functions: Seq[AggregateFunction],
      results: mutable.HashMap[Row, KeyBufferSeens]) {
    if (isGlobalAggregation && results.isEmpty) {
      val currentRow = new GenericRow(childOutput.size)
      val buffer = new GenericMutableRow(bufferSchema.length)
      var idx = 0
      while (idx < functions.length) {
        val af = functions(idx)
        af.reset(buffer)
        idx += 1
      }
      results.put(Row(0), new KeyBufferSeens(currentRow, buffer))
    }
  }

  def createIterator(iterator: Iterator[KeyBufferSeens]) = {
    val functions = aggregateFunctionBinds.map(_.function)
    val substitutions = aggregateFunctionBinds.map(_.substitution)

    new Iterator[Row] {
      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val keybuffer = iterator.next()

        var idx = 0
        while (idx < functions.length) {
          // substitute the AggregateExpression value
          val af = functions(idx)
          substitutions(idx).value = af.terminate(keybuffer.buffer)
          idx += 1
        }

        finalProjection(keybuffer.key)
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `projection` for each
 * group.
 *
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param projection expressions that are computed for each group.
 * @param namedGroupingAttributes the attributes represent the output of the groupby expressions
 * @param child the input data source.
 */
@DeveloperApi
case class AggregatePreShuffle(
    groupingExpressions: Seq[Expression],
    projection: Seq[NamedExpression],
    namedGroupingAttributes: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode with PreShuffle {

  override def requiredChildDistribution = UnspecifiedDistribution :: Nil

  override def output = bufferSchema.map(_.toAttribute) ++ namedGroupingAttributes

  override def mode: Mode = PARTIAL1 // iterate & terminalPartial will be called

  /**
   * Create Iterator for the in-memory hash map.
   */
  private[this] def createIterator(
      iterator: Iterator[KeyBufferSeens]) = {
    val functions = aggregateFunctionBinds.map(_.function)

    new Iterator[Row] {
      private[this] val joinedRow = new JoinedRow

      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val keybuffer = iterator.next()
        var idx = 0
        while (idx < functions.length) {
          val af = functions(idx)
          af.terminatePartial(keybuffer.buffer)
          idx += 1
        }

        joinedRow(keybuffer.buffer, keybuffer.key)
      }
    }
  }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val functions = aggregateFunctionBinds.map(_.function)
      val aggregates = aggregateFunctionBinds.map(_.aggregate)
      val results = new mutable.HashMap[Row, MutableRow]()
      val keybuffer = new KeyBufferSeens()

      while (iter.hasNext) {
        val currentRow = iter.next()

        val keys = groupByProjection(currentRow)
        results.get(keys) match {
          case Some(group) =>
            var idx = 0
            while (idx < functions.length) {
              val af = functions(idx)
              val ae = aggregates(idx)
              af.iterate(ae.eval(currentRow), group)
              idx += 1
            }
          case None =>
            val buffer = new GenericMutableRow(bufferSchema.length)
            var idx = 0
            while (idx < functions.length) {
              val af = functions(idx)
              val ae = aggregates(idx)
              val value = ae.eval(currentRow)
              // TODO distinctLike? We need to store the "seen" for
              // AggregationExpression that distinctLike=true
              // This is a trade off between memory & computing
              af.reset(buffer)
              af.iterate(value, buffer)
              idx += 1
            }

            results.put(keys, new GenericMutableRow(buffer.toArray))
        }
      }

      // TODO maybe too expensive
      createIterator(results.iterator.map {
        case (key, buffer) => keybuffer.withKey(key).withBuffer(buffer)
      })
    }
  }
}

case class AggregatePostShuffle(
    groupingExpressions: Seq[Expression],
    boundProjection: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PostShuffle {

  override def output = boundProjection.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  override def mode: Mode = FINAL // merge & terminate will be called

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val functions = aggregateFunctionBinds.map(_.function)
      val results = new mutable.HashMap[Row, KeyBufferSeens]()

      while (iter.hasNext) {
        val currentRow = iter.next()
        val keys = groupByProjection(currentRow)
        results.get(keys) match {
          case Some(pair) =>
            var idx = 0
            while (idx < functions.length) {
              val af = functions(idx)
              af.merge(currentRow, pair.buffer)
              idx += 1
            }
          case None =>
            val buffer = new GenericMutableRow(bufferSchema.length)
            var idx = 0
            while (idx < functions.length) {
              val af = functions(idx)
              af.reset(buffer)
              af.merge(currentRow, buffer)
              idx += 1
            }
            results.put(keys, new KeyBufferSeens(currentRow, buffer))
        }
      }

      appendNullRow(functions, results)

      createIterator(results.valuesIterator)
    }
  }
}

// TODO Currently even if only a single DISTINCT exists in the aggregate expressions, we will
// not do partial aggregation (aggregating before shuffling), all of the data have to be shuffled
// to the reduce side and do aggregation directly, this probably causes the performance regression
// for Aggregation Function like CountDistinct etc.
case class DistinctAggregate(
    groupingExpressions: Seq[Expression],
    projection: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PreShuffle with PostShuffle {
  override def output = boundProjection.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  override def mode: Mode = COMPLETE // iterate() & terminate() will be called

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val functions = aggregateFunctionBinds.map(_.function)
      val aggregates = aggregateFunctionBinds.map(_.aggregate)
      val results = new mutable.HashMap[Row, KeyBufferSeens]()

      while (iter.hasNext) {
        val currentRow = iter.next()

        val keys = groupByProjection(currentRow)
        results.get(keys) match {
          case Some(keyBufferSeens) =>
            var idx = 0
            while (idx < aggregateFunctionBinds.length) {
              val ae = aggregates(idx)
              val af = functions(idx)
              val value = ae.eval(currentRow)

              if (ae.distinct) {
                if (!keyBufferSeens.seens(idx).contains(value)) {
                  af.iterate(value, keyBufferSeens.buffer)
                  keyBufferSeens.seens(idx).add(value)
                }
              } else {
                af.iterate(value, keyBufferSeens.buffer)
              }
              idx += 1
            }
          case None =>
            val temp = new GenericMutableRow(bufferSchema.length)
            // TODO save the memory only for those DISTINCT aggregate expressions
            val seens = new Array[mutable.HashSet[Any]](aggregateFunctionBinds.length)

            var idx = 0
            while (idx < aggregateFunctionBinds.length) {
              val ae = aggregates(idx)
              val af = functions(idx)
              val value = ae.eval(currentRow)
              af.reset(temp)
              af.iterate(value, temp)

              if (ae.distinct) {
                val seen = new mutable.HashSet[Any]()
                if (value != null) {
                  seen.add(value)
                }
                seens.update(idx, seen)
              }

              idx += 1
            }
            results.put(keys, new KeyBufferSeens(currentRow, temp, seens))
        }
      }

      appendNullRow(functions, results)
      createIterator(results.valuesIterator)
    }
  }
}
