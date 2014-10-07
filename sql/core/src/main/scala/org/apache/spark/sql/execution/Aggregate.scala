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

sealed trait Aggregate {
  self: Product =>
  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  val childOutput = child.output

  def computedAggregates: Seq[AggregateExpression] = {
    aggregateExpressions.flatMap { expr =>
      expr.collect {
        case ae: AggregateExpression => ae
      }
    }
  }

  @transient lazy val aggregateFunctionBinds: Seq[AggregateFunctionBind] = {
    var pos = 0
    computedAggregates.map { ae =>
      val f = ae.newInstance(for (i <- 0 until ae.bufferDataType.length) yield {
        BoundReference(pos + i, ae.bufferDataType(i), true)
      })
      pos += ae.bufferDataType.length

      AggregateFunctionBind(ae, f, MutableLiteral(null, ae.dataType))
    }
  }

  def groupByProjection = if (groupingExpressions.isEmpty) {
    InterpretedMutableProjection(Nil)
  } else {
    new InterpretedMutableProjection(groupingExpressions, childOutput)
  }

  // This is provided by SparkPlan
  def child: SparkPlan
  def groupingExpressions: Seq[Expression]
  def aggregateExpressions: Seq[Expression]
}

sealed trait PreShuffle extends Aggregate {
  self: Product =>

  // This is a hack, instead of relying on the BindReferences for the aggregation buffer schema in PostShuffle,
  // we have a strong protocols which represented as the BoundReferences in PostShuffle for aggregation buffer.
  @transient lazy val buffersSchema: Seq[AttributeReference] =
    computedAggregates.zipWithIndex.flatMap { case (ca, idx) =>
      ca.bufferDataType.zipWithIndex.map { case (dt, i) =>
        AttributeReference(s"aggr.${idx}_$i", dt)() }
    }

  def aggregateExpressions = unboundAggregateExpressions.map(BindReferences.bindReference(_: Expression, childOutput))
  def computedAggregates: Seq[AggregateExpression]
  def unboundAggregateExpressions: Seq[NamedExpression]
}

sealed trait PostShuffle extends Aggregate {
  self: Product =>
  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  @transient lazy val finalExpressions = {
    val resultMap = aggregateFunctionBinds.map { ae => ae.aggregate -> ae.substitution }.toMap
    aggregateExpressions.map { agg =>
      agg.transform {
        case e: AggregateExpression if resultMap.contains(e) => resultMap(e)
      }
    }
  }.map(e => {BindReferences.bindReference(e: Expression, childOutput)})

  @transient lazy val finalProjection = new InterpretedMutableProjection(finalExpressions)

  def aggregateFunctionBinds: Seq[AggregateFunctionBind]
  def aggregateExpressions: Seq[Expression]
}

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param unboundAggregateExpressions expressions that are computed for each group.
 * @param namedGroupingAttributes the attributes represent the output of the groupby expressions
 * @param child the input data source.
 */
@DeveloperApi
case class AggregatePreShuffle(
    groupingExpressions: Seq[Expression],
    unboundAggregateExpressions: Seq[NamedExpression],
    namedGroupingAttributes: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode with PreShuffle {

  override def requiredChildDistribution = UnspecifiedDistribution :: Nil

  override def output = buffersSchema.map(_.toAttribute) ++ namedGroupingAttributes


  /**
   * Create Iterator for the in-memory hash map.
   */
  private[this] def createIterator(iterator: Iterator[(Row, MutableRow)]) =
    new Iterator[Row] {
      private[this] val joinedRow = new JoinedRow
      override final def hasNext: Boolean = iterator.hasNext
      override final def next(): Row = {
        val currentEntry = iterator.next()
        joinedRow(currentEntry._2, currentEntry._1)
      }
    }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val functions = aggregateFunctionBinds.map(_.function)
      val aggregates = aggregateFunctionBinds.map(_.aggregate)

      val results = new mutable.HashMap[Row, MutableRow]()
      val buffers = new GenericMutableRow(buffersSchema.length)

      while (iter.hasNext) {
        val currentRow = iter.next()
        var idx = 0
        while (idx < functions.length) {
          val af = functions(idx)
          val ae = aggregates(idx)
          val value = ae.eval(currentRow)
          // TODO distinctLike? We need to store the "seen" for AggregationExpression that distinctLike=true
          // This is a trade off between memory & computing
          af.reset(buffers)
          af.iterate(value, buffers)
          idx += 1
        }

        val keys = groupByProjection(currentRow)
        results.get(keys) match {
          case Some(group) =>
            var idx = 0
            while (idx < functions.length) {
              val af = functions(idx)

              af.merge(buffers, group)
              idx += 1
            }
          case None =>
            results.put(keys, new GenericMutableRow(buffers.toArray))
        }
      }

      createIterator(results.iterator)
    }
  }
}

case class AggregatePostShuffle(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PostShuffle {

  override def output = aggregateExpressions.map(_.toAttribute)
  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  def createIterator(iterator: Iterator[Row]) = {
    val functions = aggregateFunctionBinds.map(_.function)
    val substitutions = aggregateFunctionBinds.map(_.substitution)

    new Iterator[Row] {
      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val row = iterator.next()

        var idx = 0
        while (idx < functions.length) {
          val af = functions(idx)
          substitutions(idx).value = af.terminate(row)
          idx += 1
        }

        finalProjection(row)
      }
    }
  }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val functions = aggregateFunctionBinds.map(_.function)
      val results = new mutable.HashMap[Row, MutableRow]()

      while (iter.hasNext) {
        val currentRow = iter.next()
        val keys = groupByProjection(currentRow)
        results.get(keys) match {
          case Some(group) =>
            var idx = 0
            while (idx < functions.length) {
              val af = functions(idx)
              af.merge(currentRow, group)
              idx += 1
            }
          case None => results.put(keys, Row.toMutableRow(currentRow))
        }
      }

      createIterator(results.valuesIterator)
    }
  }
}

// TODO this probably will be removed when we support the distinct aggregation expression before shuffling.
case class DistinctAggregate(
    groupingExpressions: Seq[Expression],
    unboundAggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PreShuffle with PostShuffle {

  override def output = unboundAggregateExpressions.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  def createIterator(iterator: Iterator[(MutableRow, Array[mutable.HashSet[Any]])]) = {
    val functions = aggregateFunctionBinds.map(_.function)
    val substitutions = aggregateFunctionBinds.map(_.substitution)

    new Iterator[Row] {
      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val row = iterator.next()._1

        var idx = 0
        while (idx < functions.length) {
          val af = functions(idx)
          substitutions(idx).value = af.terminate(row)
          idx += 1
        }

        finalProjection(row)
      }
    }
  }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val functions = aggregateFunctionBinds.map(_.function)
      val aggregates = aggregateFunctionBinds.map(_.aggregate)
      val results = new mutable.HashMap[Row, (MutableRow, Array[mutable.HashSet[Any]])]()
      val buffers = new GenericMutableRow(buffersSchema.length)

      while (iter.hasNext) {
        val currentRow = iter.next()

        val keys = groupByProjection(currentRow)
        results.get(keys) match {
          case Some((group, seens)) =>
            var idx = 0
            while (idx < aggregateFunctionBinds.length) {
              val ae = aggregates(idx)
              val af = functions(idx)
              val value = ae.eval(currentRow)
              // TODO distinctLike? We need to store the "seen" for AggregationExpression that distinctLike=true
              // This is a trade off between memory & computing
              af.reset(buffers)
              af.iterate(value, buffers)

              if (ae.distinct) {
                if (!seens(idx).contains(value)) {
                  af.merge(buffers, group)
                  seens(idx).add(value)
                }
              } else {
                af.merge(buffers, group)
              }
              idx += 1
            }
          case None =>
            val temp = new GenericMutableRow(buffersSchema.length)
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
                seen.add(value)
                seens.update(idx, seen)
              }

              idx += 1
            }
            results.put(keys, (temp, seens))
        }
      }

      createIterator(results.valuesIterator)
    }
  }
}
