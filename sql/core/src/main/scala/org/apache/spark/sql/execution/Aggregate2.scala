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

import java.util.HashMap

import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
/*
/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
@DeveloperApi
case class AggregatePreShuffle(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution = UnspecifiedDistribution :: Nil

  // We will convert the aggregation & groupby expressions results into column-based for each row:
  // * all of the non-distinct aggregation expressions will be in the same row
  // * each of the distinct aggregation expression result will be in a independent row
  //
  // And the schema of output tuple is (groupby exprs, index, values), if `index` less than 0, means
  // the row represent the non-distinct aggregation expression result, else it represents the
  // distinct aggregation expression result, and the `index` means the 0-based position
  // of the aggregation expressions in projection.
  //
  // for example:
  // the table "src" has data like
  // a     b     c
  // 1     11    "aa"
  // 1     11    "dd"
  // 1     22    "bb"
  // 2     33    "cc"
  // 3     44    "dd"
  //
  // And the SQL is SELECT a, AVG(DISTINCT(b)), MAX(c), SUM(DISTINCT(b)) FROM src GROUP BY a
  //
  // 1) the output tuples before the shuffle will be:
  // ((1), -1, (1, "aa"))   --> the value is a, MAX(c)
  // ((1), 1, (11))         --> the value is AVG(DISTINCT(b))
  // ((1), 3, (11))         --> the value is SUM(DISTINCT(b))
  // ((1), -1 (1, "dd"))
  // ((1), 1, (11))
  // ((1), 3, (11))
  // ((1), -1, (1, "bb))
  // ((1), 1, (22))
  // ((1), 3, (22))
  // ((2), -1, (2, "cc"))
  // ((2), 1, (33))
  // ((2), 3, (33))
  // ((3), -1, (3, "dd"))
  // ((3), 1, (44))
  // ((3), 3, (44))
  // 2) combine the output for the same groupby exprs.
  // 3) Shuffle the data with the partition key as (groupby exprs) with sorted key as (groupby exprs, index)
  // 4) Output the tuple according to the order of the projection
  override def output = aggregateExpressions.zipWithIndex.map { case (expr, idx) =>
    AttributeReference(s"buf_$idx", expr.bufferSchema, true)()
  } ++ groupingExpressions.map(_.toAttribute)

  lazy val computedSchema = aggregateExpressions.map(_.dataType) ++ groupingExpressions.map(_.dataType)

  /** Creates a new aggregate buffer for a group. */
  protected[this] def newAggregateBuffer(): Array[AggrBuffer] = {
    val buffer = new Array[AggrBuffer](aggregateExpressions.length)
    var i = 0
    while (i < aggregateExpressions.length) {
      buffer(i) = aggregateExpressions(i).newBuff()
      i += 1
    }
    buffer
  }

  override def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val distinctMap = Array.tabulate[OpenHashMap[Any, AggrBuffer]](aggregateExpressions.length)(i => new OpenHashMap[Any, AggrBuffer]())

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < aggregateExpressions.length) {
            val v = aggregateExpressions(i).eval(currentRow)
            var buffer = distinctMap(i)(v)
            if (buffer == null) {
              buffer = aggregateExpressions(i).newBuff()
              distinctMap(i).update(v, buffer)
            }


            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = 0
        while (i < buffer.length) {
          aggregateResults(i) = buffer(i).eval(EmptyRow)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[Row, Array[AggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[Row] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow4

          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): Row = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
@DeveloperApi
case class AggregatePostShuffle(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution = if (groupingExpressions == Nil) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  private[this] val childOutput = child.output

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
                                unbound: AggregateExpression,
                                aggregate: AggregateExpression,
                                resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, childOutput),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction] = {
    val buffer = new Array[AggregateFunction](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  override def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = 0
        while (i < buffer.length) {
          aggregateResults(i) = buffer(i).eval(EmptyRow)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[Row, Array[AggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[Row] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow4

          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): Row = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}
*/