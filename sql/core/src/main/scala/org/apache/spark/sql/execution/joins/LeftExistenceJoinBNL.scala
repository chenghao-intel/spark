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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftExistenceJoin, LeftSemi}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Using BroadcastNestedLoopJoin to calculate left existence join result when there's no join keys
 * for hash join.
 */
case class LeftExistenceJoinBNL(
    streamed: SparkPlan,
    broadcast: SparkPlan,
    condition: Option[Expression],
    jt: LeftExistenceJoin)
  extends BinaryNode {
  // TODO: Override requiredChildDistribution.

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def output: Seq[Attribute] = left.output

  /** The Streamed Relation */
  override def left: SparkPlan = streamed

  /** The Broadcast relation */
  override def right: SparkPlan = broadcast

  protected override def doExecute(): RDD[InternalRow] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")

    val broadcastedRelation =
      sparkContext.broadcast(broadcast.execute().map { row =>
        numRightRows += 1
        row.copy()
      }.collect().toIndexedSeq)

    streamed.execute().mapPartitions { streamedIter =>
      val joinedRow = new JoinedRow

      val criteria = jt match {
        case LeftSemi =>
          val boundCondition =
            newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)

          (streamedRow: InternalRow) => {
          var i = 0
          var matched = false

          while (i < broadcastedRelation.value.size && !matched) {
            val broadcastedRow = broadcastedRelation.value(i)
            // break only if we find the matched row
            if (boundCondition(joinedRow(streamedRow, broadcastedRow))) {
              matched = true
            }
            i += 1
          }
          matched
        }
        case LeftAnti =>
          // Since `null` will be casted into false in newPredicate expression,
          // we simply construct 2 Predicate expressions, to represent the matched / unmatched
          // conditions.
          val boundPosCondition =
            newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)

          val boundNegCondition = condition match {
            case Some(expr) => newPredicate(Not(expr), left.output ++ right.output)
            case None => newPredicate(Literal(false), left.output ++ right.output)
          }

          (streamedRow: InternalRow) => {
            var i = 0
            var falseCount = 0

            while (i < broadcastedRelation.value.size && falseCount >= 0) {
              val tmp = joinedRow(streamedRow, broadcastedRelation.value(i))
              if (boundPosCondition(tmp)) {
                // break since we found the value in the broadcast table.
                falseCount = -1
              } else if (boundNegCondition(tmp)) {
                // we found the unmatched value in the broadcast table
                falseCount += 1
              }
              i += 1
            }
            // we must found at least single unmatched row in the broadcast side.
            falseCount > 0
          }
      }

      streamedIter.filter(streamedRow => {
        numLeftRows += 1
        val matched = criteria(streamedRow)

        if (matched) {
          numOutputRows += 1
        }

        matched
      })
    }
  }
}
