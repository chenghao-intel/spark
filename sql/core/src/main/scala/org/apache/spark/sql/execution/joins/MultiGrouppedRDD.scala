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

import org.apache.spark._

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.{NarrowCoGroupSplitDep, ShuffleCoGroupSplitDep}
import org.apache.spark.rdd.{CoGroupPartition, RDD}

import org.apache.spark.sql.catalyst.expressions.Row

// A version of CoGroupedRDD with the following changes:
// - Disable map-side aggregation.
// - Enforce return type to Array[ArrayBuffer].
class SQLCoGroupedRDD[K](@transient var rdds: Seq[RDD[(_, _)]], part: Partitioner)
  extends RDD[(Any, Array[ArrayBuffer[Row]])](rdds.head.context, Nil) with Logging {

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[Any, Any, Any](rdd, part, None)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (r, j) =>
        dependencies(j) match {
          case s: ShuffleDependency =>
            ShuffleCoGroupSplitDep(s.shuffleHandle)
          case _ =>
            NarrowCoGroupSplitDep(r, i, r.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(s: Partition, context: TaskContext)
  : Iterator[(Any, Array[ArrayBuffer[Row]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size
    val map = new JHashMap[Any, Array[ArrayBuffer[Row]]]

    def getSeq(key: Any): Array[ArrayBuffer[Row]] = {
      var values = map.get(key)
      if (values == null) {
        values = Array.fill(numRdds)(new ArrayBuffer[Row])
        map.put(key, values)
      }
      values
    }

    def mergePair(depIdx: Int, key: Any, value: Row) { getSeq(key)(depIdx) += value }

    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, itsSplitIndex, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- rdd.iterator(itsSplit, context)) { getSeq(k)(depNum) += v.asInstanceOf[Row] }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Get the shuffle fetcher
        val fetcher =
          SparkEnv.get.shuffleManager.getReader(shuffleId, split.index, split.index + 1, context)

        // TODO external merge sorting https://github.com/apache/spark/pull/3438
        // We can change the implementation by using the sort merge join for performance concerns
        // which also will reduce the memory usage for caching all of the partition data
        fetcher.read().foreach { case (key, value) =>
          mergePair(depNum, key, value)
        }
      }
    }

    new InterruptibleIterator(context, map.iterator)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}