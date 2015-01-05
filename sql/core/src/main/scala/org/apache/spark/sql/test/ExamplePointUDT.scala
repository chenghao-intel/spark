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

package org.apache.spark.sql.test

import org.apache.spark.sql.catalyst.expressions.{UserDefinedData, Row, GenericMutableRow, MutableRow}
import org.apache.spark.sql.catalyst.annotation.SQLUserDefinedType
import org.apache.spark.sql.catalyst.types._

/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 * @param x x coordinate
 * @param y y coordinate
 */
@SQLUserDefinedType(
  schema = """
        (StructField(x, DoubleType, true),
         StructField(y, DoubleType, true))""")
private[sql] class ExamplePoint(var x: Double, var y: Double) extends UserDefinedData {
  // We need default constructor for instance creation in UserDefinedType.deserialize
  def this() = this(ExamplePoint.INVALID, ExamplePoint.INVALID)
  override def newRow() = new GenericMutableRow(2)
  protected override def storeInternal(row: MutableRow): MutableRow = {
    if (x != ExamplePoint.INVALID && y != ExamplePoint.INVALID) {
      row.setDouble(0, x)
      row.setDouble(1, y)
    } else {
      row.setNullAt(0)
      row.setNullAt(1)
    }

    row
  }

  protected override def loadInternal(row: Row): this.type = {
    if (row.isNullAt(0) || row.isNullAt(1)) {
      null
    } else {
      x = row.getDouble(0)
      y = row.getDouble(1)
      this
    }
  }
}

private[sql] object ExamplePoint {
  val INVALID = Double.MinValue
}