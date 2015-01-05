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

package org.apache.spark.sql

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.annotation.SQLUserDefinedType
import org.apache.spark.sql.catalyst.expressions.{UserDefinedData, GenericMutableRow, MutableRow}
import org.apache.spark.sql.catalyst.types.decimal.Decimal
import org.apache.spark.sql.catalyst.types.UserDefinedType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.TestSQLContext._

@SQLUserDefinedType(
  schema = "(StructField(data, ArrayType(DoubleType, false), true))")
private[sql] class MyDenseVector(var data: Array[Double]) extends UserDefinedData {
  def this() = this(null)

  override def equals(other: Any): Boolean = other match {
    case v: MyDenseVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }

  override def newRow(): MutableRow = new GenericMutableRow(1)

  protected override def storeInternal(row: MutableRow): MutableRow = {
    row.setList(0, data)

    row
  }

  protected override def loadInternal(row: Row): this.type = {
    if (row.isNullAt(0)) {
      data = null
    } else {
      data = row.getList(0).asInstanceOf[Seq[Double]].toArray
    }

    this
  }
}

@SQLUserDefinedType(
  schema = """
        (StructField(a, IntegerType, true),
         StructField(b, LongType, true),
         StructField(c, DoubleType, true),
         StructField(d, FloatType, true),
         StructField(e, BooleanType, true),
         StructField(f, ShortType, true),
         StructField(g, ByteType, true),
         StructField(h, StringType, true),
         StructField(i, DateType, true),
         StructField(j, TimestampType, true),
         StructField(k, BinaryType, true),
         StructField(l, DecimalType(10,2), true),
         StructField(m, ArrayType(IntegerType, false), true),
         StructField(n, MapType(IntegerType, IntegerType, true), true),
         StructField(o, UDT(org.apache.spark.sql.MyDenseVector), true))""")
private[sql] class ExampleData(
    var a: java.lang.Integer, var b: java.lang.Long, var c: java.lang.Double,
    var d: java.lang.Float, var e: java.lang.Boolean, var f: java.lang.Short,
    var g: java.lang.Byte, var h: String, var i: java.sql.Date,
    var j: java.sql.Timestamp, var k: Array[Byte], var l: Decimal,
    var m: List[Int], var n: Map[Int, Int], var o: MyDenseVector) extends UserDefinedData {
  def this() = this(null, null, null, null, null, null, null,
                    null, null, null, null, null, null, null, null)

  override def newRow(): MutableRow = new GenericMutableRow(15)

  protected override def storeInternal(cache: MutableRow): MutableRow = {
    cache.setInt(0, a)
    cache.setLong(1, b)
    cache.setDouble(2, c)
    cache.setFloat(3, d)
    cache.setBoolean(4, e)
    cache.setShort(5, f)
    cache.setByte(6, g)
    cache.setString(7, h)
    cache.setDate(8, i)
    cache.setTimestamp(9, j)
    cache.setBinary(10, k)
    cache.setDecimal(11, l)
    cache.setList(12, m)
    cache.setMap(13, n)
    cache.setRow(14, if (o == null) null else o.store(cache.getRow(14).asInstanceOf[MutableRow]))

    cache
  }

  protected override def loadInternal(row: Row): this.type = {
    a = if (row.isNullAt(0)) null else row.getInt(0)
    b = if (row.isNullAt(1)) null else row.getLong(1)
    c = if (row.isNullAt(2)) null else row.getDouble(2)
    d = if (row.isNullAt(3)) null else row.getFloat(3)
    e = if (row.isNullAt(4)) null else row.getBoolean(4)
    f = if (row.isNullAt(5)) null else row.getShort(5)
    g = if (row.isNullAt(6)) null else row.getByte(6)
    h = if (row.isNullAt(7)) null else row.getString(7)
    i = if (row.isNullAt(8)) null else row.getDate(8)
    j = if (row.isNullAt(9)) null else row.getTimestamp(9)
    k = if (row.isNullAt(10)) null else row.getBinary(10)
    l = if (row.isNullAt(11)) null else row.getDecimal(11)
    m = if (row.isNullAt(12)) null else row.getList(12).asInstanceOf[List[Int]]
    n = if (row.isNullAt(13)) null else row.getMap(13).asInstanceOf[Map[Int, Int]]
    o = if (row.isNullAt(14)) null else new MyDenseVector().load(row.getRow(14))

    this
  }

  override def equals(other: Any): Boolean = other match {
    case v: ExampleData =>
      a == v.a &&
      b == v.b &&
      c == v.c &&
      d == v.d &&
      e == v.e &&
      f == v.f &&
      g == v.g &&
      h == v.h &&
      i == v.i &&
      j == v.j &&
      java.util.Arrays.equals(k, v.k) &&
      l == v.l &&
      m == v.m &&
      n == v.n &&
      o == v.o
    case _ => false
  }
}

@SQLUserDefinedType(
  schema = """(StructField(label, DoubleType, true), StructField(features, UDT(org.apache.spark.sql.MyDenseVector), true))""")
private[sql] class MyLabeledPoint(
    var label: Double,
    var features: MyDenseVector) extends UserDefinedData {
  override def newRow() = new GenericMutableRow(2)
  protected override def storeInternal(cache: MutableRow): MutableRow = {
    cache.setDouble(0, label)
    cache.setRow(1, if (features == null) null else features.store(cache.getRow(1).asInstanceOf[MutableRow]))

    cache
  }

  protected def loadInternal(row: Row): this.type = {
    if (row.isNullAt(0)) {
      null
    } else {
      this.label = row.getDouble(0)
      this.features = if (row.isNullAt(1)) null else new MyDenseVector().load(row.getRow(1))
      this
    }
  }
}

class UserDefinedTypeSuite extends QueryTest {
  val points = Seq(
    new MyLabeledPoint(1.0, new MyDenseVector(Array(0.1, 1.0))),
    new MyLabeledPoint(0.0, new MyDenseVector(Array(0.2, 2.0))))
  val pointsRDD: RDD[MyLabeledPoint] = sparkContext.parallelize(points)

  val e1 = new ExampleData(
    1, 2l, 3.0d, 4.0f, true,
    6.asInstanceOf[Short], 7.asInstanceOf[Byte], "8",
    new java.sql.Date(9), new Timestamp(10), Array[Byte](1, 2, 3), Decimal(11.0),
    List(12, 13), Map(14->15, 16->17), new MyDenseVector(Array(0.1, 1.0)))
  val e2 = new ExampleData(11, 222l, 333.0d, 444.0f, false, 66.asInstanceOf[Short], 77.asInstanceOf[Byte], "88",
    new java.sql.Date(99), new Timestamp(100), Array[Byte](4, 5, 6), Decimal(1111.0),
    List(1222, 1333), Map(1444->1555, 1666->1777), null)

  val examples = e1 ::  e2 :: Nil

  test("register user type: MyDenseVector for MyLabeledPoint") {
    val labels: RDD[Double] = pointsRDD.select('label).map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val udt = new UserDefinedType(classOf[MyDenseVector])

    val features: RDD[MyDenseVector] =
      pointsRDD.select('features).map(row => udt.deserialize[MyDenseVector](row.getRow(0)))
    val featuresArrays: Array[MyDenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDenseVector(Array(0.2, 2.0))))
  }

  test("UDTs and UDFs") {
    val udt = new UserDefinedType(classOf[MyDenseVector])
    registerFunction("testType", (d: Row) => udt.deserialize[MyDenseVector](d) != null)
    pointsRDD.registerTempTable("points")
    checkAnswer(
      sql("SELECT testType(features) from points"),
      Seq(Row(true), Row(true)))
  }

  test("UDT in SchemaRDD") {
    val rdd: RDD[ExampleData] = sparkContext.parallelize(examples)
    rdd.registerTempTable("testudt")
    val udt = new UserDefinedType(classOf[ExampleData])

    val result = sql("select a + 10, b, c, d, e, f, g, h, i, j, k, l, m, " +
      "n, o from testudt where o IS NULL").collect().map(udt.deserialize[ExampleData])
    e2.a = e2.a + 10

    assert(result.length === 1)
    assert(result(0) === e2)
  }

  test("ExamplePoint UDT") {
    import org.apache.spark.sql.test.ExamplePoint
    val points = Seq(
      new ExamplePoint(1.0, 1.1),
      new ExamplePoint(2.0, 2.2))
    val pointsRDD: RDD[ExamplePoint] = sparkContext.parallelize(points)
    // implicit call SQLContext.createSchemaRDDFromUDD(), but will failed in compilation with apache/master
    pointsRDD.registerTempTable("points")

    val udt = new UserDefinedType(classOf[ExamplePoint])
    // udt.deserialize provides us a ability to convert the Row object into UserDefinedData when necessary.
    // The user application should know when/how to cast to a UserDefinedData object better than catalyst.
    val result = sql("select x+2, y+2 from points where x > 1.5").collect().map(udt.deserialize[ExamplePoint])
    assert(result.length === 1)
    assert(result(0).x === 4.0)
    assert(result(0).y === 4.2)
  }
}
