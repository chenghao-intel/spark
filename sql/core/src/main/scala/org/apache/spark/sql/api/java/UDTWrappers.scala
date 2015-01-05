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

package org.apache.spark.sql.api.java

import org.apache.spark.sql.catalyst.expressions.{MutableRow, UserDefinedData}
import org.apache.spark.sql.catalyst.types.{UserDefinedType => ScalaUserDefinedType}

/**
* Scala wrapper for a Java UserDefinedType
*/
private[sql] class JavaToScalaUDTWrapper
(val javaUDT: UserDefinedType) extends ScalaUserDefinedType(javaUDT.userClass()) {

  /** Convert the user type to a SQL datum */
  override def serialize(udd: UserDefinedData, mr: MutableRow): MutableRow = {
    javaUDT.serialize(udd, mr)
  }

  /** Convert a SQL datum to the user type */
  override def deserialize[T](datum: org.apache.spark.sql.catalyst.expressions.Row): T = {
    if (datum != null) {
      // TODO unnecessary converting
      javaUDT.deserialize(Row.create(datum)).asInstanceOf[T]
    } else {
      null.asInstanceOf[T]
    }
  }

  override def userClass: java.lang.Class[_ <: UserDefinedData] = javaUDT.userClass()
}

/**
* Java wrapper for a Scala UserDefinedType
*/
private[sql] class ScalaToJavaUDTWrapper
    (val scalaUDT: ScalaUserDefinedType) extends UserDefinedType(scalaUDT.userClass) {

  /** Convert the user type to a SQL datum */
  override def serialize(udd: UserDefinedData, mr: MutableRow): MutableRow = {
    scalaUDT.serialize(udd)
  }

  /** Convert a SQL datum to the user type */
  override def deserialize(datum: Row): UserDefinedData = if (datum == null) {
    null
  } else {
    scalaUDT.deserialize(datum.row)
  }

  override def userClass: java.lang.Class[_ <: UserDefinedData] = scalaUDT.userClass
}

private[sql] object UDTWrappers {
  def wrapAsScala(udtType: UserDefinedType): ScalaUserDefinedType = {
    udtType match {
      case t: ScalaToJavaUDTWrapper => t.scalaUDT
      case _ => new JavaToScalaUDTWrapper(udtType)
    }
  }

  def wrapAsJava(udtType: ScalaUserDefinedType): UserDefinedType = {
    udtType match {
      case t: JavaToScalaUDTWrapper => t.javaUDT
      case _ => new ScalaToJavaUDTWrapper(udtType)
    }
  }
}
