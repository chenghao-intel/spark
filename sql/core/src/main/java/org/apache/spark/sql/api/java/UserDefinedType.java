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

package org.apache.spark.sql.api.java;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.catalyst.annotation.SQLUserDefinedType;
import org.apache.spark.sql.catalyst.expressions.MutableRow;
import org.apache.spark.sql.catalyst.expressions.UserDefinedData;
import org.apache.spark.sql.types.util.DataTypeConversions;

/**
 * ::DeveloperApi::
 * The data type representing User-Defined Types (UDTs).
 * UDTs may use any other DataType for an underlying representation.
 */
@DeveloperApi
public class UserDefinedType extends StructType {
    private Class<? extends UserDefinedData> clazz = null;

    protected UserDefinedType(Class<? extends UserDefinedData> clazz) {
        super(((StructType)
                (DataTypeConversions.asJavaDataType(
                   org.apache.spark.sql.catalyst.types.DataType.fromCaseClassString(
                     clazz.getAnnotation(SQLUserDefinedType.class).schema())))).getFields());
        this.clazz = clazz;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserDefinedType that = (UserDefinedType) o;
        if (clazz != that.clazz) return false;

        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + clazz.hashCode();
    }

    /** Convert the user type to a SQL datum */
    public MutableRow serialize(UserDefinedData obj, MutableRow mr) {
        return obj.store(mr);
    }

    /** Convert a SQL datum to the user type */
    public UserDefinedData deserialize(Row datum) {
        try {
            return (clazz.newInstance().load(datum.row()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Class object for the UserType */
    public Class<? extends UserDefinedData> userClass() {
        return clazz;
    }
}
