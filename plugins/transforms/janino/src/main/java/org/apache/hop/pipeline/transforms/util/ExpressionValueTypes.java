/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.util;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

/** The Java type a field of the stream is handed to a Janino expression as. */
public class ExpressionValueTypes {

  private ExpressionValueTypes() {}

  /**
   * The type to declare an expression parameter with for a field of the stream.
   *
   * <p>The transform passes the value of the field as it is (in its normal storage type), so the
   * type has to be the class of that value: the value metadata of the field is the authority on
   * that. A String is a String, an Integer a Long, a Timestamp a java.sql.Timestamp, a JSON field a
   * JsonNode, and a type contributed by a plugin whatever class it declares.
   *
   * <p>A value type without a class of its own (a Serializable field) is passed as {@link Object},
   * so the expression can at least check it for null or call the methods of Object on it. Declaring
   * it as something it is not would fail when the expression is evaluated.
   *
   * @param valueMeta the metadata of the field
   * @return the class to declare the parameter with, never null
   */
  public static Class<?> javaTypeOf(IValueMeta valueMeta) {
    try {
      Class<?> nativeClass = valueMeta.getNativeDataTypeClass();
      return nativeClass == null ? Object.class : nativeClass;
    } catch (HopValueException e) {
      return Object.class;
    }
  }
}
