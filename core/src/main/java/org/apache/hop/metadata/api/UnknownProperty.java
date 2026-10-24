/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.metadata.api;

/** A property found during deserialization that does not map to any annotated field. */
public class UnknownProperty {
  private final String name;
  private final String value;

  public UnknownProperty(String name, String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * @return The name of the unknown property as found in the serialized data.
   */
  public String getName() {
    return name;
  }

  /**
   * @return The textual value of the unknown property as found in the serialized data.
   */
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "UnknownProperty{name='" + name + "', value='" + value + "'}";
  }
}
