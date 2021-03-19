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
 *
 */

package org.apache.hop.pipeline.transforms.splunkinput;

import org.apache.hop.core.injection.Injection;

import java.util.Objects;

public class ReturnValue {

  @Injection(name = "RETURN_NAME", group = "RETURNS")
  private String name;

  @Injection(name = "RETURN_SPLUNK_NAME", group = "RETURNS")
  private String splunkName;

  @Injection(name = "RETURN_TYPE", group = "RETURNS")
  private String type;

  @Injection(name = "RETURN_LENGTH", group = "RETURNS")
  private int length;

  @Injection(name = "RETURN_FORMAT", group = "RETURNS")
  private String format;

  public ReturnValue(String name, String splunkName, String type, int length, String format) {
    this.name = name;
    this.splunkName = splunkName;
    this.type = type;
    this.length = length;
    this.format = format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReturnValue that = (ReturnValue) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "ReturnValue{" + "name='" + name + '\'' + '}';
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets splunkName
   *
   * @return value of splunkName
   */
  public String getSplunkName() {
    return splunkName;
  }

  /** @param splunkName The splunkName to set */
  public void setSplunkName(String splunkName) {
    this.splunkName = splunkName;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets length
   *
   * @return value of length
   */
  public int getLength() {
    return length;
  }

  /** @param length The length to set */
  public void setLength(int length) {
    this.length = length;
  }

  /**
   * Gets format
   *
   * @return value of format
   */
  public String getFormat() {
    return format;
  }

  /** @param format The format to set */
  public void setFormat(String format) {
    this.format = format;
  }
}
