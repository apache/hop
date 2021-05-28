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

package org.apache.hop.pipeline.transforms.injector;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class InjectorField {
  @HopMetadataProperty(key = "name")
  private String name;

  @HopMetadataProperty private String type;
  @HopMetadataProperty private String length;
  @HopMetadataProperty private String precision;

  public InjectorField() {}

  public InjectorField(String name, String type, String length, String precision) {
    this.name = name;
    this.type = type;
    this.length = length;
    this.precision = precision;
  }

  public InjectorField(InjectorField f) {
    this.name = f.name;
    this.type = f.type;
    this.length = f.length;
    this.precision = f.precision;
  }

  public IValueMeta createValueMeta(IVariables variables) throws HopException {
    String name = variables.resolve(this.name);
    int type = ValueMetaFactory.getIdForValueMeta(variables.resolve(this.type));
    int length = Const.toInt(variables.resolve(this.length), -1);
    int precision = Const.toInt(variables.resolve(this.precision), -1);
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, type, length, precision);
    return valueMeta;
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
  public String getLength() {
    return length;
  }

  /** @param length The length to set */
  public void setLength(String length) {
    this.length = length;
  }

  /**
   * Gets precision
   *
   * @return value of precision
   */
  public String getPrecision() {
    return precision;
  }

  /** @param precision The precision to set */
  public void setPrecision(String precision) {
    this.precision = precision;
  }
}
