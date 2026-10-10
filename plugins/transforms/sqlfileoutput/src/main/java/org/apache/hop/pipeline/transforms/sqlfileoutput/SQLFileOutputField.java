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

package org.apache.hop.pipeline.transforms.sqlfileoutput;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class SQLFileOutputField {

  public SQLFileOutputField() {}

  public SQLFileOutputField(SQLFileOutputField f) {
    this.name = f.name;
    this.rename = f.rename;
  }

  /** Select: Name of the selected field */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "SQLFileOutput.Injection.FIELD_NAME")
  private String name;

  /** Select: Rename to ... */
  @HopMetadataProperty(
      key = "rename",
      injectionKey = "FIELD_RENAME",
      injectionKeyDescription = "SQLFileOutput.Injection.FIELD_RENAME")
  private String rename;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SQLFileOutputField that = (SQLFileOutputField) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getRename() {
    return rename;
  }
}
