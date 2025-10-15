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

package org.apache.hop.pipeline.transforms.selectvalues;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class DeleteField implements Cloneable {

  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "SelectValues.Injection.REMOVE_NAME",
      injectionKey = "REMOVE_NAME")
  private String name;

  public DeleteField() {}

  public DeleteField(DeleteField f) {
    this();
    this.name = f.name;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DeleteField that)) return false;

    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }

  @Override
  protected DeleteField clone() throws CloneNotSupportedException {

    try {
      return (DeleteField) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }
}
