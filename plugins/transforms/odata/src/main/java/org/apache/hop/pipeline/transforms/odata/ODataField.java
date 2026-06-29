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

package org.apache.hop.pipeline.transforms.odata;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class ODataField {
  @HopMetadataProperty(key = "name")
  private String name;

  @HopMetadataProperty(key = "path")
  private String path;

  @HopMetadataProperty(key = "type", intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class)
  private int type;

  @HopMetadataProperty(key = "format")
  private String format;

  public ODataField() {
    this.name = "";
    this.path = "";
    this.type = IValueMeta.TYPE_STRING;
    this.format = "";
  }

  public ODataField(String name, String path, int type, String format) {
    this.name = name;
    this.path = path;
    this.type = type;
    this.format = format;
  }

  public ODataField(ODataField other) {
    this.name = other.name;
    this.path = other.path;
    this.type = other.type;
    this.format = other.format;
  }

  @Override
  public ODataField clone() {
    return new ODataField(this);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ODataField other)) {
      return false;
    }
    return Objects.equals(name, other.name)
        && Objects.equals(path, other.path)
        && type == other.type
        && Objects.equals(format, other.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, path, type, format);
  }

  public IValueMeta toValueMeta(String originTransformName, IVariables variables)
      throws HopPluginException {
    int hopType = getType();
    if (hopType == IValueMeta.TYPE_NONE) {
      hopType = IValueMeta.TYPE_STRING;
    }
    IValueMeta v =
        ValueMetaFactory.createValueMeta(
            variables != null ? variables.resolve(getName()) : getName(), hopType);
    v.setOrigin(originTransformName);
    v.setConversionMask(getFormat());
    return v;
  }
}
