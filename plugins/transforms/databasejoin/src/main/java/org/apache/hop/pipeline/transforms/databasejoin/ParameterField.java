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

package org.apache.hop.pipeline.transforms.databasejoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;
/** The parameters field. */
public class ParameterField implements Cloneable {

  /** The target field name */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "NAME",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.Parameter.Name")
  private String name;

  @HopMetadataProperty(
      key = "type",
      injectionKey = "TYPE",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.Parameter.Type")
  private String type = ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NONE);

  public ParameterField() {
    super();
  }

  public ParameterField(ParameterField cloned) {
    super();
    this.name = cloned.name;
    this.type = cloned.type;
  }

  @Override
  public Object clone() {
    return new ParameterField(this);
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = StringUtils.stripToNull(name);
  }

  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public void setType(int id) {
    this.type = ValueMetaFactory.getValueMetaName(id);
  }

  @Override
  public String toString() {
    return name + ":" + type;
  }
}
