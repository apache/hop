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

package org.apache.hop.pipeline.transforms.coalesce;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Contains the properties of the inputs fields, target field name, target value type and options.
 *
 * @author Nicolas ADMENT
 */
public class CoalesceField implements Cloneable {

  /** The target field name */
  @HopMetadataProperty(key = "name", injectionKey="NAME", injectionKeyDescription = "CoalesceMeta.Injection.Field.Name")
  private String name;

  @HopMetadataProperty(key = "type",  injectionKey="TYPE", injectionKeyDescription = "CoalesceMeta.Injection.Field.Type")
  private String type = ValueMetaFactory.getValueMetaName(IValueMeta.TRIM_TYPE_NONE);

  @HopMetadataProperty(key = "remove", injectionKey="REMOVE_INPUT_FIELDS", injectionKeyDescription = "CoalesceMeta.Injection.Field.Remove")
  private boolean removeFields;
     
  @HopMetadataProperty(key = "input", injectionKey="INPUT_FIELDS",  injectionKeyDescription = "CoalesceMeta.Injection.Field.InputFields")
  private String inputFields;

  private List<String> cache = new ArrayList<>();

  public CoalesceField() {
    super();
  }

  public CoalesceField(CoalesceField cloned) {
    super();
    this.name = cloned.name;
    this.type = cloned.type;
    this.removeFields = cloned.removeFields;
    this.setInputFields(cloned.inputFields);
  }

  @Override
  public Object clone() {
    return new CoalesceField(this);
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = StringUtils.stripToNull(name);
  }

  public String getInputFields() {
    return this.inputFields;
  }

  public void setInputFields(final String fields) {
    this.inputFields = fields;
        
    // Rebuild cache
    cache = new ArrayList<>();
    if (inputFields != null) {
      for (String field : inputFields.split("\\s*,\\s*")) {
          this.cache.add(field);
      }
    }    
  }
  
  public List<String> getInputFieldNames() {
      return cache;
  }
    
  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  /**
   * Remove input fields
   *
   * @return
   */
  public boolean isRemoveFields() {
    return this.removeFields;
  }

  public void setRemoveFields(boolean remove) {
    this.removeFields = remove;
  }

  @Override
  public String toString() {
    return name + ":" + type;
  }
}
