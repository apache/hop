/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hop.pipeline.transforms.uniquerows;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class UniqueField {
  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "UniqueRowsMeta.Injection.Field.Name")
  private String name;

  @HopMetadataProperty(
      key = "case_insensitive",
      injectionKeyDescription = "UniqueRowsMeta.Injection.Field.CaseInsensitive")
  private boolean caseInsensitive;
  
  public UniqueField() {
    this.caseInsensitive = false;
  }
  
  public UniqueField(String name, boolean caseInsensitive) {
    this.name = name;
    this.caseInsensitive = caseInsensitive;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
  
  public boolean isCaseInsensitive() {
    return caseInsensitive;
  }

  public void setCaseInsensitive(boolean caseInsensitive) {
    this.caseInsensitive = caseInsensitive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UniqueField that = (UniqueField) o;
    return name.equals(that.name) && caseInsensitive==that.caseInsensitive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name,caseInsensitive);
  }
}
