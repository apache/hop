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

package org.apache.hop.pipeline.transforms.webservices;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transforms.webservices.wsdl.XsdType;

@Getter
@Setter
public class WebServiceField implements Cloneable {
  @HopMetadataProperty(key = "name")
  private String name;

  @HopMetadataProperty(key = "wsName")
  private String wsName;

  @HopMetadataProperty(key = "xsdType")
  private String xsdType;

  public WebServiceField() {}

  public WebServiceField(WebServiceField f) {
    this();
    this.name = f.name;
    this.wsName = f.wsName;
    this.xsdType = f.xsdType;
  }

  @Override
  public WebServiceField clone() {
    return new WebServiceField(this);
  }

  @Override
  public String toString() {
    return name != null ? name : super.toString();
  }

  public int getType() {
    return XsdType.xsdTypeToHopType(xsdType);
  }

  /**
   * We consider a field to be complex if it's a type we don't recognize. In that case, we will give
   * back XML as a string.
   *
   * @return true if the field is complex
   */
  public boolean isComplex() {
    return getType() == IValueMeta.TYPE_NONE;
  }
}
