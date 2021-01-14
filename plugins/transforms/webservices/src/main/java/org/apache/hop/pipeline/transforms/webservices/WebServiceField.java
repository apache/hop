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

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transforms.webservices.wsdl.XsdType;

public class WebServiceField implements Cloneable {
  private String name;

  private String wsName;

  private String xsdType;

  public WebServiceField clone() {
    try {
      return (WebServiceField) super.clone();
    } catch ( CloneNotSupportedException e ) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String toString() {

    return name != null ? name : super.toString();
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getWsName() {
    return wsName;
  }

  public void setWsName( String wsName ) {
    this.wsName = wsName;
  }

  public String getXsdType() {
    return xsdType;
  }

  public void setXsdType( String xsdType ) {
    this.xsdType = xsdType;
  }

  public int getType() {
    return XsdType.xsdTypeToHopType( xsdType );
  }

  /**
   * We consider a field to be complex if it's a type we don't recognize. In that case, we will give back XML as a
   * string.
   *
   * @return
   */
  public boolean isComplex() {
    return getType() == IValueMeta.TYPE_NONE;
  }
}
