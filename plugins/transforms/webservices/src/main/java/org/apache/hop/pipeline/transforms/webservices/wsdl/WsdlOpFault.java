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

package org.apache.hop.pipeline.transforms.webservices.wsdl;

import javax.xml.namespace.QName;

/**
 * Wsdl operation fault abstraction.
 */
public final class WsdlOpFault extends WsdlOpReturnType implements java.io.Serializable {

  private static final long serialVersionUID = 1L;
  private final QName name;
  private final boolean isComplexType;
  private final boolean isElementFormQualified;

  /**
   * Create a new WsdlOpFault instance.
   *
   * @param name      QName of the parameter.
   * @param xmlType   XML type of the parameter.
   * @param wsdlTypes Wsdl type information.
   */
  protected WsdlOpFault( String name, QName xmlType, boolean isComplexType, WsdlTypes wsdlTypes ) {

    this.name = wsdlTypes.getTypeQName( name );
    this.isElementFormQualified = wsdlTypes.isElementFormQualified( this.name.getNamespaceURI() );
    this.xmlType = xmlType;
    this.isComplexType = isComplexType;
  }

  /**
   * Get the name of this fault.
   *
   * @return QName.
   */
  public QName getName() {
    return name;
  }

  /**
   * Is the XML type a complex type?
   *
   * @return true if xmltype is a complex type.
   */
  public boolean isComplexType() {
    return isComplexType;
  }

  /**
   * Is this element part of an element form qualifed schema?
   *
   * @return true if it is.
   */
  public boolean isFaultNameElementFormQualified() {
    return isElementFormQualified;
  }
}
