/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.webservices.wsdl;

import javax.xml.namespace.QName;

/**
 * Represents the return value for a WSDL operation.
 */
public class WsdlOpReturnType implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  protected QName xmlType;

  protected boolean isArray;

  // The xmlType of an array's items
  protected QName itemXmlType;

  protected ComplexType itemComplexType;

  /**
   * Constructor.
   */
  protected WsdlOpReturnType() {
  }

  /**
   * Get the Xml type.
   *
   * @return QName for the XML type.
   */
  public QName getXmlType() {
    return xmlType;
  }

  /**
   * If the return type is an array, get the xml type of the items in the array.
   *
   * @return QName for the item XML type, null if not an array.
   */
  public QName getItemXmlType() {
    return itemXmlType;
  }

  /**
   * Is this an array type?
   *
   * @return true if this is an array type.
   */
  public boolean isArray() {
    return isArray;
  }

  public ComplexType getItemComplexType() {
    return itemComplexType;
  }
}
