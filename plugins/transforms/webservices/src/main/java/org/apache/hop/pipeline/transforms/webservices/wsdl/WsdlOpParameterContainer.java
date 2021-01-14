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

import org.apache.hop.pipeline.transforms.webservices.WebServiceMeta;

import javax.xml.namespace.QName;

public class WsdlOpParameterContainer implements WsdlParamContainer {
  private WsdlOpParameter parameter;

  public WsdlOpParameterContainer( WsdlOpParameter parameter ) {
    this.parameter = parameter;
  }

  public String getContainerName() {
    return parameter.getName().getLocalPart();
  }

  public String[] getParamNames() {
    if ( parameter.isArray() ) {
      if ( parameter.getItemComplexType() != null ) {
        return parameter.getItemComplexType().listObjectNames().toArray(
          new String[ parameter.getItemComplexType().listObjectNames().size() ] );
      } else {
        if ( parameter.getItemXmlType() != null ) {
          return new String[] { parameter.getItemXmlType().getLocalPart() };
        } else {
          return new String[] { parameter.getName().getLocalPart(), };
        }
      }
    } else {
      return new String[] { parameter.getName().getLocalPart() };
    }
  }

  public String getParamType( String paramName ) {
    if ( parameter.isArray() ) {
      if ( parameter.getItemComplexType() != null ) {
        QName name = parameter.getItemComplexType().getElementType( paramName );
        return name == null ? null : name.getLocalPart();
      } else {
        if ( parameter.getItemXmlType() != null ) {
          return parameter.getItemXmlType().getLocalPart();
        } else {
          return null;
        }
      }
    } else if ( paramName.equals( parameter.getName().getLocalPart() ) ) {
      return parameter.getXmlType().getLocalPart();
    } else {
      return null;
    }
  }

  public String getItemName() {
    if ( parameter.isArray() ) {
      if ( parameter.getItemXmlType() != null
        && !WebServiceMeta.XSD_NS_URI.equals( parameter.getItemXmlType().getNamespaceURI() ) ) {
        return parameter.getItemXmlType().getLocalPart();
      } else {
        return null;
      }
    } else {
      return parameter.getName().getLocalPart();
    }
  }

  public boolean isArray() {
    return parameter.isArray();
  }
}
