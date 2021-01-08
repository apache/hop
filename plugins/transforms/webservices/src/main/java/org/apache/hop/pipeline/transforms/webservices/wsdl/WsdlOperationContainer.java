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

import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOpParameter.ParameterMode;

import java.util.ArrayList;
import java.util.List;

public class WsdlOperationContainer implements WsdlParamContainer {

  private ParameterMode mode;

  private WsdlOperation operation;

  public WsdlOperationContainer( WsdlOperation operation, ParameterMode mode ) {
    this.mode = mode;
    this.operation = operation;
  }

  public String getContainerName() {
    return null;
  }

  public String[] getParamNames() {
    List<String> paramsRet = new ArrayList<>();
    for ( WsdlOpParameter param : operation.getParameters() ) {
      if ( param.getMode().equals( mode ) ) {
        paramsRet.add( param.getName().getLocalPart() );
      }
    }
    return paramsRet.toArray( new String[ paramsRet.size() ] );
  }

  public String getParamType( String paramName ) {
    String typeRet = null;
    for ( WsdlOpParameter param : operation.getParameters() ) {
      if ( param.getMode().equals( mode ) && param.getName().getLocalPart().equals( paramName ) ) {
        typeRet = param.getXmlType().getLocalPart();
        break;
      }
    }
    return typeRet;
  }

  public String getItemName() {
    // This method is only relevant for output containers
    return null;
  }

  public boolean isArray() {
    return false;
  }
}
