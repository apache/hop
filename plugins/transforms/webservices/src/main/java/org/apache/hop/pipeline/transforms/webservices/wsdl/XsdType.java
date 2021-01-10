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

import org.apache.hop.core.row.IValueMeta;

public class XsdType {
  public static final String DATE = "date";
  public static final String TIME = "time";
  public static final String DATE_TIME = "datetime";
  public static final String INTEGER = "int";
  public static final String INTEGER_DESC = "integer";
  public static final String SHORT = "short";
  public static final String BOOLEAN = "boolean";
  public static final String STRING = "string";
  public static final String DOUBLE = "double";
  public static final String FLOAT = "float";
  public static final String BINARY = "base64Binary";
  public static final String DECIMAL = "decimal";

  public static final String[] TYPES = new String[] {
    STRING, INTEGER, INTEGER_DESC, SHORT, BOOLEAN, DATE, TIME, DATE_TIME, DOUBLE, FLOAT, BINARY, DECIMAL, };

  public static int xsdTypeToHopType( String aXsdType ) {
    int vRet = IValueMeta.TYPE_NONE;
    if ( aXsdType != null ) {
      if ( aXsdType.equalsIgnoreCase( DATE ) ) {
        vRet = IValueMeta.TYPE_DATE;
      } else if ( aXsdType.equalsIgnoreCase( TIME ) ) {
        vRet = IValueMeta.TYPE_DATE;
      } else if ( aXsdType.equalsIgnoreCase( DATE_TIME ) ) {
        vRet = IValueMeta.TYPE_DATE;
      } else if ( aXsdType.equalsIgnoreCase( INTEGER ) || aXsdType.equalsIgnoreCase( INTEGER_DESC ) ) {
        vRet = IValueMeta.TYPE_INTEGER;
      } else if ( aXsdType.equalsIgnoreCase( SHORT ) ) {
        vRet = IValueMeta.TYPE_INTEGER;
      } else if ( aXsdType.equalsIgnoreCase( BOOLEAN ) ) {
        vRet = IValueMeta.TYPE_BOOLEAN;
      } else if ( aXsdType.equalsIgnoreCase( STRING ) ) {
        vRet = IValueMeta.TYPE_STRING;
      } else if ( aXsdType.equalsIgnoreCase( DOUBLE ) ) {
        vRet = IValueMeta.TYPE_NUMBER;
      } else if ( aXsdType.equalsIgnoreCase( BINARY ) ) {
        vRet = IValueMeta.TYPE_BINARY;
      } else if ( aXsdType.equalsIgnoreCase( DECIMAL ) ) {
        vRet = IValueMeta.TYPE_BIGNUMBER;
      } else {
        // When all else fails, map it to a String
        vRet = IValueMeta.TYPE_NONE;
      }
    }
    return vRet;
  }
}
