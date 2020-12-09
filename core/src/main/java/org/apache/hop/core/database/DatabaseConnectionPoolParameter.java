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

package org.apache.hop.core.database;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;

import java.util.ArrayList;
import java.util.List;

public class DatabaseConnectionPoolParameter {
  private String parameter;
  private String defaultValue;
  private String description;

  public DatabaseConnectionPoolParameter() {
  }

  /**
   * @param parameter
   * @param defaultValue
   * @param description
   */
  public DatabaseConnectionPoolParameter( String parameter, String defaultValue, String description ) {
    this();
    this.parameter = parameter;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  /**
   * @return the defaultValue
   */
  public String getDefaultValue() {
    return defaultValue;
  }

  /**
   * @param defaultValue the defaultValue to set
   */
  public void setDefaultValue( String defaultValue ) {
    this.defaultValue = defaultValue;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description the description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * @return the parameter
   */
  public String getParameter() {
    return parameter;
  }

  /**
   * @param parameter the parameter to set
   */
  public void setParameter( String parameter ) {
    this.parameter = parameter;
  }

  public static final String[] getParameterNames( DatabaseConnectionPoolParameter[] poolParameters ) {
    String[] names = new String[ poolParameters.length ];
    for ( int i = 0; i < names.length; i++ ) {
      names[ i ] = poolParameters[ i ].getParameter();
    }
    return names;
  }

  public static final DatabaseConnectionPoolParameter findParameter( String parameterName,
                                                                     DatabaseConnectionPoolParameter[] poolParameters ) {
    for ( int i = 0; i < poolParameters.length; i++ ) {
      if ( poolParameters[ i ].getParameter().equalsIgnoreCase( parameterName ) ) {
        return poolParameters[ i ];
      }
    }
    return null;
  }

  public static final List<RowMetaAndData> getRowList( DatabaseConnectionPoolParameter[] poolParameters,
                                                       String titleParameter, String titleDefaultValue, String titleDescription ) {
    IRowMeta rowMeta = new RowMeta();

    rowMeta.addValueMeta( new ValueMetaString( titleParameter ) );
    rowMeta.addValueMeta( new ValueMetaString( titleDefaultValue ) );
    rowMeta.addValueMeta( new ValueMetaString( titleDescription ) );

    List<RowMetaAndData> list = new ArrayList<>();

    for ( int i = 0; i < poolParameters.length; i++ ) {
      DatabaseConnectionPoolParameter p = poolParameters[ i ];

      Object[] row = new Object[ rowMeta.size() ];
      row[ 0 ] = p.getParameter();
      row[ 1 ] = p.getDefaultValue();
      row[ 2 ] = p.getDescription();

      list.add( new RowMetaAndData( rowMeta, row ) );
    }

    return list;
  }
}
