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

package org.apache.hop.pipeline;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;

public class DatabaseImpact {
  private static final Class<?> PKG = Pipeline.class; // For Translator

  public static final int TYPE_IMPACT_NONE = 0;
  public static final int TYPE_IMPACT_READ = 1;
  public static final int TYPE_IMPACT_WRITE = 2;
  public static final int TYPE_IMPACT_READ_WRITE = 3;
  public static final int TYPE_IMPACT_TRUNCATE = 4;
  public static final int TYPE_IMPACT_DELETE = 5;
  public static final int TYPE_IMPACT_UPDATE = 6;

  public static final String[] typeDesc = {
    BaseMessages.getString( PKG, "DatabaseImpact.TypeDesc.Label.None" ),
    BaseMessages.getString( PKG, "DatabaseImpact.TypeDesc.Label.Read" ),
    BaseMessages.getString( PKG, "DatabaseImpact.TypeDesc.Label.Write" ),
    BaseMessages.getString( PKG, "DatabaseImpact.TypeDesc.Label.ReadOrWrite" ),
    BaseMessages.getString( PKG, "DatabaseImpact.TypeDesc.Label.Truncate" ),
    BaseMessages.getString( PKG, "DatabaseImpact.TypeDesc.Label.Delete" ),
    BaseMessages.getString( PKG, "DatabaseImpact.TypeDesc.Label.Update" ) };

  private String pipelineName;
  private String transformName;
  private String dbname;
  private String table;
  private String field;
  private String valuename;
  private String valueorigin;
  private String sql;
  private String remark;
  private int type;

  public DatabaseImpact( int type, String pipelineName, String transformName, String dbname, String table, String field,
                         String valuename, String valueorigin, String sql, String remark ) {
    this.type = type;
    this.pipelineName = pipelineName;
    this.transformName = transformName;
    this.dbname = dbname;
    this.table = table;
    this.field = field;
    this.valuename = valuename;
    this.valueorigin = valueorigin;
    this.sql = sql;
    this.remark = remark;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public String getTransformName() {
    return transformName;
  }

  public String getValueOrigin() {
    return valueorigin;
  }

  public String getDatabaseName() {
    return dbname;
  }

  public String getTable() {
    return table;
  }

  public String getField() {
    return field;
  }

  public String getValue() {
    return valuename;
  }

  public String getSql() {
    return sql;
  }

  public String getRemark() {
    return remark;
  }

  public String getTypeDesc() {
    return typeDesc[ type ];
  }

  public static final int getTypeDesc( String typedesc ) {
    for ( int i = 1; i < typeDesc.length; i++ ) {
      if ( typeDesc[ i ].equalsIgnoreCase( typedesc ) ) {
        return i;
      }
    }
    return TYPE_IMPACT_NONE;
  }

  public int getType() {
    return type;
  }

  public RowMetaAndData getRow() {
    RowMetaAndData r = new RowMetaAndData();
    r.addValue(
      new ValueMetaString(
        BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Type" ) ),
      getTypeDesc() );
    r.addValue( new ValueMetaString(
      BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Pipeline" ) ), getPipelineName() );
    r.addValue(
      new ValueMetaString(
        BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Transform" ) ),
      getTransformName() );
    r
      .addValue(
        new ValueMetaString(
          BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Database" ) ), getDatabaseName() );
    r.addValue(
      new ValueMetaString(
        BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Table" ) ),
      getTable() );
    r.addValue(
      new ValueMetaString(
        BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Field" ) ),
      getField() );
    r.addValue(
      new ValueMetaString(
        BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Value" ) ),
      getValue() );
    r.addValue(
      new ValueMetaString(
        BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.ValueOrigin" ) ), getValueOrigin() );
    r.addValue(
      new ValueMetaString(
        BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.SQL" ) ),
      getSql() );
    r
      .addValue(
        new ValueMetaString(
          BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Remarks" ) ), getRemark() );

    return r;
  }

}
