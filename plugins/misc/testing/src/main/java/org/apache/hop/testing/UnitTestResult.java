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

package org.apache.hop.testing;

import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;

import java.util.ArrayList;
import java.util.List;

public class UnitTestResult {
  private static final Class<?> PKG = UnitTestResult.class; // For Translator

  private String pipelineName;
  private String unitTestName;
  private String dataSetName;
  private String transformName;
  private boolean error;
  private String comment;

  public UnitTestResult() {
    super();
  }

  public UnitTestResult( String pipelineName, String unitTestName, String dataSetName, String transformName, boolean error, String comment ) {
    super();
    this.pipelineName = pipelineName;
    this.unitTestName = unitTestName;
    this.dataSetName = dataSetName;
    this.transformName = transformName;
    this.error = error;
    this.comment = comment;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public void setPipelineName( String pipelineName ) {
    this.pipelineName = pipelineName;
  }

  public String getUnitTestName() {
    return unitTestName;
  }

  public void setUnitTestName( String unitTestName ) {
    this.unitTestName = unitTestName;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public void setDataSetName( String dataSetName ) {
    this.dataSetName = dataSetName;
  }

  public String getTransformName() {
    return transformName;
  }

  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  public boolean isError() {
    return error;
  }

  public void setError( boolean error ) {
    this.error = error;
  }

  public String getComment() {
    return comment;
  }

  public void setComment( String comment ) {
    this.comment = comment;
  }

  public static final IRowMeta getRowMeta() {
    IRowMeta rowMeta = new RowMeta();

    rowMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "UnitTestResult.FieldName.PipelineName" ) ) );
    rowMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "UnitTestResult.FieldName.UnitTestName" ) ) );
    rowMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "UnitTestResult.FieldName.DataSetName" ) ) );
    rowMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "UnitTestResult.FieldName.TransformName" ) ) );
    rowMeta.addValueMeta( new ValueMetaBoolean( BaseMessages.getString( PKG, "UnitTestResult.FieldName.Error" ) ) );
    rowMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "UnitTestResult.FieldName.Comment" ) ) );

    return rowMeta;
  }

  public static final List<Object[]> getRowData( List<UnitTestResult> results ) {
    List<Object[]> rows = new ArrayList<>();
    IRowMeta rowMeta = getRowMeta();
    for ( UnitTestResult result : results ) {
      int index = 0;
      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      row[ index++ ] = result.getPipelineName();
      row[ index++ ] = result.getUnitTestName();
      row[ index++ ] = result.getDataSetName();
      row[ index++ ] = result.getTransformName();
      row[ index++ ] = result.isError();
      row[ index++ ] = result.getComment();
      rows.add( row );
    }
    return rows;
  }

}
