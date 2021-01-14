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
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatabaseImpactTest {

  private Class<?> PKG = Pipeline.class;

  @Test
  public void testGetRow() throws HopValueException {
    DatabaseImpact testObject =
      new DatabaseImpact( DatabaseImpact.TYPE_IMPACT_READ, "myPipeline", "aTransform", "ProdDB", "DimCustomer",
        "Customer_Key", "MyValue", "Calculator 2", "SELECT * FROM dimCustomer", "Some remarks" );
    RowMetaAndData rmd = testObject.getRow();

    assertNotNull( rmd );
    assertEquals( 10, rmd.size() );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 0 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Type" ), rmd.getValueMeta( 0 ).getName() );
    assertEquals( "Read", rmd.getString( 0, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 1 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Pipeline" ), rmd.getValueMeta( 1 )
      .getName() );
    assertEquals( "myPipeline", rmd.getString( 1, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 2 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Transform" ), rmd.getValueMeta( 2 ).getName() );
    assertEquals( "aTransform", rmd.getString( 2, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 3 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Database" ), rmd.getValueMeta( 3 )
      .getName() );
    assertEquals( "ProdDB", rmd.getString( 3, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 4 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Table" ), rmd.getValueMeta( 4 )
      .getName() );
    assertEquals( "DimCustomer", rmd.getString( 4, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 5 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Field" ), rmd.getValueMeta( 5 )
      .getName() );
    assertEquals( "Customer_Key", rmd.getString( 5, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 6 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Value" ), rmd.getValueMeta( 6 )
      .getName() );
    assertEquals( "MyValue", rmd.getString( 6, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 7 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.ValueOrigin" ), rmd.getValueMeta( 7 )
      .getName() );
    assertEquals( "Calculator 2", rmd.getString( 7, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 8 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.SQL" ), rmd.getValueMeta( 8 ).getName() );
    assertEquals( "SELECT * FROM dimCustomer", rmd.getString( 8, "default" ) );
    assertEquals( IValueMeta.TYPE_STRING, rmd.getValueMeta( 9 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "DatabaseImpact.RowDesc.Label.Remarks" ), rmd.getValueMeta( 9 )
      .getName() );
    assertEquals( "Some remarks", rmd.getString( 9, "default" ) );
  }
}
