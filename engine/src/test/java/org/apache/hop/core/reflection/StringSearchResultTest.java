/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.reflection;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.i18n.BaseMessages;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StringSearchResultTest {

  private Class<?> PKG = Const.class;

  @Test
  public void testgetResultRowMeta() {
    RowMetaInterface rm = StringSearchResult.getResultRowMeta();
    assertNotNull( rm );
    assertEquals( 4, rm.getValueMetaList().size() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 0 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.TransOrJob" ), rm.getValueMeta( 0 ).getName() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 1 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.StepDatabaseNotice" ), rm.getValueMeta( 1 ).getName() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 2 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.String" ), rm.getValueMeta( 2 ).getName() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 3 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.FieldName" ), rm.getValueMeta( 3 ).getName() );
  }
}
