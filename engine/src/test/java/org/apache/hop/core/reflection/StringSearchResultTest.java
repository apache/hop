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

package org.apache.hop.core.reflection;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StringSearchResultTest {

  private Class<?> PKG = Const.class;

  @Test
  public void testgetResultRowMeta() {
    IRowMeta rm = StringSearchResult.getResultRowMeta();
    assertNotNull( rm );
    assertEquals( 4, rm.getValueMetaList().size() );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( 0 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.PipelineOrWorkflow" ), rm.getValueMeta( 0 ).getName() );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( 1 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.TransformDatabaseNotice" ), rm.getValueMeta( 1 ).getName() );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( 2 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.String" ), rm.getValueMeta( 2 ).getName() );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( 3 ).getType() );
    assertEquals( BaseMessages.getString( PKG, "SearchResult.FieldName" ), rm.getValueMeta( 3 ).getName() );
  }
}
