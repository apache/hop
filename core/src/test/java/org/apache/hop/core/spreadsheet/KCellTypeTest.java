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

package org.apache.hop.core.spreadsheet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KCellTypeTest {

  @Test
  public void testEnums() {
    assertEquals( "Empty", KCellType.EMPTY.getDescription() );
    assertEquals( "Boolean", KCellType.BOOLEAN.getDescription() );
    assertEquals( "Boolean formula", KCellType.BOOLEAN_FORMULA.getDescription() );
    assertEquals( "Date", KCellType.DATE.getDescription() );
    assertEquals( "Date formula", KCellType.DATE_FORMULA.getDescription() );
    assertEquals( "Label", KCellType.LABEL.getDescription() );
    assertEquals( "String formula", KCellType.STRING_FORMULA.getDescription() );
    assertEquals( "Number", KCellType.NUMBER.getDescription() );
    assertEquals( "Number formula", KCellType.NUMBER_FORMULA.getDescription() );
  }
}
