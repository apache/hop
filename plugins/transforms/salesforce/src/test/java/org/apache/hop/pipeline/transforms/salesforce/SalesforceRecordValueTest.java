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

package org.apache.hop.pipeline.transforms.salesforce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.sforce.soap.partner.sobject.SObject;
import java.util.Date;
import org.junit.jupiter.api.Test;

public class SalesforceRecordValueTest {

  @Test
  public void testClass() {
    SalesforceRecordValue srv = new SalesforceRecordValue(100);
    assertEquals(100, srv.getRecordIndex());
    assertNull(srv.getRecordValue());
    assertFalse(srv.isRecordIndexChanges());
    assertFalse(srv.isAllRecordsProcessed());
    assertNull(srv.getDeletionDate());

    srv.setRecordIndex(120);
    assertEquals(120, srv.getRecordIndex());

    srv.setRecordValue(mock(SObject.class));
    assertNotNull(srv.getRecordValue());

    srv.setAllRecordsProcessed(true);
    assertTrue(srv.isAllRecordsProcessed());
    srv.setAllRecordsProcessed(false);
    assertFalse(srv.isRecordIndexChanges());

    srv.setRecordIndexChanges(true);
    assertTrue(srv.isRecordIndexChanges());
    srv.setRecordIndexChanges(false);
    assertFalse(srv.isRecordIndexChanges());

    srv.setDeletionDate(new Date());
    assertNotNull(srv.getDeletionDate());
  }
}
