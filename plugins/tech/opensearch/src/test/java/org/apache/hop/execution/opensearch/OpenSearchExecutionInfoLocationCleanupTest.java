/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.execution.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.execution.caching.CacheEntry;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

class OpenSearchExecutionInfoLocationCleanupTest {

  @Test
  void cleanupSqlUsesTheRequestedLimitAndCutoff() {
    Date cutoff = new Date(1_700_000_000_000L);
    String sql = OpenSearchExecutionInfoLocation.cleanupSql("hop-executions", "sales", cutoff, 200);

    assertTrue(sql.contains("FROM hop-executions"));
    assertTrue(sql.contains("projectId = 'sales'"));
    assertTrue(sql.contains("creationDate < datetime('"));
    assertTrue(sql.contains("creationDate IS NULL"));
    assertTrue(sql.contains("LIMIT 200"));
    assertFalse(sql.contains("LIMIT 50"));
    assertTrue(sql.contains(utcMinute(cutoff)));
  }

  @Test
  void refreshPathTargetsTheLocationIndex() {
    assertEquals(
        "hop-executions/_refresh", OpenSearchExecutionInfoLocation.refreshPath("hop-executions"));
  }

  @Test
  void documentPathWaitsUntilSearchCanSeeTheWrite() {
    assertEquals(
        "hop/_doc?refresh=wait_for", OpenSearchExecutionInfoLocation.documentPath("hop", null));
    assertEquals(
        "hop/_doc?refresh=wait_for", OpenSearchExecutionInfoLocation.documentPath("hop", ""));
    assertEquals(
        "hop/_doc/abc?refresh=wait_for",
        OpenSearchExecutionInfoLocation.documentPath("hop", "abc"));
  }

  @Test
  void searchHitKeepsTheOpenSearchDocumentId() throws Exception {
    JSONObject source = new JSONObject();
    source.put("id", "exec-1");
    source.put("name", "run");
    JSONObject hit = new JSONObject();
    hit.put("_id", "os-doc-7");
    hit.put("_source", source);

    CacheEntry entry = OpenSearchExecutionInfoLocation.cacheEntryFromSearchHit(hit);

    assertEquals("exec-1", entry.getId());
    assertEquals("run", entry.getName());
    assertEquals("os-doc-7", entry.getInternalId());
  }

  @Test
  void searchHitWithoutADocumentIdLeavesTheInternalIdEmpty() throws Exception {
    JSONObject source = new JSONObject();
    source.put("id", "exec-1");
    JSONObject hit = new JSONObject();
    hit.put("_source", source);

    CacheEntry entry = OpenSearchExecutionInfoLocation.cacheEntryFromSearchHit(hit);

    assertEquals("exec-1", entry.getId());
    assertNull(entry.getInternalId());
    assertNull(OpenSearchExecutionInfoLocation.cacheEntryFromSearchHit(null));
  }

  @Test
  void cleanupSqlWithoutACutoffStillPages() {
    String sql = OpenSearchExecutionInfoLocation.cleanupSql("hop-executions", "", null, 200);
    assertTrue(sql.contains("LIMIT 200"));
    assertFalse(sql.contains("creationDate <"));
    assertFalse(sql.contains("projectId"));
  }

  private static String utcMinute(Date date) {
    java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format.format(date);
  }
}
