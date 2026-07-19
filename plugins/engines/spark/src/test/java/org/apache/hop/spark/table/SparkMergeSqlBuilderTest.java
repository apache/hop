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

package org.apache.hop.spark.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class SparkMergeSqlBuilderTest {

  @Test
  void buildDefaultUpsert() throws Exception {
    String sql =
        SparkMergeSqlBuilder.build(
            "delta.`/tmp/t`",
            "hop_merge_src_Upsert",
            "t.id = s.id",
            SparkMergeSqlBuilder.MATCHED_UPDATE_ALL,
            SparkMergeSqlBuilder.NOT_MATCHED_INSERT_ALL,
            SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_NONE);
    assertTrue(sql.startsWith("MERGE INTO delta.`/tmp/t` AS t"));
    assertTrue(sql.contains("USING hop_merge_src_Upsert AS s"));
    assertTrue(sql.contains("ON t.id = s.id"));
    assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET *"));
    assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT *"));
    assertTrue(!sql.contains("NOT MATCHED BY SOURCE"));
  }

  @Test
  void buildDeleteMatchedOnly() throws Exception {
    String sql =
        SparkMergeSqlBuilder.build(
            "lake.db.t",
            "hop_merge_src_x",
            "t.k = s.k",
            SparkMergeSqlBuilder.MATCHED_DELETE,
            SparkMergeSqlBuilder.NOT_MATCHED_NONE,
            SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_NONE);
    assertTrue(sql.contains("WHEN MATCHED THEN DELETE"));
    assertTrue(!sql.contains("INSERT *"));
  }

  @Test
  void buildNotMatchedBySourceDelete() throws Exception {
    String sql =
        SparkMergeSqlBuilder.build(
            "delta.`/p`",
            "hop_merge_src_a",
            "t.id = s.id",
            SparkMergeSqlBuilder.MATCHED_UPDATE_ALL,
            SparkMergeSqlBuilder.NOT_MATCHED_INSERT_ALL,
            SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_DELETE);
    assertTrue(sql.contains("WHEN NOT MATCHED BY SOURCE THEN DELETE"));
  }

  @Test
  void rejectsEmptyCondition() {
    assertThrows(
        HopException.class,
        () ->
            SparkMergeSqlBuilder.build(
                "t",
                "hop_merge_src_a",
                "",
                SparkMergeSqlBuilder.MATCHED_UPDATE_ALL,
                SparkMergeSqlBuilder.NOT_MATCHED_INSERT_ALL,
                SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_NONE));
  }

  @Test
  void rejectsAllNoneActions() {
    assertThrows(
        HopException.class,
        () ->
            SparkMergeSqlBuilder.build(
                "t",
                "hop_merge_src_a",
                "t.id=s.id",
                SparkMergeSqlBuilder.MATCHED_NONE,
                SparkMergeSqlBuilder.NOT_MATCHED_NONE,
                SparkMergeSqlBuilder.NOT_MATCHED_BY_SOURCE_NONE));
  }

  @Test
  void sourceViewNameSanitizes() {
    assertEquals("hop_merge_src_My_Transform", SparkMergeSqlBuilder.sourceViewName("My Transform"));
    assertTrue(SparkMergeSqlBuilder.sourceViewName("123").startsWith("hop_merge_src_"));
  }
}
