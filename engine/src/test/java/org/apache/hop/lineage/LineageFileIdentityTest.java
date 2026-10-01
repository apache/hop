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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.lineage.LineageFileIdentity.Identity;
import org.junit.jupiter.api.Test;

/**
 * Locks the OpenLineage file/object-store naming rules so a physical file yields one stable {@code
 * (namespace, name)} identity regardless of which transform or engine emitted it.
 */
public class LineageFileIdentityTest {

  @Test
  public void localFileUsesFileNamespaceAndAbsolutePath() {
    assertEquals(
        new Identity("file", "/data/customers.csv"),
        LineageFileIdentity.of("file:///data/customers.csv"));
  }

  @Test
  public void s3BucketInNamespaceKeyWithoutLeadingSlash() {
    assertEquals(
        new Identity("s3://my-bucket", "warehouse/orders/part-0.parquet"),
        LineageFileIdentity.of("s3://my-bucket/warehouse/orders/part-0.parquet"));
  }

  @Test
  public void s3aAndS3nNormaliseToS3() {
    assertEquals(new Identity("s3://b", "k.csv"), LineageFileIdentity.of("s3a://b/k.csv"));
    assertEquals(new Identity("s3://b", "k.csv"), LineageFileIdentity.of("s3n://b/k.csv"));
  }

  @Test
  public void gcsBucketInNamespace() {
    assertEquals(
        new Identity("gs://bkt", "path/to/file"), LineageFileIdentity.of("gs://bkt/path/to/file"));
  }

  @Test
  public void hdfsHostPortInNamespacePathKept() {
    assertEquals(
        new Identity("hdfs://namenode:8020", "/warehouse/t/f"),
        LineageFileIdentity.of("hdfs://namenode:8020/warehouse/t/f"));
  }

  @Test
  public void azureContainerAndAccountKeptInNamespace() {
    assertEquals(
        new Identity("abfss://fs@acct.dfs.core.windows.net", "/dir/file"),
        LineageFileIdentity.of("abfss://fs@acct.dfs.core.windows.net/dir/file"));
  }

  // Credentials in a URI must never leak into the lineage namespace.
  @Test
  public void userInfoIsStrippedFromGenericSchemes() {
    assertEquals(
        new Identity("sftp://host:22", "/upload/data.txt"),
        LineageFileIdentity.of("sftp://user:secret@host:22/upload/data.txt"));
  }

  @Test
  public void schemeWithoutHostFallsBackToScheme() {
    assertEquals(new Identity("hdfs", "/p"), LineageFileIdentity.of("hdfs:/p"));
  }

  @Test
  public void blankOrSchemelessOrUnparsableYieldsNull() {
    assertNull(LineageFileIdentity.of(null));
    assertNull(LineageFileIdentity.of("   "));
    assertNull(LineageFileIdentity.of("/just/a/path"));
    assertNull(LineageFileIdentity.of("::::"));
  }

  // The invariant: identity is a pure function of the URI, so the same physical file emitted by
  // different transforms/engines reconciles onto one node.
  @Test
  public void sameUriAlwaysYieldsSameIdentity() {
    String uri = "s3://bucket/key/file.parquet";
    assertEquals(LineageFileIdentity.of(uri), LineageFileIdentity.of(uri));
  }
}
