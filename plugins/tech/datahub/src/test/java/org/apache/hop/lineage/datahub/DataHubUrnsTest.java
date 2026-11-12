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

package org.apache.hop.lineage.datahub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Dataset URNs have to be the ones DataHub's own sources give the same data. */
class DataHubUrnsTest {

  @Test
  void encodesWhatWouldSplitATupleUrn() {
    assertEquals("Read %28a%2C b%29 100%25", DataHubUrns.encode("Read (a, b) 100%"));
  }

  @Test
  void aLocalFileIsItsPath() {
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:file,/data/orders.csv,PROD)",
        DataHubUrns.datasetForOpenLineage("file", "/data/orders.csv", "PROD"));
  }

  @Test
  void anObjectIsNamedWithItsBucket() {
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/landing/orders.csv,PROD)",
        DataHubUrns.datasetForOpenLineage("s3://bucket", "landing/orders.csv", "PROD"));
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/orders.csv,PROD)",
        DataHubUrns.datasetForOpenLineage("gs://bucket", "orders.csv", "PROD"));
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:abs,container/orders.csv,PROD)",
        DataHubUrns.datasetForOpenLineage("abfss://container@account", "/orders.csv", "PROD"));
  }

  @Test
  void aTableIsItsQualifiedNameOnTheDatabasePlatform() {
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)",
        DataHubUrns.datasetForOpenLineage("postgres://db:5432", "analytics.public.orders", "PROD"));
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales.dbo.orders,DEV)",
        DataHubUrns.datasetForOpenLineage("sqlserver://db:1433", "sales.dbo.orders", "DEV"));
  }

  // A pipeline is its file in the project; outside a project the file as it is, and without a
  // file at all its name.
  @Test
  void aFlowIsItsFileInTheProject() {
    assertEquals(
        "pipelines/load.hpl", DataHubUrns.flowId("${PROJECT_HOME}/pipelines/load.hpl", "load"));
    assertEquals("/opt/etl/load.hwf", DataHubUrns.flowId("/opt/etl/load.hwf", "load"));
    assertEquals("load", DataHubUrns.flowId(null, "load"));
    assertEquals(
        "urn:li:dataFlow:(hop,pipelines/load.hpl,prod)",
        DataHubUrns.flowOfJob(
            "urn:li:dataJob:(urn:li:dataFlow:(hop,pipelines/load.hpl,prod),Read %28a%29)"));
  }

  @Test
  void anIncompleteIdentityHasNoUrn() {
    assertNull(DataHubUrns.datasetForOpenLineage("", "x", "PROD"));
    assertNull(DataHubUrns.datasetForOpenLineage("file", " ", "PROD"));
  }
}
