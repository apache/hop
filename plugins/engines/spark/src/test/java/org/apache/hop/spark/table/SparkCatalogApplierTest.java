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

import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.spark.metadata.SparkCatalog;
import org.junit.jupiter.api.Test;

class SparkCatalogApplierTest {

  @Test
  void hadoopCatalogExpandsConf() throws Exception {
    SparkCatalog cat = new SparkCatalog();
    cat.setName("my-lake");
    cat.setCatalogName("lake");
    cat.setCatalogType(SparkCatalog.TYPE_HADOOP);
    cat.setWarehouse("/tmp/warehouse");
    cat.setConfExtra("io-impl=org.apache.iceberg.hadoop.HadoopFileIO\n# comment\n");

    Map<String, String> conf = SparkCatalogApplier.toSparkConfigs(cat, new Variables());
    assertEquals(SparkLakeFormats.ICEBERG_CATALOG, conf.get("spark.sql.catalog.lake"));
    assertEquals("hadoop", conf.get("spark.sql.catalog.lake.type"));
    assertTrue(conf.get("spark.sql.catalog.lake.warehouse").startsWith("file:"));
    assertEquals(
        "org.apache.iceberg.hadoop.HadoopFileIO", conf.get("spark.sql.catalog.lake.io-impl"));
  }

  @Test
  void restCatalogRequiresUri() {
    SparkCatalog cat = new SparkCatalog();
    cat.setName("rest");
    cat.setCatalogName("remote");
    cat.setCatalogType(SparkCatalog.TYPE_REST);
    assertThrows(
        HopException.class, () -> SparkCatalogApplier.toSparkConfigs(cat, new Variables()));
  }

  @Test
  void restCatalogWithToken() throws Exception {
    SparkCatalog cat = new SparkCatalog();
    cat.setName("rest");
    cat.setCatalogName("remote");
    cat.setCatalogType(SparkCatalog.TYPE_REST);
    cat.setUri("https://catalog.example.com/iceberg");
    cat.setCredential("secret-token");
    Map<String, String> conf = SparkCatalogApplier.toSparkConfigs(cat, new Variables());
    assertEquals("rest", conf.get("spark.sql.catalog.remote.type"));
    assertEquals("https://catalog.example.com/iceberg", conf.get("spark.sql.catalog.remote.uri"));
    assertEquals("secret-token", conf.get("spark.sql.catalog.remote.token"));
  }

  @Test
  void fullSparkKeyInConfExtra() throws Exception {
    SparkCatalog cat = new SparkCatalog();
    cat.setName("c");
    cat.setCatalogName("c");
    cat.setCatalogType(SparkCatalog.TYPE_CUSTOM);
    cat.setImplementation("com.example.MyCatalog");
    cat.setConfExtra("spark.sql.defaultCatalog=c");
    Map<String, String> conf = SparkCatalogApplier.toSparkConfigs(cat, new Variables());
    assertEquals("com.example.MyCatalog", conf.get("spark.sql.catalog.c"));
    assertEquals("c", conf.get("spark.sql.defaultCatalog"));
  }
}
