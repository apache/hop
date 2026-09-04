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
package org.apache.hop.database.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The column types Databricks generates. The full matrix lives in the golden file next to {@link
 * DatabricksFieldDefinitionGoldenTest}; what is here is the handful of decisions that file cannot
 * explain on its own.
 */
class DatabricksDatabaseMetaTest {

  private DatabricksDatabaseMeta nativeMeta;
  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void setUpClass() throws Exception {
    // DatabaseMeta resolves its dialect through the plugin registry.
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    nativeMeta = new DatabricksDatabaseMeta();
    nativeMeta.addDefaultOptions();
    // The engine reaches column types through DatabaseMeta, which is what consults the type rules.
    databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(nativeMeta);
  }

  /** The type name only, which is what the dialect decides. */
  private String type(IValueMeta valueMeta) {
    return databaseMeta.getFieldDefinition(valueMeta, null, null, false, false, false);
  }

  @Test
  void jsonBecomesVariant() {
    // Databricks holds semi structured data in a VARIANT rather than in text.
    assertEquals("VARIANT", type(new ValueMetaJson("COL")));
  }

  @Test
  void unsizedIntegerIsABigint() {
    // A Hop integer is a 64 bit long. Narrowing an unstated length down to TINYINT, as several
    // older dialects do, silently overflows on the first large value: issue #4174.
    assertEquals("BIGINT", type(new ValueMetaInteger("COL", -1, -1)));
    assertEquals("BIGINT", type(new ValueMetaInteger("COL", 0, 0)));
  }

  @Test
  void integerWidensWithItsDigits() {
    assertEquals("TINYINT", type(new ValueMetaInteger("COL", 2, 0)));
    assertEquals("SMALLINT", type(new ValueMetaInteger("COL", 4, 0)));
    assertEquals("INT", type(new ValueMetaInteger("COL", 9, 0)));
    assertEquals("BIGINT", type(new ValueMetaInteger("COL", 19, 0)));
    // Past what a BIGINT holds the column has to be a decimal.
    assertEquals("DECIMAL(20)", type(new ValueMetaInteger("COL", 20, 0)));
  }

  @Test
  void decimalIsClampedToWhatDatabricksCarries() {
    assertEquals("DECIMAL(10,2)", type(new ValueMetaBigNumber("COL", 10, 2)));
    assertEquals("DECIMAL(15,0)", type(new ValueMetaBigNumber("COL", 15, 0)));
    // An unstated size is the widest precision Databricks has, with the scale Spark picks itself.
    assertEquals("DECIMAL(38,18)", type(new ValueMetaBigNumber("COL", -1, -1)));
    // A length no DECIMAL can carry, which is what a CLOB sized field arrives as.
    assertEquals("DECIMAL(38,0)", type(new ValueMetaBigNumber("COL", DatabaseMeta.CLOB_LENGTH, 0)));
  }

  @Test
  void plainTypes() {
    // A Databricks STRING is unbounded, so a length has nothing to say.
    assertEquals("STRING", type(new ValueMetaString("COL", 15, 0)));
    assertEquals("STRING", type(new ValueMetaString("COL", DatabaseMeta.CLOB_LENGTH, 0)));
    assertEquals("DOUBLE", type(new ValueMetaNumber("COL", 10, 2)));
    assertEquals("BINARY", type(new ValueMetaBinary("COL")));
    // A Hop date carries a time of day, which the DATE column would drop.
    assertEquals("TIMESTAMP", type(new ValueMetaDate("COL")));
    assertEquals("TIMESTAMP", type(new ValueMetaTimestamp("COL")));
  }

  @Test
  void booleanIsAlwaysABoolean() {
    assertEquals("BOOLEAN", type(new ValueMetaBoolean("COL")));

    // Databricks has had a BOOLEAN type all along, so the option that lets a dialect without one
    // fall back to a single character column has nothing to say here.
    nativeMeta.setSupportsBooleanDataType(false);
    assertEquals("BOOLEAN", type(new ValueMetaBoolean("COL")));
  }

  @Test
  void aTypeDatabricksHasNoColumnForBecomesText() {
    // Nothing claims an address, so the fallback writes it the way Databricks can hold it.
    assertEquals("STRING", type(new ValueMetaInternetAddress("COL")));
  }

  @Test
  void keyColumnsAreIdentityBigints() {
    IValueMeta key = new ValueMetaInteger("ID", 9, 0);

    assertEquals(
        "BIGINT NOT NULL PRIMARY KEY",
        databaseMeta.getFieldDefinition(key, "ID", null, false, false, false));
    // Delta has no AUTO_INCREMENT; a generated key is an identity column.
    assertEquals(
        "BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY",
        databaseMeta.getFieldDefinition(key, "ID", null, true, false, false));
  }

  /**
   * Databricks bundles no driver, so the connection dialog's "Download driver" button is the only
   * way to get one - and that button only appears when the dialect declares a download.
   */
  @Test
  void aDownloadableDriverIsDeclared() {
    DriverDownload download = nativeMeta.getDriverDownload();
    assertNotNull(download);
    assertEquals("com.databricks:databricks-jdbc", download.getMavenCoordinate());
    // The 2.x line is the proprietary Simba driver; only 3.x is Apache-2.0 and freely downloadable.
    assertEquals("3", download.getDefaultVersion().split("\\.")[0]);
    assertFalse(download.isRestricted());
  }

  @Test
  void alterStatementsCarryTheColumnAndItsType() {
    assertEquals(
        "ALTER TABLE TBL ADD COLUMN COL VARIANT",
        databaseMeta
            .getAddColumnStatement("TBL", new ValueMetaJson("COL"), null, false, null, false)
            .trim());
    assertEquals(
        "ALTER TABLE TBL ALTER COLUMN COL TYPE STRING",
        databaseMeta
            .getModifyColumnStatement(
                "TBL", new ValueMetaString("COL", 15, 0), null, false, null, false)
            .trim());
  }
}
