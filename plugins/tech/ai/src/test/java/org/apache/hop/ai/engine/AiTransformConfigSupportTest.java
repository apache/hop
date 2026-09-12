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

package org.apache.hop.ai.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.junit.jupiter.api.Test;

class AiTransformConfigSupportTest {

  @Test
  void overlaysSqlConnectionAndNestedFileName() throws Exception {
    SampleMeta meta = new SampleMeta();
    meta.file = new SampleFile();
    AiProposal proposal = new AiProposal();
    proposal.setType("ADD_TRANSFORM");
    proposal.setParameters(
        Map.of(
            "transformPluginId",
            "TableInput",
            "name",
            "Read current customers",
            "locationX",
            "100",
            "locationY",
            "80",
            "sql",
            "SELECT customer_id FROM d_customer",
            "connection",
            "test_edw",
            "filename",
            "${PROJECT_HOME}/output/current_customers.xlsx",
            "sheet",
            "current_customers",
            "header",
            "Y"));

    AiTransformConfigSupport.apply(meta, proposal);

    assertEquals("SELECT customer_id FROM d_customer", meta.getSql());
    assertEquals("test_edw", meta.getConnection());
    assertEquals("${PROJECT_HOME}/output/current_customers.xlsx", meta.getFile().getFileName());
    assertEquals("current_customers", meta.getFile().getSheetname());
    assertTrue(meta.isHeader());
  }

  @Test
  void configJsonIsFlattenedThenOverlaid() throws Exception {
    SampleMeta meta = new SampleMeta();
    meta.file = new SampleFile();
    AiProposal proposal = new AiProposal();
    proposal.setParameters(
        Map.of("config", "{\"sql\":\"SELECT 1\",\"file\":{\"name\":\"out.xlsx\"}}"));

    AiTransformConfigSupport.apply(meta, proposal);

    assertEquals("SELECT 1", meta.getSql());
    assertEquals("out.xlsx", meta.getFile().getFileName());
  }

  public static class SampleMeta {
    @HopMetadataProperty(key = "sql")
    private String sql;

    @HopMetadataProperty private String connection;

    @HopMetadataProperty(key = "header")
    private boolean header;

    @HopMetadataProperty private SampleFile file;

    public String getSql() {
      return sql;
    }

    public void setSql(String sql) {
      this.sql = sql;
    }

    public String getConnection() {
      return connection;
    }

    public void setConnection(String connection) {
      this.connection = connection;
    }

    public boolean isHeader() {
      return header;
    }

    public void setHeader(boolean header) {
      this.header = header;
    }

    public SampleFile getFile() {
      return file;
    }

    public void setFile(SampleFile file) {
      this.file = file;
    }
  }

  public static class SampleFile {
    @HopMetadataProperty(key = "name")
    private String fileName;

    @HopMetadataProperty private String sheetname;

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public String getSheetname() {
      return sheetname;
    }

    public void setSheetname(String sheetname) {
      this.sheetname = sheetname;
    }
  }
}
