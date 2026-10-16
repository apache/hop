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

package org.apache.hop.pipeline.transforms.jdbcmetadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * An output field with a blank "Rename to" must keep the JDBC metadata column name. Up to 2.17 a
 * saved {@code <rename/>} loaded as null; since 2.18 it loads as an empty string, which must not
 * produce output fields without a name.
 */
class JdbcMetadataBlankRenameTest {

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void blankRenameKeepsOriginalFieldName() throws Exception {
    JdbcMetadataMeta meta = loadMeta("<rename/>", "<rename/>");

    IRowMeta row = new RowMeta();
    meta.getFields(row, "JDBC metadata", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"TABLE_NAME", "REMARKS"}, row.getFieldNames());
  }

  @Test
  void specifiedRenameIsApplied() throws Exception {
    JdbcMetadataMeta meta = loadMeta("<rename>tableName</rename>", "<rename>comment</rename>");

    IRowMeta row = new RowMeta();
    meta.getFields(row, "JDBC metadata", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"tableName", "comment"}, row.getFieldNames());
  }

  private static JdbcMetadataMeta loadMeta(String tableNameRename, String remarksRename)
      throws Exception {
    String xml =
        "<transform>"
            + "<connection/>"
            + "<methodName>getTables</methodName>"
            + "<alwaysPassInputRow>N</alwaysPassInputRow>"
            + "<argumentSourceFields>N</argumentSourceFields>"
            + "<removeArgumentFields>N</removeArgumentFields>"
            + "<arguments/>"
            + "<outputFields>"
            + "<outputField><name>TABLE_NAME</name>"
            + tableNameRename
            + "</outputField>"
            + "<outputField><name>REMARKS</name>"
            + remarksRename
            + "</outputField>"
            + "</outputFields>"
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    JdbcMetadataMeta meta = new JdbcMetadataMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
