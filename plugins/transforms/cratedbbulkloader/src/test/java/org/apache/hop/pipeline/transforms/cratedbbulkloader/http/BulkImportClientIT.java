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

package org.apache.hop.pipeline.transforms.cratedbbulkloader.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.http.exceptions.CrateDBHopException;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.http.exceptions.UnauthorizedCrateDBAccessException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.cratedb.CrateDBContainer;
import org.testcontainers.utility.MountableFile;

public class BulkImportClientIT {

  private static String CRATE_ENDPOINT;

  private static Connection connection;

  @ClassRule
  public static CrateDBContainer crateDBContainer =
      new CrateDBContainer("crate")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("crate.yml"), "/crate/config/crate.yml")
          .withExposedPorts(4200, 5432);

  @BeforeClass
  public static void setupAll() throws SQLException {
    CRATE_ENDPOINT =
        "http://"
            + crateDBContainer.getHost()
            + ":"
            + crateDBContainer.getMappedPort(4200)
            + "/_sql";
    connection = crateDBContainer.createConnection("");

    connection
        .createStatement()
        .execute("CREATE TABLE crate.foo (id INT PRIMARY KEY, name VARCHAR(10), description TEXT)");

    connection.createStatement().execute("CREATE USER alice WITH (password='password')");
    connection.createStatement().execute("GRANT ALL PRIVILEGES TO alice");

    connection.createStatement().execute("CREATE USER bob WITH (password='password')");
    connection.createStatement().execute("GRANT DQL ON SCHEMA crate TO bob");
  }

  @Test
  public void whenDataSizeGreaterThanMaxSize_shouldReturnRejectedRows()
      throws HopException, JsonProcessingException, CrateDBHopException {
    BulkImportClient client = new BulkImportClient(CRATE_ENDPOINT, "alice", "password");

    var response =
        client.batchInsert(
            "crate",
            "foo",
            new String[] {"id", "name", "description"},
            List.of(
                new Object[] {1, "Very Long Name", "This is Alice"},
                new Object[] {2, "Bob", "This is Bob"}));

    assertEquals(200, response.statusCode());
    assertEquals(0, response.outputRows());
    assertEquals(2, response.rejectedRows());
  }

  @Test
  public void whenRequestIsValid_shouldReturn200AndResult()
      throws HopException, JsonProcessingException, SQLException, CrateDBHopException {
    BulkImportClient client = new BulkImportClient(CRATE_ENDPOINT, "alice", "password");
    HttpBulkImportResponse response =
        client.batchInsert(
            "crate",
            "foo",
            new String[] {"id", "name", "description"},
            List.of(
                new Object[] {1, "Alice", "This is Alice"},
                new Object[] {2, "Bob", "This is Bob"}));

    connection.createStatement().execute("REFRESH TABLE crate.foo");
    ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM crate.foo");

    int size = 0;
    while (rs.next()) {
      size++;
    }
    assertEquals(200, response.statusCode());
    assertEquals(3, rs.getMetaData().getColumnCount());
    assertEquals(2, size);
  }

  @Test
  public void whenWrongPassword_shouldThrowUnauthorizedException()
      throws HopException, JsonProcessingException, SQLException, CrateDBHopException {
    BulkImportClient client = new BulkImportClient(CRATE_ENDPOINT, "alice", "wrongpassword");

    assertThrows(
        UnauthorizedCrateDBAccessException.class,
        () ->
            client.batchInsert(
                "crate",
                "foo",
                new String[] {"id", "name", "description"},
                List.of(
                    new Object[] {1, "Alice", "This is Alice"},
                    new Object[] {2, "Bob", "This is Bob"})));
  }

  @Test
  public void whenWrongUser_shouldThrowUnauthorizedException() {
    BulkImportClient client = new BulkImportClient(CRATE_ENDPOINT, "charlie", "apassword");

    assertThrows(
        UnauthorizedCrateDBAccessException.class,
        () ->
            client.batchInsert(
                "crate",
                "foo",
                new String[] {"id", "name", "description"},
                List.of(
                    new Object[] {1, "Alice", "This is Alice"},
                    new Object[] {2, "Bob", "This is Bob"})));
  }

  @Test
  public void whenUserNotAuthorized_shouldThrowUnauthorizedInsteadOfForbidden() {
    BulkImportClient client = new BulkImportClient(CRATE_ENDPOINT, "bob", "password");

    assertThrows(
        UnauthorizedCrateDBAccessException.class,
        () ->
            client.batchInsert(
                "crate",
                "foo",
                new String[] {"id", "name", "description"},
                List.of(
                    new Object[] {1, "Alice", "This is Alice"},
                    new Object[] {2, "Bob", "This is Bob"})));
  }
}
