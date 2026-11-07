/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.mql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ActionMqlTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private SerializableMetadataProvider metadataProvider;

  @Test
  void testConstructorAndClone() {
    ActionMql action = new ActionMql("test-mql-action");
    action.setMql("{ ping: 1 }");
    action.setConnection("test-connection");
    action.setUseVariableSubstitution(true);

    ActionMql clone = (ActionMql) action.clone();

    assertNotNull(clone);
    assertEquals("test-mql-action", clone.getName());
    assertEquals("{ ping: 1 }", clone.getMql());
    assertEquals("test-connection", clone.getConnection());
    assertTrue(clone.isUseVariableSubstitution());
  }

  @Test
  void testExecuteFailsWhenNoConnectionName() {
    ActionMql action = new ActionMql("test-mql");
    action.setMetadataProvider(metadataProvider);
    Result result = new Result();

    action.execute(result, 0);
    assertEquals(1, result.getNrErrors());
    assertFalse(result.isResult());
  }

  @Test
  void testExecuteFailsWhenConnectionNotFound() {
    ActionMql action = new ActionMql("test-mql");
    action.setConnection("non-existent-connection");
    action.setMetadataProvider(metadataProvider);
    Result result = new Result();

    action.execute(result, 0);
    assertEquals(1, result.getNrErrors());
    assertFalse(result.isResult());
  }

  @Test
  void testExecuteFailsWhenMqlIsEmpty() {
    ActionMql action = new ActionMql("test-mql");
    action.setConnection("test-connection");
    action.setUseExternalFile(false);
    action.setMql("");
    action.setMetadataProvider(metadataProvider);
    Result result = new Result();

    action.execute(result, 0);
    assertEquals(1, result.getNrErrors());
    assertFalse(result.isResult());
  }

  @Test
  void testExecuteFailsWhenExternalFileIsMissing() {
    ActionMql action = new ActionMql("test-mql");
    action.setConnection("test-connection");
    action.setUseExternalFile(true);
    action.setCommandFile("");
    action.setMetadataProvider(metadataProvider);
    Result result = new Result();

    action.execute(result, 0);
    assertEquals(1, result.getNrErrors());
    assertFalse(result.isResult());
  }

  @Test
  void testExecuteWithSingleCommand() throws Exception {

    ActionMql action = new ActionMql("test-single_command");
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    IHopMetadataSerializer<MongoDbConnection> serializer = mock(IHopMetadataSerializer.class);
    MongoDbConnection mongoConnection = mock(MongoDbConnection.class);
    MongoClientWrapper mockWrapper = mock(MongoClientWrapper.class);

    action.setConnection("test-connection");
    action.setMql("{ ping: 1 }");
    action.setMetadataProvider(metadataProvider);
    action.setVariables(new java.util.HashMap<>());

    when(metadataProvider.getSerializer(MongoDbConnection.class)).thenReturn(serializer);
    when(serializer.load("test-connection")).thenReturn(mongoConnection);
    when(mongoConnection.getDbName()).thenReturn("testdb");

    when(mongoConnection.createWrapper(any(), any())).thenReturn(mockWrapper);

    doAnswer(
            invocation -> {
              String dbName = invocation.getArgument(0);
              org.apache.hop.mongo.wrapper.MongoDBAction<?> mongoAction = invocation.getArgument(1);

              com.mongodb.client.MongoDatabase mockDb =
                  mock(com.mongodb.client.MongoDatabase.class);
              Document fakeResponse = new Document("ok", 1.0);

              when(mockDb.runCommand(any(Document.class))).thenReturn(fakeResponse);

              return mongoAction.perform(mockDb);
            })
        .when(mockWrapper)
        .perform(anyString(), any());

    Result result = new Result();
    Result finalResult = action.execute(result, 0);

    assertTrue(finalResult.isResult());
    assertEquals(0, finalResult.getNrErrors());

    verify(mockWrapper).dispose();
  }

  @Test
  void testExecuteWithMultipleCommands() throws Exception {
    ActionMql action = new ActionMql("test-multiple-commands");
    action.setMql("[ { \"cmd1\": 1 }, { \"cmd2\": 1 } ]");
    action.setUseExternalFile(false);

    String trimmed = action.getMql().trim();
    if (trimmed.startsWith("[")) {
      List<Document> commands =
          Document.parse("{ \"array\": " + trimmed + " }").getList("array", Document.class);

      assertEquals(2, commands.size());
      assertEquals(1, commands.get(0).get("cmd1"));
      assertEquals(1, commands.get(1).get("cmd2"));
    }
  }

  @Test
  void testParseCommands() throws Exception {
    assertEquals(1, ActionMql.parseCommands(" { ping: 1 } ").size());
    assertEquals(2, ActionMql.parseCommands("[ { \"cmd1\": 1 }, { \"cmd2\": 1 } ]").size());

    // Comma-separated documents without brackets are all run, not only the first
    List<Document> commands =
        ActionMql.parseCommands(
            "{\"insert\": \"c\", \"documents\": [{\"v\": \"a\"}]},"
                + "{\"insert\": \"c\", \"documents\": [{\"v\": \"b\"}]}");
    assertEquals(2, commands.size());
    assertEquals("a", commands.get(0).getList("documents", Document.class).get(0).get("v"));
    assertEquals("b", commands.get(1).getList("documents", Document.class).get(0).get("v"));

    // Text after the first document is run or is an error, never silently ignored
    assertEquals(2, ActionMql.parseCommands("{ ping: 1 } { ping: 1 }").size());
    assertThrows(Exception.class, () -> ActionMql.parseCommands("{ ping: 1 } garbage"));
    assertThrows(Exception.class, () -> ActionMql.parseCommands("[ { ping: 1 } ] garbage"));
    assertThrows(
        HopException.class, () -> ActionMql.parseCommands("[ { ping: 1 } ], \"x\": { ping: 1 }"));

    assertThrows(HopException.class, () -> ActionMql.parseCommands("   "));
    assertThrows(HopException.class, () -> ActionMql.parseCommands("[]"));
  }

  @Test
  void testCheckReply() throws Exception {
    ActionMql.checkReply(new Document("ok", 1.0).append("n", 2), 1, 1);

    Document writeErrors =
        new Document("ok", 1.0)
            .append("n", 1)
            .append(
                "writeErrors",
                List.of(
                    new Document("index", 1)
                        .append("code", 11000)
                        .append("errmsg", "E11000 duplicate key error")));
    MongoDbException e =
        assertThrows(MongoDbException.class, () -> ActionMql.checkReply(writeErrors, 2, 3));
    assertTrue(e.getMessage().contains("E11000"), e.getMessage());
    assertTrue(e.getMessage().contains("2 of 3"), e.getMessage());

    Document writeConcernError =
        new Document("ok", 1.0)
            .append("n", 1)
            .append(
                "writeConcernError",
                new Document("code", 64).append("errmsg", "waiting for replication timed out"));
    e = assertThrows(MongoDbException.class, () -> ActionMql.checkReply(writeConcernError, 1, 1));
    assertTrue(e.getMessage().contains("replication"), e.getMessage());
  }

  @Test
  void testExecuteFailsOnWriteErrors() throws Exception {
    ActionMql action = new ActionMql("test-write-errors");
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    IHopMetadataSerializer<MongoDbConnection> serializer = mock(IHopMetadataSerializer.class);
    MongoDbConnection mongoConnection = mock(MongoDbConnection.class);
    MongoClientWrapper mockWrapper = mock(MongoClientWrapper.class);

    action.setConnection("test-connection");
    action.setMql(
        "[ {\"insert\": \"c\", \"documents\": [{\"_id\": 1}, {\"_id\": 1}]}, {\"drop\": \"c\"} ]");
    action.setMetadataProvider(metadataProvider);

    when(metadataProvider.getSerializer(MongoDbConnection.class)).thenReturn(serializer);
    when(serializer.load("test-connection")).thenReturn(mongoConnection);
    when(mongoConnection.getDbName()).thenReturn("testdb");
    when(mongoConnection.createWrapper(any(), any())).thenReturn(mockWrapper);

    com.mongodb.client.MongoDatabase mockDb = mock(com.mongodb.client.MongoDatabase.class);
    when(mockDb.runCommand(any(Document.class)))
        .thenReturn(
            new Document("ok", 1.0)
                .append("n", 1)
                .append(
                    "writeErrors",
                    List.of(
                        new Document("index", 1)
                            .append("code", 11000)
                            .append("errmsg", "E11000 duplicate key error"))));

    doAnswer(
            invocation -> {
              org.apache.hop.mongo.wrapper.MongoDBAction<?> mongoAction = invocation.getArgument(1);
              return mongoAction.perform(mockDb);
            })
        .when(mockWrapper)
        .perform(anyString(), any());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    // The instruction after the failing one is not run
    verify(mockDb, times(1)).runCommand(any(Document.class));
    verify(mockWrapper).dispose();
  }
}
