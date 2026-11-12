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

import com.mongodb.client.MongoDatabase;
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
  void testExecuteWithMultipleCommands() {
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

    // A semicolon at the end worked before and still does
    assertEquals(1, ActionMql.parseCommands("{ ping: 1 };").size());
    assertEquals(1, ActionMql.parseCommands("{ ping: 1 } ;\n ;\n").size());
    assertEquals(2, ActionMql.parseCommands("[ { ping: 1 }, { ping: 2 } ];").size());
    assertEquals(
        "x;", ActionMql.parseCommands("{ \"insert\": \"x;\" };").get(0).getString("insert"));

    assertThrows(HopException.class, () -> ActionMql.parseCommands(null));
    assertThrows(HopException.class, () -> ActionMql.parseCommands("   "));
    assertThrows(HopException.class, () -> ActionMql.parseCommands(" ; "));
    assertThrows(HopException.class, () -> ActionMql.parseCommands("[]"));
  }

  @Test
  void testParseCommandsRejectsTextAfterTheCommand() {
    // Text after the first document was ignored before: it is an error now, it is never run
    assertTextAfter("{\"drop\":\"tmp\"}, {\"drop\":\"prod\"}");
    assertTextAfter("{ ping: 1 } { ping: 1 }");
    assertTextAfter("{ ping: 1 } garbage");
    assertTextAfter("[ { ping: 1 } ] garbage");
    assertTextAfter("{ ping: 1 }; { ping: 2 }");
    assertTextAfter("{ ping: 1 } // note");
    assertTextAfter("{ ping: 1 }}");
    // A repeated key can't replace the first array
    assertTextAfter("[ {\"drop\":\"a\"} ], \"array\": [ {\"ping\":1} ]");
    assertTextAfter("{a:1}, [{b:1}]");
  }

  private static void assertTextAfter(String mql) {
    HopException e = assertThrows(HopException.class, () -> ActionMql.parseCommands(mql), mql);
    assertTrue(e.getMessage().contains("Text after the JSON instruction"), e.getMessage());
  }

  @Test
  void testParseCommandsRejectsValuesThatAreNotDocuments() {
    HopException e = assertThrows(HopException.class, () -> ActionMql.parseCommands("[1, 2]"));
    assertTrue(e.getMessage().contains("Instruction 1 of the JSON array"), e.getMessage());
    e = assertThrows(HopException.class, () -> ActionMql.parseCommands("[ {a:1}, [ {b:1} ] ]"));
    assertTrue(e.getMessage().contains("Instruction 2 of the JSON array"), e.getMessage());
    e = assertThrows(HopException.class, () -> ActionMql.parseCommands("\"ping\""));
    assertTrue(e.getMessage().contains("not a document or an array"), e.getMessage());
  }

  @Test
  void testParseCommandsErrorPositionMatchesTheText() {
    // The position is the one in the text as typed, not in a wrapper around it
    String mql = "  { \"a\": 1, \"b\": # }";
    HopException e = assertThrows(HopException.class, () -> ActionMql.parseCommands(mql));
    // (the parser counts from 1)
    assertTrue(e.getMessage().contains("Position: " + (mql.indexOf('#') + 1)), e.getMessage());
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

    // bulkWrite (MongoDB 8) counts its errors in nErrors, the errors are in its cursor
    Document bulkWrite =
        new Document("ok", 1.0)
            .append("nErrors", 1)
            .append("nInserted", 1)
            .append(
                "cursor",
                new Document("id", 0L)
                    .append(
                        "firstBatch",
                        List.of(
                            new Document("ok", 1.0).append("idx", 0).append("n", 1),
                            new Document("ok", 0.0)
                                .append("idx", 1)
                                .append("code", 11000)
                                .append("errmsg", "E11000 duplicate key error"))));
    e = assertThrows(MongoDbException.class, () -> ActionMql.checkReply(bulkWrite, 1, 1));
    assertTrue(e.getMessage().contains("E11000"), e.getMessage());
    ActionMql.checkReply(new Document("ok", 1.0).append("nErrors", 0), 1, 1);
  }

  @Test
  void testExecuteFailsOnWriteErrors() throws Exception {
    MongoDatabase mockDb = mockDbWithWriteErrors();
    ActionMql action = newWriteErrorsAction(mockDb, true);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    // The instruction after the failing one is not run
    verify(mockDb, times(1)).runCommand(any(Document.class));
  }

  @Test
  void testExecuteIgnoresWriteErrorsByDefault() throws Exception {
    // An unordered insert that skips duplicate keys stays true, as it was before the option
    assertFalse(new ActionMql().isFailOnWriteErrors());
    MongoDatabase mockDb = mockDbWithWriteErrors();
    ActionMql action = newWriteErrorsAction(mockDb, false);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    verify(mockDb, times(2)).runCommand(any(Document.class));
  }

  private static MongoDatabase mockDbWithWriteErrors() {
    MongoDatabase mockDb = mock(MongoDatabase.class);
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
    return mockDb;
  }

  private static ActionMql newWriteErrorsAction(MongoDatabase mockDb, boolean failOnWriteErrors)
      throws Exception {
    ActionMql action = new ActionMql("test-write-errors");
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    IHopMetadataSerializer<MongoDbConnection> serializer = mock(IHopMetadataSerializer.class);
    MongoDbConnection mongoConnection = mock(MongoDbConnection.class);
    MongoClientWrapper mockWrapper = mock(MongoClientWrapper.class);

    action.setConnection("test-connection");
    action.setMql(
        "[ {\"insert\": \"c\", \"documents\": [{\"_id\": 1}, {\"_id\": 1}], \"ordered\": false},"
            + " {\"drop\": \"c\"} ]");
    action.setFailOnWriteErrors(failOnWriteErrors);
    action.setMetadataProvider(metadataProvider);

    when(metadataProvider.getSerializer(MongoDbConnection.class)).thenReturn(serializer);
    when(serializer.load("test-connection")).thenReturn(mongoConnection);
    when(mongoConnection.getDbName()).thenReturn("testdb");
    when(mongoConnection.createWrapper(any(), any())).thenReturn(mockWrapper);
    doAnswer(
            invocation -> {
              org.apache.hop.mongo.wrapper.MongoDBAction<?> mongoAction = invocation.getArgument(1);
              return mongoAction.perform(mockDb);
            })
        .when(mockWrapper)
        .perform(anyString(), any());
    return action;
  }
}
