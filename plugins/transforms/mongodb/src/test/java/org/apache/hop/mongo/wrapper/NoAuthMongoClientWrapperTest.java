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

package org.apache.hop.mongo.wrapper;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertTrue;

public class NoAuthMongoClientWrapperTest {

  public static String REP_SET_CONFIG =
      "{\"_id\" : \"foo\", \"version\" : 1, "
          + "\"members\" : ["
          + "{"
          + "\"_id\" : 0, "
          + "\"host\" : \"palladium.lan:27017\", "
          + "\"tags\" : {"
          + "\"dc.one\" : \"primary\", "
          + "\"use\" : \"production\""
          + "}"
          + "}, "
          + "{"
          + "\"_id\" : 1, "
          + "\"host\" : \"palladium.local:27018\", "
          + "\"tags\" : {"
          + "\"dc.two\" : \"slave1\""
          + "}"
          + "}, "
          + "{"
          + "\"_id\" : 2, "
          + "\"host\" : \"palladium.local:27019\", "
          + "\"tags\" : {"
          + "\"dc.three\" : \"slave2\", "
          + "\"use\" : \"production\""
          + "}"
          + "}"
          + "],"
          + "\"settings\" : {"
          + "\"getLastErrorModes\" : { "
          + "\"DCThree\" : {"
          + "\"dc.three\" : 1"
          + "}"
          + "}"
          + "}"
          + "}";

  private static final String TAG_SET = "{\"use\" : \"production\"}";
  private static final String TAG_SET2 = "{\"use\" : \"ops\"}";
  private static final String TAG_SET_LIST =
      "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" },"
          + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"d\" },"
          + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"mem\": \"r\"}";

  @Mock private MongoClient mockMongoClient;
  @Mock private MongoUtilLogger mockMongoUtilLogger;
  @Mock private MongoProperties mongoProperties;
  @Mock private DefaultMongoClientFactory mongoClientFactory;
  @Mock private MongoClientOptions mongoClientOptions;
  @Mock private DB mockDB;
  @Mock DBCollection collection;
  @Mock private RuntimeException runtimeException;
  @Captor private ArgumentCaptor<List<ServerAddress>> serverAddresses;
  @Captor private ArgumentCaptor<List<MongoCredential>> mongoCredentials;

  private NoAuthMongoClientWrapper noAuthMongoClientWrapper;

  private static Class<?> PKG = NoAuthMongoClientWrapper.class; // For Translator

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    Mockito.when(
            mongoClientFactory.getMongoClient(
                Mockito.anyList(),
                Mockito.anyList(),
                Mockito.any(MongoClientOptions.class),
                Mockito.anyBoolean()))
        .thenReturn(mockMongoClient);
    NoAuthMongoClientWrapper.clientFactory = mongoClientFactory;
    noAuthMongoClientWrapper =
        new NoAuthMongoClientWrapper(mockMongoClient, mongoProperties, mockMongoUtilLogger);
  }

  @Test
  public void testPerform() throws Exception {
    MongoDBAction mockMongoDBAction = Mockito.mock(MongoDBAction.class);
    noAuthMongoClientWrapper.perform("Test", mockMongoDBAction);
    Mockito.verify(mockMongoDBAction, Mockito.times(1))
        .perform(noAuthMongoClientWrapper.getDb("Test"));
  }

  @Test
  public void testGetLastErrorMode() throws MongoDbException {
    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);
    DBCollection dbCollection = Mockito.mock(DBCollection.class);
    Mockito.when(dbCollection.findOne()).thenReturn(config);
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(dbCollection);

    Assert.assertThat(
        noAuthMongoClientWrapper.getLastErrorModes(), IsEqual.equalTo(Arrays.asList("DCThree")));
  }

  @Test
  public void testGetAllReplicaSetMemberRecords() {
    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);
    Object members = config.get(NoAuthMongoClientWrapper.REPL_SET_MEMBERS);

    assertTrue(members != null);
    assertTrue(members instanceof BasicDBList);
    Assert.assertEquals(3, ((BasicDBList) members).size());
  }

  @Test
  public void testSetupAllTags() {
    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);
    Object members = config.get(NoAuthMongoClientWrapper.REPL_SET_MEMBERS);

    List<String> allTags = noAuthMongoClientWrapper.setupAllTags((BasicDBList) members);

    Assert.assertEquals(4, allTags.size());
  }

  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();

    List<DBObject> tagSets = new ArrayList<>(); // tags to satisfy

    DBObject tSet = (DBObject) JSON.parse(TAG_SET);
    tagSets.add(tSet);

    List<String> satisfy = noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
    // two replica set members have the "use : production" tag in their tag sets
    Assert.assertEquals(2, satisfy.size());
    Assert.assertThat(satisfy.get(0), containsString("palladium.lan:27017"));
    Assert.assertThat(satisfy.get(1), containsString("palladium.local:27019"));
  }

  @Test
  public void testGetReplicaSetMembersBadInput() throws MongoDbException {
    setupMockedReplSet();
    try {
      noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(null);
      Assert.fail("expected exception");
    } catch (Exception e) {
      Assert.assertThat(e, CoreMatchers.instanceOf(MongoDbException.class));
    }
  }

  @Test
  public void testGetReplicaSetMembersDoesntSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<>(); // tags to satisfy
    DBObject tSet = (DBObject) JSON.parse(TAG_SET2);
    tagSets.add(tSet);
    List<String> satisfy = noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
    // no replica set members have the "use : ops" tag in their tag sets
    Assert.assertEquals(0, satisfy.size());
  }

  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSetsThrowsOnDbError() throws MongoDbException {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<>(); // tags to satisfy
    DBObject tSet = (DBObject) JSON.parse(TAG_SET);
    tagSets.add(tSet);
    Mockito.doThrow(runtimeException)
        .when(mockMongoClient)
        .getDB(NoAuthMongoClientWrapper.LOCAL_DB);
    try {
      noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
      Assert.fail("expected exception.");
    } catch (Exception e) {
      Assert.assertThat(e, CoreMatchers.instanceOf(MongoDbException.class));
    }
  }

  @Test
  public void testGetClientStandalone() throws MongoDbException, UnknownHostException {
    ServerAddress address = new ServerAddress("fakehost", 1010);
    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn(address.getHost());
    Mockito.when(mongoProperties.get(MongoProp.PORT)).thenReturn("1010");
    Mockito.when(mongoProperties.useAllReplicaSetMembers()).thenReturn(false);

    noAuthMongoClientWrapper.getClient(mongoClientOptions);

    Mockito.verify(mongoClientFactory)
        .getMongoClient(
            serverAddresses.capture(),
            mongoCredentials.capture(),
            Mockito.any(MongoClientOptions.class),
            Mockito.eq(false));

    Assert.assertThat(serverAddresses.getValue(), IsEqual.equalTo(Arrays.asList(address)));
    Assert.assertThat(
        "No credentials should be associated w/ NoAuth",
        mongoCredentials.getValue().size(),
        IsEqual.equalTo(0));
  }

  @Test
  public void testGetClientRepSet() throws MongoDbException, UnknownHostException {
    Mockito.when(mongoProperties.get(MongoProp.HOST))
        .thenReturn("host1:1010,host2:2020,host3:3030");
    Mockito.when(mongoProperties.useAllReplicaSetMembers()).thenReturn(true);
    noAuthMongoClientWrapper.getClient(mongoClientOptions);

    Mockito.verify(mongoClientFactory)
        .getMongoClient(
            serverAddresses.capture(),
            mongoCredentials.capture(),
            Mockito.eq(mongoClientOptions),
            Mockito.eq(true));

    List<ServerAddress> addresses =
        Arrays.asList(
            new ServerAddress("host1", 1010),
            new ServerAddress("host2", 2020),
            new ServerAddress("host3", 3030));
    Assert.assertThat(serverAddresses.getValue(), IsEqual.equalTo(addresses));
  }

  @Test
  public void testBadHostsAndPorts() throws MongoDbException, UnknownHostException {
    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("host1:1010:2020");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      Assert.fail("expected exception:  malformed host ");
    } catch (MongoDbException e) {
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      Assert.fail("expected exception:  empty host string ");
    } catch (MongoDbException e) {
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("   ");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      Assert.fail("expected exception:  empty host string ");
    } catch (MongoDbException e) {
    }

    Mockito.when(mongoProperties.get(MongoProp.HOST)).thenReturn("host1:badport");
    try {
      noAuthMongoClientWrapper.getClient(mongoClientOptions);
      Assert.fail("expected exception:  bad port ");
    } catch (MongoDbException e) {
    }
  }

  @Test
  public void testConstructorInitializesClientWithProps() throws MongoDbException {
    final AtomicBoolean clientCalled = new AtomicBoolean(false);
    final MongoClientOptions options = Mockito.mock(MongoClientOptions.class);
    Mockito.when(mongoProperties.buildMongoClientOptions(mockMongoUtilLogger)).thenReturn(options);
    new NoAuthMongoClientWrapper(mongoProperties, mockMongoUtilLogger) {
      protected MongoClient getClient(MongoClientOptions opts) {
        clientCalled.set(true);
        Assert.assertThat(opts, IsEqual.equalTo(options));
        return null;
      }
    };
    Mockito.verify(mongoProperties).buildMongoClientOptions(mockMongoUtilLogger);
    assertTrue(clientCalled.get());
  }

  @Test
  public void operationsDelegateToMongoClient() throws MongoDbException {
    noAuthMongoClientWrapper.getDatabaseNames();
    Mockito.verify(mockMongoClient).getDatabaseNames();

    noAuthMongoClientWrapper.getDb("foo");
    Mockito.verify(mockMongoClient).getDB("foo");

    Mockito.when(mockMongoClient.getDB("foo")).thenReturn(mockDB);
    noAuthMongoClientWrapper.getCollectionsNames("foo");
    Mockito.verify(mockDB).getCollectionNames();
  }

  @Test
  public void mongoExceptionsPropogate() {
    Mockito.doThrow(runtimeException).when(mockMongoClient).getDatabaseNames();
    try {
      noAuthMongoClientWrapper.getDatabaseNames();
      Assert.fail("expected exception");
    } catch (Exception mde) {
      Assert.assertThat(mde, CoreMatchers.instanceOf(MongoDbException.class));
    }
    Mockito.doThrow(runtimeException).when(mockMongoClient).getDB("foo");
    try {
      noAuthMongoClientWrapper.getDb("foo");
      Assert.fail("expected exception");
    } catch (Exception mde) {
      Assert.assertThat(mde, CoreMatchers.instanceOf(MongoDbException.class));
    }
  }

  @Test
  public void mongoGetCollNamesExceptionPropgates() {
    Mockito.when(mockMongoClient.getDB("foo")).thenReturn(mockDB);
    Mockito.doThrow(runtimeException).when(mockDB).getCollectionNames();
    try {
      noAuthMongoClientWrapper.getCollectionsNames("foo");
      Assert.fail("expected exception");
    } catch (Exception mde) {
      Assert.assertThat(mde, CoreMatchers.instanceOf(MongoDbException.class));
    }
  }

  @Test
  public void testGetIndex() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(true);
    DBCollection collection = Mockito.mock(DBCollection.class);
    DBObject indexInfoObj = Mockito.mock(DBObject.class);
    Mockito.when(indexInfoObj.toString()).thenReturn("indexInfo");
    List<DBObject> indexInfo = Arrays.asList(indexInfoObj);
    Mockito.when(collection.getIndexInfo()).thenReturn(indexInfo);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);

    Assert.assertThat(
        noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection"),
        IsEqual.equalTo(Arrays.asList("indexInfo")));
  }

  @Test
  public void testGetIndexCollectionDoesntExist() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(false);
    DBCollection collection = Mockito.mock(DBCollection.class);
    DBObject indexInfoObj = Mockito.mock(DBObject.class);
    Mockito.when(indexInfoObj.toString()).thenReturn("indexInfo");
    List<DBObject> indexInfo = Arrays.asList(indexInfoObj);
    Mockito.when(collection.getIndexInfo()).thenReturn(indexInfo);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);
    Assert.assertThat(
        noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection"),
        IsEqual.equalTo(Arrays.asList("indexInfo")));
    Mockito.verify(mockDB).createCollection("collection", null);
  }

  @Test
  public void testGetIndexCollectionNotSpecified() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "");
      Assert.fail("expected exception");
    } catch (Exception e) {
      Assert.assertThat(e, CoreMatchers.instanceOf(MongoDbException.class));
    }
  }

  @Test
  public void getIndexInfoErrorConditions() {
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection");
      Assert.fail("expected exception since DB is null");
    } catch (Exception e) {
    }
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "");
      Assert.fail("expected exception since no collection specified.");
    } catch (Exception e) {
    }
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(true);
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection");
      Assert.fail("expected exception since null collection");
    } catch (Exception e) {
      Mockito.verify(mockDB).getCollection("collection");
      Assert.assertThat(
          e.getMessage(),
          containsString(
              BaseMessages.getString(
                  PKG,
                  "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection",
                  "collection")));
    }
  }

  @Test
  public void testGetIndexNoIndexThrows() {
    initFakeDb();
    try {
      noAuthMongoClientWrapper.getIndexInfo("fakeDb", "collection");
      Assert.fail("expected exception since no index info");
    } catch (Exception e) {
      Mockito.verify(mockDB).getCollection("collection");
      Assert.assertThat(
          e.getMessage(),
          containsString(
              BaseMessages.getString(
                  PKG,
                  "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection",
                  "collection")));
    }
  }

  private void initFakeDb() {
    Mockito.when(mockMongoClient.getDB("fakeDb")).thenReturn(mockDB);
    Mockito.when(mockDB.collectionExists("collection")).thenReturn(true);
    Mockito.when(mockDB.getCollection("collection")).thenReturn(collection);
  }

  @Test
  public void testGetAllTagsNoDB() throws MongoDbException {
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.Message.Warning.LocalDBNotAvailable"));
    Assert.assertThat(tags.size(), IsEqual.equalTo(0));
  }

  @Test
  public void testGetAllTagsNoRepSet() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.ReplicaSetCollectionUnavailable"));
    Assert.assertThat(tags.size(), IsEqual.equalTo(0));
  }

  @Test
  public void testGetAllTagsRepSetEmtpy() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    DBObject membersList = new BasicDBList();
    DBObject basicDBObject =
        new BasicDBObject(NoAuthMongoClientWrapper.REPL_SET_MEMBERS, membersList);
    Mockito.when(collection.findOne()).thenReturn(basicDBObject);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    Assert.assertThat(tags.size(), IsEqual.equalTo(0));
  }

  @Test
  public void testGetAllTagsRepSetNull() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    Mockito.when(collection.findOne()).thenReturn(null);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    Assert.assertThat(tags.size(), IsEqual.equalTo(0));
  }

  @Test
  public void testGetAllTagsRepSetUnexpectedType() throws MongoDbException {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    DBObject dbObj = Mockito.mock(DBObject.class);
    Mockito.when(collection.findOne()).thenReturn(dbObj);
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Mockito.verify(mockMongoUtilLogger)
        .info(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined"));
    Assert.assertThat(tags.size(), IsEqual.equalTo(0));
  }

  @Test
  public void testGetAllTags() throws MongoDbException {
    setupMockedReplSet();
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Collections.sort(tags, String.CASE_INSENSITIVE_ORDER);
    Assert.assertThat(
        tags,
        IsEqual.equalTo(
            Arrays.asList(
                "\"dc.one\" : \"primary\"",
                "\"dc.three\" : \"slave2\"",
                "\"dc.two\" : \"slave1\"",
                "\"use\" : \"production\"")));
  }

  @Test
  public void testGetCreateCollection() throws MongoDbException {
    initFakeDb();
    noAuthMongoClientWrapper.getCollection("fakeDb", "collection");
    Mockito.verify(mockDB).getCollection("collection");
    noAuthMongoClientWrapper.createCollection("fakeDb", "newCollection");
    Mockito.verify(mockDB).createCollection("newCollection", null);
  }

  @Test
  public void testClientDelegation() {
    noAuthMongoClientWrapper.dispose();
    Mockito.verify(mockMongoClient).close();
    noAuthMongoClientWrapper.getReplicaSetStatus();
    Mockito.verify(mockMongoClient).getReplicaSetStatus();
  }

  private void setupMockedReplSet() {
    Mockito.when(mockMongoClient.getDB(NoAuthMongoClientWrapper.LOCAL_DB)).thenReturn(mockDB);
    Mockito.when(mockDB.getCollection(NoAuthMongoClientWrapper.REPL_SET_COLLECTION))
        .thenReturn(collection);
    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);
    Object members = config.get(NoAuthMongoClientWrapper.REPL_SET_MEMBERS);
    DBObject basicDBObject = new BasicDBObject(NoAuthMongoClientWrapper.REPL_SET_MEMBERS, members);
    Mockito.when(collection.findOne()).thenReturn(basicDBObject);
  }
}
