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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.apache.hop.mongo.Util;
import org.apache.hop.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.bson.Document;

/**
 * Unified implementation of MongoClientWrapper that handles both authenticated and
 * non-authenticated connections. Should only be instantiated by MongoClientWrapperFactory.
 */
class MongoClientWrapperImpl implements MongoClientWrapper {
  private static final Class<?> PKG = MongoClientWrapperImpl.class;
  public static final int MONGO_DEFAULT_PORT = 27017;

  public static final String LOCAL_DB = "local";
  public static final String REPL_SET_COLLECTION = "system.replset";
  public static final String REPL_SET_SETTINGS = "settings";
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes";
  public static final String REPL_SET_MEMBERS = "members";
  public static final String
      CONST_MONGO_NO_AUTH_WRAPPER_MESSAGE_WARNING_NO_REPLICA_SET_MEMBERS_DEFINED =
          "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined";

  static MongoClientFactory clientFactory = new DefaultMongoClientFactory();

  private final MongoClient mongo;
  private final MongoUtilLogger log;

  protected MongoProperties props;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the transform meta data
   *
   * @param props properties to use
   * @param log for logging
   * @throws MongoDbException if a problem occurs
   */
  MongoClientWrapperImpl(MongoProperties props, MongoUtilLogger log) throws MongoDbException {
    this.log = log;
    this.props = props;
    mongo = getClient(props.buildMongoClientSettings(log));
  }

  MongoClientWrapperImpl(MongoClient mongo, MongoProperties props, MongoUtilLogger log) {
    this.mongo = mongo;
    this.log = log;
    this.props = props;
  }

  MongoClient getMongo() {
    return mongo;
  }

  @Override
  public void test() throws MongoDbException {
    String databaseName = props.get(MongoProp.DBNAME);
    try {
      mongo.getDatabase(databaseName).runCommand(new Document("ping", 1));
    } catch (Exception e) {
      throw new MongoDbException("Error pinging database " + databaseName, e);
    }
  }

  private List<ServerAddress> getServerAddressList() throws MongoDbException {
    String hostsPorts = props.get(MongoProp.HOST);
    String singlePort = props.get(MongoProp.PORT);

    int singlePortI = -1;

    try {
      singlePortI = Integer.parseInt(singlePort);
    } catch (NumberFormatException n) {
      // don't complain
    }

    if (Util.isEmpty(hostsPorts)) {
      throw new MongoDbException(
          BaseMessages.getString(PKG, "MongoNoAuthWrapper.Message.Error.EmptyHostsString"));
    }

    List<ServerAddress> serverList = new ArrayList<>();

    String[] parts = hostsPorts.trim().split(",");
    for (String part : parts) {
      // host:port?
      int port = singlePortI != -1 ? singlePortI : MONGO_DEFAULT_PORT;
      String[] hp = part.split(":");
      if (hp.length > 2) {
        throw new MongoDbException(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.Message.Error.MalformedHost", part));
      }

      String host = hp[0];
      if (hp.length == 2) {
        // non-default port
        try {
          port = Integer.parseInt(hp[1].trim());
        } catch (NumberFormatException n) {
          throw new MongoDbException(
              BaseMessages.getString(
                  PKG, "MongoNoAuthWrapper.Message.Error.UnableToParsePortNumber", hp[1]));
        }
      }

      try {
        ServerAddress s = new ServerAddress(host, port);
        serverList.add(s);
      } catch (Throwable u) {
        throw new MongoDbException(u);
      }
    }
    return serverList;
  }

  protected MongoClient getClient(MongoClientSettings.Builder settingsBuilder)
      throws MongoDbException {
    // Check if a connection string is provided - if so, use it directly
    String connectionString = props.get(MongoProp.CONNECTION_STRING);
    if (!Util.isEmpty(connectionString)) {
      // Get credentials separately (best practice - avoids URL encoding issues)
      // The connection string should be clean (without credentials)
      List<MongoCredential> credList = getCredentialList();
      return getClientFactory(props).getMongoClient(connectionString, settingsBuilder, credList);
    }

    // Otherwise, fall back to building from hostname/port
    List<MongoCredential> credList = getCredentialList();
    List<ServerAddress> serverAddressList = getServerAddressList();

    if (serverAddressList.isEmpty()) {
      // There should be minimally one server defined.  Default is localhost, so
      // this can only happen if it's been explicitly set to NULL.
      throw new MongoDbException(
          BaseMessages.getString(
              MongoClientWrapper.class, "MongoNoAuthWrapper.Message.Error.NoHostSet"));
    }
    return getClientFactory(props)
        .getMongoClient(
            serverAddressList, credList, settingsBuilder, props.useAllReplicaSetMembers());
  }

  /**
   * Retrieve all database names found in MongoDB as visible by the authenticated user.
   *
   * @throws MongoDbException
   */
  @Override
  public List<String> getDatabaseNames() throws MongoDbException {
    try {
      List<String> names = new ArrayList<>();
      getMongo().listDatabaseNames().into(names);
      return names;
    } catch (Exception e) {
      throw new MongoDbException(e);
    }
  }

  protected MongoDatabase getDb(String dbName) throws MongoDbException {
    try {
      return getMongo().getDatabase(dbName);
    } catch (Exception e) {
      throw new MongoDbException(e);
    }
  }

  /**
   * Get the set of collections for a MongoDB database.
   *
   * @param dB Name of database
   * @return Set of collections in the database requested.
   * @throws MongoDbException If an error occurs.
   */
  @Override
  public Set<String> getCollectionsNames(String dB) throws MongoDbException {
    try {
      Set<String> names = new HashSet<>();
      getDb(dB).listCollectionNames().into(names);
      return names;
    } catch (Exception e) {
      if (e instanceof MongoDbException mongoDbException) {
        throw mongoDbException;
      } else {
        throw new MongoDbException(e);
      }
    }
  }

  /**
   * Return a list of custom "lastErrorModes" (if any) defined in the replica set configuration
   * object on the server. These can be used as the "w" setting for the write concern in addition to
   * the standard "w" values of <number> or "majority".
   *
   * @return a list of the names of any custom "lastErrorModes"
   * @throws MongoDbException if a problem occurs
   */
  @Override
  public List<String> getLastErrorModes() throws MongoDbException {
    List<String> customLastErrorModes = new ArrayList<>();

    MongoDatabase local = getDb(LOCAL_DB);
    if (local != null) {
      try {
        MongoCollection<Document> replset = local.getCollection(REPL_SET_COLLECTION);
        if (replset != null) {
          Document config = replset.find().first();
          extractLastErrorModes(config, customLastErrorModes);
        }
      } catch (Exception e) {
        throw new MongoDbException(e);
      }
    }

    return customLastErrorModes;
  }

  protected void extractLastErrorModes(Document config, List<String> customLastErrorModes) {
    if (config != null) {
      Object settings = config.get(REPL_SET_SETTINGS);

      if (settings != null) {
        Object getLastErrModes = ((Document) settings).get(REPL_SET_LAST_ERROR_MODES);

        if (getLastErrModes != null) {
          for (String m : ((Document) getLastErrModes).keySet()) {
            customLastErrorModes.add(m);
          }
        }
      }
    }
  }

  @Override
  public List<String> getIndexInfo(String dbName, String collection) throws MongoDbException {
    try {
      MongoDatabase db = getDb(dbName);

      if (db == null) {
        throw new MongoDbException(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.ErrorMessage.NonExistentDB", dbName));
      }

      if (Util.isEmpty(collection)) {
        throw new MongoDbException(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified"));
      }

      // Check if collection exists, create if not
      boolean exists = false;
      for (String name : db.listCollectionNames()) {
        if (name.equals(collection)) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        db.createCollection(collection);
      }

      MongoCollection<Document> coll = db.getCollection(collection);
      if (coll == null) {
        throw new MongoDbException(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection", collection));
      }

      List<String> result = new ArrayList<>();
      try (MongoCursor<Document> cursor = coll.listIndexes().iterator()) {
        while (cursor.hasNext()) {
          Document index = cursor.next();
          result.add(index.toJson());
        }
      }

      if (Utils.isEmpty(result)) {
        throw new MongoDbException(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection", collection));
      }

      return result;
    } catch (Exception e) {
      log.error(
          BaseMessages.getString(PKG, "MongoNoAuthWrapper.ErrorMessage.GeneralError.Message")
              + ":\n\n"
              + e.getMessage(),
          e);
      if (e instanceof MongoDbException mongoDbException) {
        throw mongoDbException;
      } else {
        throw new MongoDbException(e);
      }
    }
  }

  @Override
  public List<String> getAllTags() throws MongoDbException {
    return setupAllTags(getRepSetMemberRecords());
  }

  private List<Document> getRepSetMemberRecords() throws MongoDbException {
    List<Document> setMembers = null;
    try {
      MongoDatabase local = getDb(LOCAL_DB);
      if (local != null) {

        MongoCollection<Document> replset = local.getCollection(REPL_SET_COLLECTION);
        if (replset != null) {
          Document config = replset.find().first();

          if (config != null) {
            Object members = config.get(REPL_SET_MEMBERS);

            if (members instanceof List) {
              @SuppressWarnings("unchecked")
              List<Document> membersList = (List<Document>) members;
              if (membersList.isEmpty()) {
                // log that there are no replica set members defined
                logInfo(
                    BaseMessages.getString(
                        PKG,
                        CONST_MONGO_NO_AUTH_WRAPPER_MESSAGE_WARNING_NO_REPLICA_SET_MEMBERS_DEFINED));
              } else {
                setMembers = membersList;
              }

            } else {
              // log that there are no replica set members defined
              logInfo(
                  BaseMessages.getString(
                      PKG,
                      CONST_MONGO_NO_AUTH_WRAPPER_MESSAGE_WARNING_NO_REPLICA_SET_MEMBERS_DEFINED));
            }
          } else {
            // log that there are no replica set members defined
            logInfo(
                BaseMessages.getString(
                    PKG,
                    CONST_MONGO_NO_AUTH_WRAPPER_MESSAGE_WARNING_NO_REPLICA_SET_MEMBERS_DEFINED));
          }
        } else {
          // log that the replica set collection is not available
          logInfo(
              BaseMessages.getString(
                  PKG, "MongoNoAuthWrapper.Message.Warning.ReplicaSetCollectionUnavailable"));
        }
      } else {
        // log that the local database is not available!!
        logInfo(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.Message.Warning.LocalDBNotAvailable"));
      }
    } catch (Exception ex) {
      throw new MongoDbException(ex);
    } finally {
      if (getMongo() != null) {
        getMongo().close();
      }
    }

    return setMembers;
  }

  private void logInfo(String message) {
    if (log != null) {
      log.info(message);
    }
  }

  protected List<String> setupAllTags(List<Document> members) {
    HashSet<String> tempTags = new HashSet<>();

    if (!Utils.isEmpty(members)) {
      for (Document member : members) {
        if (member != null) {
          Document tags = (Document) member.get("tags");
          if (tags == null) {
            continue;
          }

          for (String tagName : tags.keySet()) {
            String tagVal = tags.get(tagName).toString();
            String combined = quote(tagName) + " : " + quote(tagVal);
            tempTags.add(combined);
          }
        }
      }
    }

    return new ArrayList<>(tempTags);
  }

  protected static String quote(String string) {
    if (string.indexOf('"') >= 0) {
      string = string.replace("\"", "\\\"");
    }

    string = ("\"" + string + "\"");

    return string;
  }

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of tag set. It is
   * assumed that members satisfy according to an OR relationship = i.e. a member satisfies if it
   * satisfies at least one of the tag sets in the supplied list.
   *
   * @param tagSets the list of tag sets to match against
   * @return a list of replica set members who's tags satisfy the supplied list of tag sets
   * @throws MongoDbException if a problem occurs
   */
  @Override
  public List<String> getReplicaSetMembersThatSatisfyTagSets(List<Document> tagSets)
      throws MongoDbException {
    try {
      List<String> result = new ArrayList<>();
      for (Document object :
          checkForReplicaSetMembersThatSatisfyTagSets(tagSets, getRepSetMemberRecords())) {
        result.add(object.toJson());
      }
      return result;
    } catch (Exception ex) {
      if (ex instanceof MongoDbException mongoDbException) {
        throw mongoDbException;
      } else {
        throw new MongoDbException(
            BaseMessages.getString(
                PKG, "MongoNoAuthWrapper.ErrorMessage.UnableToGetReplicaSetMembers"),
            ex);
      }
    }
  }

  protected List<Document> checkForReplicaSetMembersThatSatisfyTagSets(
      List<Document> tagSets, List<Document> members) {
    List<Document> satisfy = new ArrayList<>();
    if (!Utils.isEmpty(members)) {
      for (Document m : members) {
        if (m != null) {
          Document tags = (Document) m.get("tags");
          if (tags == null) {
            continue;
          }

          for (Document toMatch : tagSets) {
            boolean match = true;

            for (String tagName : toMatch.keySet()) {
              String tagValue = toMatch.get(tagName).toString();

              // does replica set member m's tags contain this tag?
              Object matchVal = tags.get(tagName);

              if (matchVal == null) {
                match = false; // doesn't match this particular tag set
                // no need to check any other keys in toMatch
                break;
              }

              if (!matchVal.toString().equals(tagValue)) {
                // rep set member m's tags has this tag, but it's value does not
                // match
                match = false;

                // no need to check any other keys in toMatch
                break;
              }
            }

            if (match && !satisfy.contains(m)) {
              // all tag/values present and match - add this member (only if its
              // not already there)
              satisfy.add(m);
            }
          }
        }
      }
    }

    return satisfy;
  }

  @Override
  public MongoCollectionWrapper getCollection(String db, String name) throws MongoDbException {
    return wrap(getDb(db).getCollection(name));
  }

  @Override
  public MongoCollectionWrapper createCollection(String db, String name) throws MongoDbException {
    getDb(db).createCollection(name);
    return wrap(getDb(db).getCollection(name));
  }

  @Override
  public List<MongoCredential> getCredentialList() {
    List<MongoCredential> credList = new ArrayList<>();
    String username = props.get(MongoProp.USERNAME);
    String password = props.get(MongoProp.PASSWORD);

    // If no username or password, return empty list (no auth)
    if (username == null || username.trim().isEmpty() || password == null) {
      return credList;
    }

    String authDatabase = props.get(MongoProp.AUTH_DATABASE);
    String dbName = props.get(MongoProp.DBNAME);
    String authMecha = props.get(MongoProp.AUTH_MECHA);
    // if not value on AUTH_MECHA set default authentication mechanism
    if (authMecha == null) {
      authMecha = "";
    }

    // Use the AuthDatabase if one was supplied, otherwise use the connecting database
    authDatabase = (authDatabase == null || authDatabase.trim().isEmpty()) ? dbName : authDatabase;

    // If authDatabase is still null or empty, default to "admin"
    if (authDatabase == null || authDatabase.trim().isEmpty()) {
      authDatabase = "admin";
    }

    // Convert password to char array (handle empty string)
    char[] passwordChars =
        (password != null && !password.isEmpty()) ? password.toCharArray() : new char[0];

    if (authMecha.equalsIgnoreCase("SCRAM-SHA-1") || authMecha.equalsIgnoreCase("SCRAM_SHA_1")) {
      credList.add(
          MongoCredential.createScramSha1Credential(username, authDatabase, passwordChars));
    } else if (authMecha.equalsIgnoreCase("SCRAM-SHA-256")
        || authMecha.equalsIgnoreCase("SCRAM_SHA_256")) {
      credList.add(
          MongoCredential.createScramSha256Credential(username, authDatabase, passwordChars));
    } else if (authMecha.equalsIgnoreCase("PLAIN")) {
      credList.add(MongoCredential.createPlainCredential(username, authDatabase, passwordChars));
    } else {
      // Default to SCRAM-SHA-256 (the modern default)
      credList.add(MongoCredential.createCredential(username, authDatabase, passwordChars));
    }
    return credList;
  }

  protected MongoCollectionWrapper wrap(MongoCollection<Document> collection) {
    return new DefaultMongoCollectionWrapper(collection);
  }

  @Override
  public void dispose() {
    if (getMongo() != null) {
      getMongo().close();
    }
  }

  @Override
  public boolean isReplicaSet() {
    // Check cluster description
    try {
      return getMongo()
          .getClusterDescription()
          .getType()
          .equals(com.mongodb.connection.ClusterType.REPLICA_SET);
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public <ReturnType> ReturnType perform(String db, MongoDBAction<ReturnType> action)
      throws MongoDbException {
    return action.perform(getDb(db));
  }

  public MongoClientFactory getClientFactory(MongoProperties opts) {
    return clientFactory;
  }
}
