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

import com.mongodb.DBObject;
import com.mongodb.MongoCredential;
import com.mongodb.ReplicaSetStatus;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;

import java.util.List;
import java.util.Set;

/**
 * Defines the wrapper interface for all interactions with a MongoClient. This interface for the
 * most part passes on method calls to the underlying MongoClient implementations, but run in the
 * desired AuthContext. This interface also includes some convenience methods (e.g. getAllTags(),
 * getLastErrorModes()) which are not present in MongoClient.
 */
public interface MongoClientWrapper {
  public Set<String> getCollectionsNames(String dB) throws MongoDbException;

  public List<String> getIndexInfo(String dbName, String collection) throws MongoDbException;

  /**
   * Retrieve all database names found in MongoDB as visible by the authenticated user.
   *
   * @throws MongoDbException
   */
  public List<String> getDatabaseNames() throws MongoDbException;

  /**
   * Get a list of all tagName : tagValue pairs that occur in the tag sets defined across the
   * replica set.
   *
   * @return a list of tags that occur in the replica set configuration
   * @throws MongoDbException if a problem occurs
   */
  public List<String> getAllTags() throws MongoDbException;

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of tag set. It is
   * assumed that members satisfy according to an OR relationship = i.e. a member satisfies if it
   * satisfies at least one of the tag sets in the supplied list.
   *
   * @param tagSets the list of tag sets to match against
   * @return a list of replica set members who's tags satisfy the supplied list of tag sets
   * @throws MongoDbException if a problem occurs
   */
  public List<String> getReplicaSetMembersThatSatisfyTagSets(List<DBObject> tagSets)
      throws MongoDbException;

  /**
   * Return a list of custom "lastErrorModes" (if any) defined in the replica set configuration
   * object on the server. These can be used as the "w" setting for the write concern in addition to
   * the standard "w" values of <number> or "majority".
   *
   * @return a list of the names of any custom "lastErrorModes"
   * @throws MongoDbException if a problem occurs
   */
  public List<String> getLastErrorModes() throws MongoDbException;

  /** Gets the list of credentials that this client authenticates all connections with. */
  public List<MongoCredential> getCredentialList();

  /**
   * Creates a new collection using the specified db and name
   *
   * @param db The database name
   * @param name The new collection name
   * @return a MongoCollectionWrapper which wraps the DBCollection object.
   * @throws MongoDbException
   */
  public MongoCollectionWrapper createCollection(String db, String name) throws MongoDbException;

  /**
   * Gets a collection with a given name. If the collection does not exist, a new collection is
   * created.
   *
   * @param db database name
   * @param name collection name
   * @return a MongoCollectionWrapper which wraps the DBCollection object
   * @throws MongoDbException
   */
  public MongoCollectionWrapper getCollection(String db, String name) throws MongoDbException;

  /**
   * Calls the close() method on the underling MongoClient.
   *
   * @throws MongoDbException
   */
  public void dispose() throws MongoDbException;

  /**
   * Performs an action with the given database
   *
   * @param db the database name
   * @param action the action to perform
   * @return the result of the action
   * @throws MongoDbException
   */
  public <ReturnType> ReturnType perform(String db, MongoDBAction<ReturnType> action)
      throws MongoDbException;

  /** @return the ReplicaSetStatus for the cluster. */
  ReplicaSetStatus getReplicaSetStatus();
}
