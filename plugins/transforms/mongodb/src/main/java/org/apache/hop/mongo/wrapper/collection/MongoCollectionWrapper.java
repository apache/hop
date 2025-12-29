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
package org.apache.hop.mongo.wrapper.collection;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import java.util.List;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * Defines the wrapper interface for all interactions with a MongoCollection via a
 * MongoClientWrapper. All method calls should correspond directly to the call in the underlying
 * MongoCollection, but if appropriate run in the desired AuthContext.
 */
public interface MongoCollectionWrapper {

  MongoCursorWrapper find(Bson query, Bson projection) throws MongoDbException;

  AggregateIterable<Document> aggregate(List<? extends Bson> pipeline);

  AggregateIterable<Document> aggregate(Bson firstP, Bson[] remainder) throws MongoDbException;

  MongoCursorWrapper find() throws MongoDbException;

  void drop() throws MongoDbException;

  UpdateResult update(Bson updateQuery, Bson insertUpdate, boolean upsert, boolean multi)
      throws MongoDbException;

  InsertManyResult insert(List<Document> batch) throws MongoDbException;

  MongoCursorWrapper find(Bson query) throws MongoDbException;

  void dropIndex(Bson mongoIndex) throws MongoDbException;

  void createIndex(Bson mongoIndex) throws MongoDbException;

  void createIndex(Bson mongoIndex, Bson options) throws MongoDbException;

  DeleteResult remove() throws MongoDbException;

  DeleteResult remove(Bson query) throws MongoDbException;

  UpdateResult save(Document toTry) throws MongoDbException;

  long count() throws MongoDbException;

  List distinct(String key) throws MongoDbException;
}
