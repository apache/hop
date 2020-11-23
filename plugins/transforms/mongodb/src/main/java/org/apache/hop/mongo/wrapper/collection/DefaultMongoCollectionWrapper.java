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

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.cursor.DefaultCursorWrapper;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DefaultMongoCollectionWrapper implements MongoCollectionWrapper {
  private final DBCollection collection;

  public DefaultMongoCollectionWrapper(DBCollection collection) {
    this.collection = collection;
  }

  @Override
  public MongoCursorWrapper find(DBObject dbObject, DBObject dbObject2) throws MongoDbException {
    return wrap(collection.find(dbObject, dbObject2));
  }

  @Override
  public Cursor aggregate(List<? extends DBObject> pipeline, AggregationOptions options) {
    return collection.aggregate(pipeline, options);
  }

  @Override
  public Cursor aggregate(DBObject firstP, DBObject[] remainder) {
    AggregationOptions options = AggregationOptions.builder().build();
    List<DBObject> pipeline = new ArrayList<>();
    pipeline.add(firstP);
    Collections.addAll(pipeline, remainder);
    return aggregate(pipeline, options);
  }

  @Override
  public MongoCursorWrapper find() throws MongoDbException {
    return wrap(collection.find());
  }

  @Override
  public void drop() throws MongoDbException {
    collection.drop();
  }

  @Override
  public WriteResult update(
      DBObject updateQuery, DBObject insertUpdate, boolean upsert, boolean multi)
      throws MongoDbException {
    return collection.update(updateQuery, insertUpdate, upsert, multi);
  }

  @Override
  public WriteResult insert(List<DBObject> m_batch) throws MongoDbException {
    return collection.insert(m_batch);
  }

  @Override
  public MongoCursorWrapper find(DBObject query) throws MongoDbException {
    return wrap(collection.find(query));
  }

  @Override
  public void dropIndex(BasicDBObject mongoIndex) throws MongoDbException {
    collection.dropIndex(mongoIndex);
  }

  @Override
  public void createIndex(BasicDBObject mongoIndex) throws MongoDbException {
    collection.createIndex(mongoIndex);
  }

  @Override
  public void createIndex(BasicDBObject mongoIndex, BasicDBObject options) throws MongoDbException {
    collection.createIndex(mongoIndex, options);
  }

  @Override
  public WriteResult remove() throws MongoDbException {
    return remove(new BasicDBObject());
  }

  @Override
  public WriteResult remove(DBObject query) throws MongoDbException {
    return collection.remove(query);
  }

  @Override
  public WriteResult save(DBObject toTry) throws MongoDbException {
    return collection.save(toTry);
  }

  @Override
  public long count() throws MongoDbException {
    return collection.count();
  }

  @Override
  public List distinct(String key) throws MongoDbException {
    return collection.distinct(key);
  }

  protected MongoCursorWrapper wrap(DBCursor cursor) {
    return new DefaultCursorWrapper(cursor);
  }
}
