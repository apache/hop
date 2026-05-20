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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.cursor.DefaultCursorWrapper;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.bson.Document;
import org.bson.conversions.Bson;

public class DefaultMongoCollectionWrapper implements MongoCollectionWrapper {
  private final MongoCollection<Document> collection;

  public DefaultMongoCollectionWrapper(MongoCollection<Document> collection) {
    this.collection = collection;
  }

  @Override
  public MongoCursorWrapper find(Bson query, Bson projection) throws MongoDbException {
    FindIterable<Document> findIterable = collection.find(query);
    if (projection != null) {
      findIterable = findIterable.projection(projection);
    }
    return wrap(findIterable);
  }

  @Override
  public AggregateIterable<Document> aggregate(List<? extends Bson> pipeline) {
    return collection.aggregate(new ArrayList<>(pipeline));
  }

  @Override
  public AggregateIterable<Document> aggregate(Bson firstP, Bson[] remainder)
      throws MongoDbException {
    List<Bson> pipeline = new ArrayList<>();
    pipeline.add(firstP);
    Collections.addAll(pipeline, remainder);
    return aggregate(pipeline);
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
  public UpdateResult update(Bson updateQuery, Bson insertUpdate, boolean upsert, boolean multi)
      throws MongoDbException {
    UpdateOptions options = new UpdateOptions().upsert(upsert);
    if (multi) {
      return collection.updateMany(updateQuery, insertUpdate, options);
    } else {
      return collection.updateOne(updateQuery, insertUpdate, options);
    }
  }

  @Override
  public InsertManyResult insert(List<Document> batch) throws MongoDbException {
    return collection.insertMany(batch);
  }

  @Override
  public MongoCursorWrapper find(Bson query) throws MongoDbException {
    return wrap(collection.find(query));
  }

  @Override
  public void dropIndex(Bson mongoIndex) throws MongoDbException {
    collection.dropIndex(mongoIndex);
  }

  @Override
  public void createIndex(Bson mongoIndex) throws MongoDbException {
    collection.createIndex(mongoIndex);
  }

  @Override
  public void createIndex(Bson mongoIndex, Bson options) throws MongoDbException {
    IndexOptions indexOptions = new IndexOptions();
    if (options instanceof Document doc) {
      if (doc.containsKey("background")) {
        indexOptions.background(doc.getBoolean("background", false));
      }
      if (doc.containsKey("unique")) {
        indexOptions.unique(doc.getBoolean("unique", false));
      }
      if (doc.containsKey("sparse")) {
        indexOptions.sparse(doc.getBoolean("sparse", false));
      }
      if (doc.containsKey("name")) {
        indexOptions.name(doc.getString("name"));
      }
    }
    collection.createIndex(mongoIndex, indexOptions);
  }

  @Override
  public DeleteResult remove() throws MongoDbException {
    return remove(new Document());
  }

  @Override
  public DeleteResult remove(Bson query) throws MongoDbException {
    return collection.deleteMany(query);
  }

  @Override
  public UpdateResult save(Document toTry) throws MongoDbException {
    // In MongoDB 5.x, save() is replaced with replaceOne with upsert
    Object id = toTry.get("_id");
    if (id != null) {
      return collection.replaceOne(Filters.eq("_id", id), toTry, new ReplaceOptions().upsert(true));
    } else {
      // No _id, insert instead
      collection.insertOne(toTry);
      // Return a synthetic UpdateResult - document was inserted (no upsertedId needed)
      return UpdateResult.acknowledged(0, 1L, null);
    }
  }

  @Override
  public long count() throws MongoDbException {
    return collection.countDocuments();
  }

  @Override
  public List distinct(String key) throws MongoDbException {
    List<Object> result = new ArrayList<>();
    collection.distinct(key, Object.class).into(result);
    return result;
  }

  protected MongoCursorWrapper wrap(FindIterable<Document> findIterable) {
    return new DefaultCursorWrapper(findIterable);
  }
}
