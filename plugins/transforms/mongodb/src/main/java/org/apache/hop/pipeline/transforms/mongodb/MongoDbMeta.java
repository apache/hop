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
package org.apache.hop.pipeline.transforms.mongodb;

import org.apache.hop.core.injection.Injection;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputMeta;

public abstract class MongoDbMeta<Main extends ITransform, Data extends ITransformData>
    extends BaseTransformMeta implements ITransformMeta<Main, Data> {
  protected static Class<?> PKG = MongoDbInputMeta.class; // For Translator

  @Injection(name = "CONNECTION")
  protected String connectionName;

  @Injection(name = "COLLECTION")
  protected String collection;

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /** @param connectionName The connectionName to set */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets collection
   *
   * @return value of collection
   */
  public String getCollection() {
    return collection;
  }

  /** @param collection The collection to set */
  public void setCollection(String collection) {
    this.collection = collection;
  }
}
