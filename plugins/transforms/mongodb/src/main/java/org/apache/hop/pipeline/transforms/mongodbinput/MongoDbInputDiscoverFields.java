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

package org.apache.hop.pipeline.transforms.mongodbinput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.field.MongoField;

import java.util.List;

/** Created by brendan on 11/4/14. */
public interface MongoDbInputDiscoverFields {

  List<MongoField> discoverFields(
    IVariables variables,
    MongoDbConnection connection,
    String collection,
    String query,
    String fields,
    boolean isPipeline,
    int docsToSample,
    MongoDbInputMeta transform )
      throws HopException;

  void discoverFields(
    IVariables variables,
    MongoDbConnection connection,
    String collection,
    String query,
    String fields,
    boolean isPipeline,
    int docsToSample,
    MongoDbInputMeta transform,
    DiscoverFieldsCallback discoverFieldsCallback )
      throws HopException;
}
