/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.mongodbdelete;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class MongoDbDeleteField implements Cloneable {

  @HopMetadataProperty(
      key = "incoming_field_1",
      injectionKey = "INCOMING_FIELD_1",
      injectionGroupKey = "MONGODB_FIELDS")
  public String incomingField1 = "";

  @HopMetadataProperty(
      key = "incoming_field_2",
      injectionKey = "INCOMING_FIELD_2",
      injectionGroupKey = "MONGODB_FIELDS")
  public String incomingField2 = "";

  @HopMetadataProperty(
      key = "doc_path",
      injectionKey = "DOC_PATH",
      injectionGroupKey = "MONGODB_FIELDS")
  public String mongoDocPath = "";

  @HopMetadataProperty(
      key = "comparator",
      injectionKey = "COMPARATOR",
      injectionGroupKey = "MONGODB_FIELDS")
  public String comparator = "";

  protected List<String> pathList;
  protected List<String> tempPathList;

  public MongoDbDeleteField copy() {
    MongoDbDeleteField newF = new MongoDbDeleteField();
    newF.incomingField1 = incomingField1;
    newF.incomingField2 = incomingField2;
    newF.mongoDocPath = mongoDocPath;
    newF.comparator = comparator;

    return newF;
  }

  public void init(IVariables vars) {
    pathList = new ArrayList<>();

    String path = vars.resolve(mongoDocPath);
    pathList.add(path);

    tempPathList = new ArrayList<>(pathList);
  }

  public void reset() {
    if (!Utils.isEmpty(tempPathList)) {
      tempPathList.clear();
    }
    if (tempPathList != null) {
      tempPathList.addAll(pathList);
    }
  }
}
