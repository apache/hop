/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hop.pipeline.transforms.insertupdate;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class InsertUpdateValue {

  /** Field value to update after lookup */
  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "InsertUpdateMeta.Injection.UPDATE_LOOKUP",
      injectionKey = "UPDATE_LOOKUP")
  private String updateLookup;

  /** Stream name to update value with */
  @HopMetadataProperty(
      key = "rename",
      injectionKeyDescription = "InsertUpdateMeta.Injection.UPDATE_STREAM",
      injectionKey = "UPDATE_STREAM")
  private String updateStream;

  /** Stream name to update value with */
  @HopMetadataProperty(
      key = "update",
      injectionKeyDescription = "InsertUpdateMeta.Injection.UPDATE_FLAG",
      injectionKey = "UPDATE_FLAG")
  private boolean update;

  public InsertUpdateValue() {}

  public InsertUpdateValue(String updateLookup, String updateStream) {
    this.updateLookup = updateLookup;
    this.updateStream = updateStream;
    this.update = true;
  }

  public InsertUpdateValue(String updateLookup, String updateStream, boolean doUpdate) {
    this.updateLookup = updateLookup;
    this.updateStream = updateStream;
    this.update = doUpdate;
  }

  public String getUpdateLookup() {
    return updateLookup;
  }

  public void setUpdateLookup(String updateLookup) {
    this.updateLookup = updateLookup;
  }

  public String getUpdateStream() {
    return updateStream;
  }

  public void setUpdateStream(String updateStream) {
    this.updateStream = updateStream;
  }

  public boolean isUpdate() {
    return update;
  }

  public void setUpdate(boolean update) {
    this.update = update;
  }
}
