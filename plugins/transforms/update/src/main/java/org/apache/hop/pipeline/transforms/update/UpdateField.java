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

package org.apache.hop.pipeline.transforms.update;

import org.apache.hop.core.injection.Injection;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class UpdateField {

    /** Field value to update after lookup */
    @HopMetadataProperty(key = "name",
            injectionKeyDescription = "UpdateMeta.Injection.UpdateLookup",
            injectionKey = "UPDATE_LOOKUP")
    private String updateLookup;

    /** Stream name to update value with */
    @HopMetadataProperty(key = "rename",
            injectionKeyDescription = "UpdateMeta.Injection.UpdateStream",
            injectionKey = "UPDATE_STREAM")
    private String updateStream;

    public UpdateField() {
    }

    public UpdateField(String updateLookup, String updateStream) {
        this.updateLookup = updateLookup;
        this.updateStream = updateStream;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateField that = (UpdateField) o;
        return updateLookup.equals(that.updateLookup) && updateStream.equals(that.updateStream);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateLookup, updateStream);
    }
}
