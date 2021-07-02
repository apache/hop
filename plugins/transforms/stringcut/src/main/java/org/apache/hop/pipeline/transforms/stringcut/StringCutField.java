/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.hop.pipeline.transforms.stringcut;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class StringCutField {

    @HopMetadataProperty(key = "in_stream_name", injectionKey="FIELD_IN_STREAM", injectionKeyDescription = "StringCutDialog.Injection.InStream.Field")
    private String fieldInStream;

    @HopMetadataProperty(key = "out_stream_name", injectionKey="FIELD_OUT_STREAM", injectionKeyDescription = "StringCutDialog.Injection.OutStream.Field")
    private String fieldOutStream;

    @HopMetadataProperty(key = "cut_from", injectionKey="CUT_FROM", injectionKeyDescription = "StringCutDialog.Injection.CutFrom.Field")
    private String cutFrom;

    @HopMetadataProperty(key = "cut_to", injectionKey="CUT_TO", injectionKeyDescription = "StringCutDialog.Injection.CutTo.Field")
    private String cutTo;

    public StringCutField() {
    }

    public StringCutField(String fieldInStream, String fieldOutStream, String cutFrom, String cutTo) {
        this.fieldInStream = fieldInStream;
        this.fieldOutStream = fieldOutStream;
        this.cutFrom = cutFrom;
        this.cutTo = cutTo;
    }

    public StringCutField(StringCutField f) {
        this.fieldInStream = f.fieldInStream;
        this.fieldOutStream = f.fieldOutStream;
        this.cutFrom = f.cutFrom;
        this.cutTo = f.cutTo;
    }

    public String getFieldInStream() {
        return fieldInStream;
    }

    public void setFieldInStream(String fieldInStream) {
        this.fieldInStream = fieldInStream;
    }

    public String getFieldOutStream() {
        return fieldOutStream;
    }

    public void setFieldOutStream(String fieldOutStream) {
        this.fieldOutStream = fieldOutStream;
    }

    public String getCutFrom() {
        return cutFrom;
    }

    public void setCutFrom(String cutFrom) {
        this.cutFrom = cutFrom;
    }

    public String getCutTo() {
        return cutTo;
    }

    public void setCutTo(String cutTo) {
        this.cutTo = cutTo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringCutField that = (StringCutField) o;
        return Objects.equals(fieldInStream, that.fieldInStream) && Objects.equals(cutFrom, that.cutFrom) && Objects.equals(cutTo, that.cutTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldInStream, fieldOutStream, cutFrom, cutTo);
    }
}
