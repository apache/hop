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
 *
 */

package org.apache.hop.metadata.inject.sample;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

@Getter
@Setter
public class SampleMeta {
  @HopMetadataProperty(injectionKey = "STRING", injectionGroupKey = "STRINGS")
  private List<String> strings;

  @HopMetadataProperty(injectionKey = "FILE_NAME")
  private String fileName;

  @HopMetadataProperty(injectionKey = "LIMIT")
  private int limit;

  @HopMetadataProperty(storeWithCode = true, injectionKey = "SAMPLE_TYPE")
  private SampleType sampleType;

  @HopMetadataProperty(
      storeWithName = true,
      injectionKey = "CONNECTION_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private DatabaseMeta databaseMeta;

  @HopMetadataProperty private Additional additional;

  @HopMetadataProperty(injectionGroupKey = "FIELDS")
  private List<Field> fields;

  @HopMetadataProperty(injectionGroupKey = "MORE_FIELDS")
  private List<Field> moreFields;

  public SampleMeta() {
    this.strings = new ArrayList<>();
    this.additional = new Additional();
    this.fields = new ArrayList<>();
    this.moreFields = new ArrayList<>();
  }
}
