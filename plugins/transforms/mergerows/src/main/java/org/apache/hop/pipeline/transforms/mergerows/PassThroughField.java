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

package org.apache.hop.pipeline.transforms.mergerows;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class PassThroughField {
  @HopMetadataProperty private String sourceField;

  @HopMetadataProperty private String renameTo;

  @HopMetadataProperty private boolean referenceField;

  public PassThroughField() {}

  public PassThroughField(String sourceField, String renameTo, boolean referenceField) {
    this.sourceField = sourceField;
    this.renameTo = renameTo;
    this.referenceField = referenceField;
  }

  public PassThroughField(PassThroughField f) {
    this.sourceField = f.sourceField;
    this.renameTo = f.renameTo;
    this.referenceField = f.referenceField;
  }
}
