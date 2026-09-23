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

package org.apache.hop.metadata.serializer.json.renamed;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

/** A metadata type which was renamed from key "RenamedType" to "renamed-type". */
@Getter
@Setter
@HopMetadata(
    name = "Renamed type",
    key = "renamed-type",
    legacyKeys = {"RenamedType"})
public class RenamedType extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private String description;

  public RenamedType() {}

  public RenamedType(String name, String description) {
    this.name = name;
    this.description = description;
  }
}
