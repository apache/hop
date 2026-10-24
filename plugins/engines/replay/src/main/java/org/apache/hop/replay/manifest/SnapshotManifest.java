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

package org.apache.hop.replay.manifest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.json.HopJson;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class SnapshotManifest {

  public static final String STATUS_SEALED = "SEALED";
  public static final String STATUS_INTERRUPTED = "INTERRUPTED";

  private String snapshotId;
  private String pipelineName;
  private String transformName;
  private int copyNr;
  private String createdTimestamp;
  private long rowCount;
  private String compression;
  private String dataFile;
  private String checksum;
  private String status;
  private List<SnapshotFieldMeta> fields = new ArrayList<>();

  public static SnapshotManifest fromJson(String json) throws Exception {
    return HopJson.newMapper().readValue(json, SnapshotManifest.class);
  }

  public String toJson() throws Exception {
    return HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }
}
