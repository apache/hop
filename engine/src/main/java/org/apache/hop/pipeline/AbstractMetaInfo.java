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

package org.apache.hop.pipeline;

import java.util.Date;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public abstract class AbstractMetaInfo {
  @HopMetadataProperty protected String name;

  @HopMetadataProperty(key = "name_sync_with_filename")
  protected boolean nameSynchronizedWithFilename;

  @HopMetadataProperty(groupKey = "info", key = "description")
  protected String description;

  @HopMetadataProperty(groupKey = "info", key = "extended_description")
  protected String extendedDescription;

  @HopMetadataProperty(key = "created_user")
  protected String createdUser;

  @HopMetadataProperty(key = "modified_user")
  protected String modifiedUser;

  @HopMetadataProperty(key = "created_date")
  protected Date createdDate;

  @HopMetadataProperty(key = "modified_date")
  protected Date modifiedDate;

  /**
   * The version of Hop that created this. Initialized empty rather than to the running version: the
   * XML deserializer only assigns a field when its element is present, so a real version defaulted
   * here would survive both loading a file written before this element existed and every undo,
   * which clears and re-deserializes. That file would then claim to have been created by whichever
   * version happened to open it. An empty value makes no such claim and reads as "unknown".
   */
  @HopMetadataProperty(key = "created_hop_version")
  protected String createdHopVersion;

  /** The version of Hop that last saved this. Initialized empty for the same reason, see above. */
  @HopMetadataProperty(key = "modified_hop_version")
  protected String modifiedHopVersion;

  protected AbstractMetaInfo() {
    this.nameSynchronizedWithFilename = true;
    this.createdDate = new Date();
    this.modifiedDate = new Date();
    this.createdUser = "-";
    this.modifiedUser = "-";
    this.createdHopVersion = "";
    this.modifiedHopVersion = "";
  }
}
