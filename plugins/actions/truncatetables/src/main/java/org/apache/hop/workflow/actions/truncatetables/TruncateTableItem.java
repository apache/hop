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

package org.apache.hop.workflow.actions.truncatetables;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
@NoArgsConstructor
public class TruncateTableItem {

  @HopMetadataProperty(key = "name")
  private String tableName;

  @HopMetadataProperty(key = "schemaname")
  private String schemaName;

  public TruncateTableItem(String tableName, String schemaName) {
    this.tableName = tableName;
    this.schemaName = schemaName;
  }

  @Override
  public String toString() {
    return "TruncateTableItem{"
        + "name='"
        + tableName
        + '\''
        + ", schemaName='"
        + schemaName
        + '\''
        + '}';
  }
}
