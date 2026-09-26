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

package org.apache.hop.schema;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/** Results of the XML schema export process. */
@Getter
@Setter
public class HopXmlSchemaExportResult {

  private final List<String> generatedFiles = new ArrayList<>();
  private final List<String> warnings = new ArrayList<>();
  private final List<String> errors = new ArrayList<>();

  private boolean pipelineSchemaGenerated;
  private boolean workflowSchemaGenerated;
  private int transformSchemasCount;
  private int actionSchemasCount;

  public HopXmlSchemaExportResult() {}

  public int getTotalGeneratedFilesCount() {
    return (pipelineSchemaGenerated ? 1 : 0)
        + (workflowSchemaGenerated ? 1 : 0)
        + transformSchemasCount
        + actionSchemasCount;
  }
}
