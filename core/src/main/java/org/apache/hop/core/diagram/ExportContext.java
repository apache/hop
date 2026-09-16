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

package org.apache.hop.core.diagram;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Standard implementation of {@link IExportContext}. */
@Getter
@Setter
@Builder
public class ExportContext implements IExportContext {
  private IVariables variables;
  private IHopMetadataProvider metadataProvider;
  private ILogChannel log;
  private ExportEnvironment environment;

  public ExportContext() {
    this.environment = ExportEnvironment.CLI;
  }

  public ExportContext(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      ExportEnvironment environment) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = log;
    this.environment = environment != null ? environment : ExportEnvironment.CLI;
  }
}
