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

package org.apache.hop.imp;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** This class represents the transform plugin type. */
@PluginMainClassType(IHopImport.class)
@PluginAnnotationType(ImportPlugin.class)
public class ImportPluginType extends BasePluginType<ImportPlugin> {

  private static ImportPluginType importPluginType;

  protected ImportPluginType() {
    super(ImportPlugin.class, "IMPORT", "Import");
  }

  public static ImportPluginType getInstance() {
    if (importPluginType == null) {
      importPluginType = new ImportPluginType();
    }
    return importPluginType;
  }

  @Override
  protected String extractID(ImportPlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(ImportPlugin annotation) {
    return annotation.name();
  }

  @Override
  protected String extractDesc(ImportPlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractDocumentationUrl(ImportPlugin annotation) {
    return annotation.documentationUrl();
  }
}
