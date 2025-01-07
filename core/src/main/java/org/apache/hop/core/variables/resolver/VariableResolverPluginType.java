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
 *
 */

package org.apache.hop.core.variables.resolver;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** This class represents the value meta plugin type. */
@PluginMainClassType(IVariableResolver.class)
@PluginAnnotationType(VariableResolverPlugin.class)
public class VariableResolverPluginType extends BasePluginType<VariableResolverPlugin> {

  private static VariableResolverPluginType pluginType;

  private VariableResolverPluginType() {
    super(VariableResolverPlugin.class, "VARIABLERESOLVERPLUGIN", "VariableResolver");
  }

  public static VariableResolverPluginType getInstance() {
    if (pluginType == null) {
      pluginType = new VariableResolverPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractDesc(VariableResolverPlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractID(VariableResolverPlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(VariableResolverPlugin annotation) {
    return annotation.name();
  }

  @Override
  protected boolean extractSeparateClassLoader(VariableResolverPlugin annotation) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractDocumentationUrl(VariableResolverPlugin annotation) {
    return annotation.documentationUrl();
  }

  @Override
  protected String extractCasesUrl(VariableResolverPlugin annotation) {
    return annotation.casesUrl();
  }

  @Override
  protected String extractForumUrl(VariableResolverPlugin annotation) {
    return annotation.forumUrl();
  }

  @Override
  protected String extractClassLoaderGroup(VariableResolverPlugin annotation) {
    return annotation.classLoaderGroup();
  }
}
