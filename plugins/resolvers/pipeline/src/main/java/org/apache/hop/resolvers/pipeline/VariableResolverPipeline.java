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

package org.apache.hop.resolvers.pipeline;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.resolver.IVariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolverPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.ui.hopgui.file.pipeline.extension.TypePipelineFile;
import org.json.simple.JSONObject;

@Getter
@Setter
@GuiPlugin
@VariableResolverPlugin(
    id = "Variable-Resolver-Pipeline",
    name = "Pipeline Variable Resolver",
    description = "Use a pipeline to resolve the value of a variable expression",
    documentationUrl = "/metadata-types/variable-resolvers/pipeline-variable-resolver.html")
public class VariableResolverPipeline implements IVariableResolver {

  /** The name of the pipeline filename to use to resolve variable expressions */
  @GuiWidgetElement(
      id = "filename",
      order = "01",
      label = "i18n::VariableResolverEditor.label.Filename",
      typeFilename = TypePipelineFile.class,
      type = GuiElementType.FILENAME,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty()
  private String filename;

  /** The name of the local Hop pipeline run configuration to use */
  @GuiWidgetElement(
      id = "runConfigurationName",
      order = "02",
      label = "i18n::VariableResolverEditor.label.RunConfigurationName",
      type = GuiElementType.METADATA,
      metadata = PipelineRunConfiguration.class,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String runConfigurationName;

  /** The name of the variable that will contain the expression in the pipeline. */
  @GuiWidgetElement(
      id = "expressionVariableName",
      order = "03",
      label = "i18n::VariableResolverEditor.label.ExpressionVariableName",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String expressionVariableName;

  /** The name of the variable that will contain the expression in the pipeline. */
  @GuiWidgetElement(
      id = "outputTransformName",
      order = "03",
      label = "i18n::VariableResolverEditor.label.OutputTransformName",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String outputTransformName;

  @Override
  public void init() {
    // This space was intentionally left blank.
  }

  @Override
  public void setPluginId() {}

  @Override
  public String getPluginId() {
    return "Variable-Resolver-Pipeline";
  }

  @Override
  public void setPluginName(String pluginName) {}

  @Override
  public String getPluginName() {
    return "Pipeline Variable Resolver";
  }

  @Override
  public String resolve(String expression, IVariables variables) throws HopException {
    MultiMetadataProvider metadataProvider = HopMetadataInstance.getMetadataProvider();
    String pipelineFilename = variables.resolve(filename);
    if (StringUtils.isEmpty(pipelineFilename)) {
      throw new HopException(
          "Please provide a pipeline filename to use to resolve the variable expression.");
    }
    String pipelineRunConfigurationName = variables.resolve(runConfigurationName);
    PipelineRunConfiguration defaultRunConfiguration =
        PipelineRunConfiguration.findDefault(metadataProvider);
    if (defaultRunConfiguration == null && StringUtils.isEmpty(pipelineRunConfigurationName)) {
      throw new HopException(
          "Please specify a local pipeline run configuration to use to resolve the variable expression.");
    }

    String variableName = variables.resolve(expressionVariableName);
    if (StringUtils.isEmpty(variableName)) {
      throw new HopException(
          "Please specify a variable name to use to contain the variable expression.");
    }

    PipelineExecutionConfiguration configuration = new PipelineExecutionConfiguration();
    configuration.setLogLevel(LogLevel.BASIC);
    configuration.setRunConfiguration(pipelineRunConfigurationName);

    PipelineMeta pipelineMeta = new PipelineMeta(pipelineFilename, metadataProvider, variables);
    IPipelineEngine<PipelineMeta> pipeline =
        PipelineEngineFactory.createPipelineEngine(
            variables, pipelineRunConfigurationName, metadataProvider, pipelineMeta);
    pipeline.getPipelineMeta().setInternalHopVariables(pipeline);
    pipeline.initializeFrom(null);
    pipeline.setVariable(variableName, expression);
    pipeline.setLogLevel(configuration.getLogLevel());
    pipeline.setMetadataProvider(metadataProvider);

    if (!(pipeline instanceof LocalPipelineEngine)) {
      LogChannel.GENERAL.logError(
          "The pipeline run configuration needs to be of type 'Native Local' to resolve variable expression '"
              + expression
              + "'");
      return null;
    }

    // Run the pipeline
    //
    pipeline.prepareExecution();

    // Collect output rows...
    //
    JSONObject resultJs = new JSONObject();
    ITransform transform =
        ((LocalPipelineEngine) pipeline).getRunThread(variables.resolve(outputTransformName), 0);
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowReadEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            // We're only interested in the first row.
            //
            if (resultJs.isEmpty()) {
              for (int i = 0; i < rowMeta.size(); i++) {
                IValueMeta valueMeta = rowMeta.getValueMeta(i);
                String name = valueMeta.getName();
                try {
                  String value = rowMeta.getString(row, i);
                  resultJs.put(name, value);
                } catch (Exception e) {
                  LogChannel.GENERAL.logError(
                      "Error getting string field from value '" + name + "'", e);
                }
              }
            }
          }
        });

    pipeline.startThreads();
    pipeline.waitUntilFinished();

    // After completion, get the JSON of the row back.
    //
    return resultJs.toJSONString();
  }
}
