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

package org.apache.hop.lineage.datahub;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.Hop;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.lineage.spi.LineageSinkHealth;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;

/**
 * {@code hop datahub-publish}: publishes a project's pipelines and workflows to DataHub without
 * running them. See {@link DataHubPublisher}.
 */
@Getter
@Setter
@CommandLine.Command(
    mixinStandardHelpOptions = true,
    name = "datahub-publish",
    description = "Publish a project's pipelines and workflows to DataHub without running them")
@HopCommand(
    id = "datahub-publish",
    description = "Publish a project's pipelines and workflows to DataHub without running them")
public class DataHubPublishCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {

  @CommandLine.Option(
      names = {"-s", "--sink"},
      required = true,
      description = "The lineage sink of type DataHub to publish to")
  private String sinkName;

  @CommandLine.Option(
      names = {"-f", "--folder"},
      description =
          "The folder with the pipelines and workflows to publish, with its subfolders. The"
              + " default is the project's home folder.")
  private String folder;

  @CommandLine.Option(
      names = {"-d", "--include-documentation"},
      description =
          "Also publish what hop doc writes for each pipeline and workflow as a DataHub"
              + " document. Needs DataHub 1.7 or later.")
  private boolean includingDocumentation;

  @CommandLine.Option(
      names = {"-ip", "--include-parameters"},
      description =
          "With -d: list the parameters of each pipeline and workflow in its document, as hop"
              + " doc -ip does")
  private boolean includingParameters;

  @CommandLine.Option(
      names = {"-in", "--include-notes"},
      description =
          "With -d: add the text of the notes of each pipeline and workflow to its document, as"
              + " hop doc -in does")
  private boolean includingNotes;

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;
  private ILogChannel log;

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("DataHubPublish");
    // The project and environment options (-j, -e), as hop doc has them
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_DOC);
  }

  @Override
  public void run() {
    try {
      for (Map.Entry<String, Object> mixin : cmd.getMixins().entrySet()) {
        if (mixin.getValue() instanceof IConfigOptions options) {
          options.handleOption(log, this, variables);
        }
      }
      String source = folder != null ? folder : "${PROJECT_HOME}";
      DataHubPublisher publisher = new DataHubPublisher(variables, metadataProvider, log);
      publisher.setIncludingDocumentation(includingDocumentation);
      publisher.setIncludingParameters(includingParameters);
      publisher.setIncludingNotes(includingNotes);
      DataHubPublisher.Result result = publisher.publish(sinkName, source);
      log.logBasic(describe(result));
      if (!result.isSuccess()) {
        System.exit(1);
      }
    } catch (Exception e) {
      log.logError("Unable to publish to DataHub", e);
      System.exit(1);
    }
  }

  /** One line on what was published, and what went wrong. */
  static String describe(DataHubPublisher.Result result) {
    LineageSinkHealth health = result.health();
    StringBuilder line =
        new StringBuilder("Published ")
            .append(result.pipelines())
            .append(" pipeline(s) and ")
            .append(result.workflows())
            .append(" workflow(s) to DataHub");
    if (health != null) {
      line.append(": ")
          .append(health.sent())
          .append(" event(s) accepted, ")
          .append(health.rejected())
          .append(" refused, ")
          .append(health.waiting())
          .append(" not delivered");
      if (health.lastError() != null) {
        line.append(". Last error: ").append(health.lastError());
      }
    }
    if (!result.unreadable().isEmpty()) {
      line.append(". Not published: ").append(String.join(", ", result.unreadable()));
    }
    if (result.documentationNotPublished() != null) {
      line.append(". ").append(result.documentationNotPublished());
    }
    return line.toString();
  }
}
