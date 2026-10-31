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

package org.apache.hop.ai.copilot;

import com.github.copilot.CopilotClient;
import com.github.copilot.generated.AssistantMessageEvent;
import com.github.copilot.rpc.CopilotClientMode;
import com.github.copilot.rpc.CopilotClientOptions;
import com.github.copilot.rpc.MessageOptions;
import com.github.copilot.rpc.ModelInfo;
import com.github.copilot.rpc.PermissionHandler;
import com.github.copilot.rpc.PermissionRequestResult;
import com.github.copilot.rpc.SessionConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.AiAuthKind;
import org.apache.hop.ai.provider.AiChatMessage;
import org.apache.hop.ai.provider.AiChatRequest;
import org.apache.hop.ai.provider.AiChatResponse;
import org.apache.hop.ai.provider.AiProviderPlugin;
import org.apache.hop.ai.provider.BaseAiProvider;
import org.apache.hop.ai.provider.IAiChatProvider;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** GitHub Copilot CLI-backed chat provider for Hop's AI Assistant. */
@Getter
@Setter
@GuiPlugin(classLoaderGroup = "hop-ai-copilot")
@AiProviderPlugin(
    id = "github-copilot",
    name = "GitHub Copilot",
    description = "GitHub Copilot CLI",
    documentationUrl = "/metadata-types/ai-provider.html",
    classLoaderGroup = "hop-ai-copilot")
public class GitHubCopilotProvider extends BaseAiProvider implements IAiChatProvider {

  private static final Class<?> PKG = GitHubCopilotProvider.class;
  private static final long DEFAULT_TIMEOUT_SECONDS = 60;
  private static final String COPILOT_CLI_PATH = "COPILOT_CLI_PATH";
  static final String DEFAULT_CLI_COMMAND = "copilot";

  /**
   * The session gets no tools, so nothing should ask for permission. Should the CLI ask anyway, the
   * answer is no: an answer from the assistant may not run commands or touch files.
   */
  static final PermissionHandler DENY_ALL =
      (request, invocation) ->
          CompletableFuture.completedFuture(
              PermissionRequestResult.reject("Hop does not allow GitHub Copilot to use tools."));

  @HopMetadataProperty
  @GuiWidgetElement(
      id = "0100-copilot-cli-path",
      order = "0100",
      type = GuiElementType.TEXT,
      parentId = IAiProvider.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::GitHubCopilotProvider.CliPath.Label",
      toolTip = "i18n::GitHubCopilotProvider.CliPath.Tooltip")
  private String cliPath = "";

  public GitHubCopilotProvider() {
    setAuthKind(AiAuthKind.OAUTH);
    setRequiresApiKey(false);
    setHopModelType("COPILOT");
  }

  @Override
  public boolean supportsBaseUrl() {
    return false;
  }

  @Override
  public boolean supportsTemperature() {
    return false;
  }

  @Override
  public boolean supportsContextSize() {
    return false;
  }

  @Override
  public boolean supportsMaxOutputTokens() {
    return false;
  }

  @Override
  public boolean supportsThinking() {
    return false;
  }

  @Override
  public boolean supportsStructuredAnswers() {
    return false;
  }

  @Override
  public AiChatResponse chat(AiChatRequest request, IVariables variables) throws HopException {
    long started = System.nanoTime();
    long timeoutSeconds = timeoutSeconds(request);
    try (CopilotClient client = newClient(variables)) {
      await(client.start(), started, timeoutSeconds, "starting the Copilot CLI");
      SessionConfig config = sessionConfig(request.modelName());
      var session =
          await(
              client.createSession(config), started, timeoutSeconds, "creating a Copilot session");
      try (session) {
        AssistantMessageEvent response =
            await(
                session.sendAndWait(new MessageOptions().setPrompt(prompt(request))),
                started,
                timeoutSeconds,
                "waiting for a Copilot response");
        return new AiChatResponse(
            Utils.isEmpty(response.getData().content()) ? "" : response.getData().content(),
            null,
            null,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException(BaseMessages.getString(PKG, "GitHubCopilotProvider.Interrupted"), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      throw new HopException(
          BaseMessages.getString(PKG, "GitHubCopilotProvider.Failed", e.getMessage()), cause);
    } catch (TimeoutException e) {
      throw timeoutError(timeoutSeconds, e);
    }
  }

  @Override
  public List<String> listModelNames(IVariables variables, Duration timeout) throws HopException {
    long timeoutSeconds = modelListingTimeout(timeout);
    long started = System.nanoTime();
    try (CopilotClient client = newClient(variables)) {
      await(client.start(), started, timeoutSeconds, "starting the Copilot CLI");
      List<String> names = new ArrayList<>();
      for (ModelInfo model :
          await(client.listModels(), started, timeoutSeconds, "listing Copilot models")) {
        if (model != null && !Utils.isEmpty(model.getId())) {
          names.add(model.getId());
        }
      }
      names.sort(String.CASE_INSENSITIVE_ORDER);
      return names.stream().distinct().toList();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException(BaseMessages.getString(PKG, "GitHubCopilotProvider.Interrupted"), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      throw new HopException(
          BaseMessages.getString(PKG, "GitHubCopilotProvider.Failed", e.getMessage()), cause);
    } catch (TimeoutException e) {
      throw timeoutError(timeoutSeconds, e);
    }
  }

  private CopilotClient newClient(IVariables variables) {
    String copilotHome = System.getenv("COPILOT_HOME");
    if (Utils.isEmpty(copilotHome)) {
      copilotHome = Path.of(System.getProperty("user.home"), ".copilot").toString();
    }
    String resolvedCliPath = variables == null ? cliPath : variables.resolve(cliPath);
    CopilotClientOptions options =
        clientOptions(cliCommand(resolvedCliPath, System.getenv(COPILOT_CLI_PATH)), copilotHome);
    return new CopilotClient(options);
  }

  /**
   * The CLI to start: the path in the provider, then {@code COPILOT_CLI_PATH}, then {@code copilot}
   * on the PATH. Without a path the SDK looks for a runtime bundled in a jar that Hop does not
   * ship.
   */
  static String cliCommand(String configuredPath, String environmentPath) {
    if (!Utils.isEmpty(configuredPath)) {
      return configuredPath;
    }
    if (!Utils.isEmpty(environmentPath)) {
      return environmentPath;
    }
    return DEFAULT_CLI_COMMAND;
  }

  static CopilotClientOptions clientOptions(String resolvedCliPath, String copilotHome) {
    CopilotClientOptions options =
        new CopilotClientOptions()
            .setMode(CopilotClientMode.COPILOT_CLI)
            .setUseLoggedInUser(true)
            .setCopilotHome(copilotHome);
    if (!Utils.isEmpty(resolvedCliPath)) {
      options.setCliPath(resolvedCliPath);
    }
    return options;
  }

  static SessionConfig sessionConfig(String modelName) {
    SessionConfig config =
        new SessionConfig()
            .setAvailableTools(List.of())
            .setEnableSessionTelemetry(false)
            .setEnableFileChangeTracking(false)
            .setEnableFileHooks(false)
            .setEnableHostGitOperations(false)
            .setEnableSessionStore(false)
            .setEnableSkills(false)
            .setEnableConfigDiscovery(false)
            .setSkipCustomInstructions(true)
            .setEnableOnDemandInstructionDiscovery(false)
            .setEnableMcpApps(false)
            .setOnPermissionRequest(DENY_ALL);
    if (!Utils.isEmpty(modelName)) {
      config.setModel(modelName);
    }
    return config;
  }

  private static long timeoutSeconds(AiChatRequest request) {
    return request.timeout() == null
        ? DEFAULT_TIMEOUT_SECONDS
        : Math.max(1, request.timeout().getSeconds());
  }

  static long modelListingTimeout(Duration timeout) {
    return timeout == null ? DEFAULT_TIMEOUT_SECONDS : Math.max(1, timeout.getSeconds());
  }

  static String prompt(AiChatRequest request) {
    StringBuilder prompt = new StringBuilder();
    if (!Utils.isEmpty(request.systemPrompt())) {
      prompt.append("System instructions:\n").append(request.systemPrompt()).append("\n\n");
    }
    if (!request.conversationHistory().isEmpty()) {
      prompt.append("Conversation so far:\n");
      for (AiChatMessage message : request.conversationHistory()) {
        prompt
            .append(
                switch (message.role()) {
                  case SYSTEM -> "System";
                  case USER -> "User";
                  case ASSISTANT -> "Assistant";
                })
            .append(": ")
            .append(message.content())
            .append('\n');
      }
      prompt.append('\n');
    }
    prompt.append("Current request:\n").append(request.userPrompt());
    if (request.jsonOnly()) {
      prompt.append("\n\nReturn only valid JSON with no surrounding prose or markdown.");
    }
    return prompt.toString();
  }

  private static <T> T await(
      CompletableFuture<T> future, long started, long timeoutSeconds, String operation)
      throws InterruptedException, ExecutionException, TimeoutException {
    long remainingNanos = TimeUnit.SECONDS.toNanos(timeoutSeconds) - (System.nanoTime() - started);
    if (remainingNanos <= 0) {
      throw new TimeoutException(operation);
    }
    try {
      return future.get(remainingNanos, TimeUnit.NANOSECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      throw new ExecutionException(operation + ": " + cause.getMessage(), cause);
    } catch (TimeoutException e) {
      throw new TimeoutException(operation);
    }
  }

  private static HopException timeoutError(long timeoutSeconds, TimeoutException cause) {
    return new HopException(
        BaseMessages.getString(
            PKG,
            "GitHubCopilotProvider.Timeout",
            cause.getMessage(),
            Long.toString(timeoutSeconds)),
        cause);
  }
}
