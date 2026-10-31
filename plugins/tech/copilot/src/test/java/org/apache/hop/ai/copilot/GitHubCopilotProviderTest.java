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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.copilot.rpc.CopilotClientMode;
import com.github.copilot.rpc.PermissionRequestResult;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.ai.provider.AiChatMessage;
import org.apache.hop.ai.provider.AiChatRequest;
import org.junit.jupiter.api.Test;

class GitHubCopilotProviderTest {

  @Test
  void clientUsesCliAuthenticationAndDoesNotPersistAHopCredential() {
    var options =
        GitHubCopilotProvider.clientOptions("C:\\tools\\copilot.exe", "C:\\Users\\me\\.copilot");

    assertEquals(CopilotClientMode.COPILOT_CLI, options.getMode());
    assertTrue(options.getUseLoggedInUser().orElse(false));
    assertEquals("C:\\tools\\copilot.exe", options.getCliPath());
    assertEquals("C:\\Users\\me\\.copilot", options.getCopilotHome());
    assertFalse(new GitHubCopilotProvider().requiresApiKey());
    assertFalse(new GitHubCopilotProvider().supportsStructuredAnswers());
  }

  @Test
  void sessionDisablesToolsAndTelemetry() {
    var config = GitHubCopilotProvider.sessionConfig("gpt-4.1");

    assertEquals("gpt-4.1", config.getModel());
    assertEquals(List.of(), config.getAvailableTools());
    assertFalse(config.getEnableSessionTelemetry().orElse(true));
    assertFalse(config.getEnableFileChangeTracking().orElse(true));
    assertFalse(config.getEnableFileHooks().orElse(true));
    assertFalse(config.getEnableHostGitOperations().orElse(true));
    assertFalse(config.getEnableSessionStore().orElse(true));
    assertFalse(config.getEnableSkills().orElse(true));
    assertFalse(config.getEnableConfigDiscovery().orElse(true));
    assertTrue(config.getSkipCustomInstructions().orElse(false));
    assertFalse(config.getEnableOnDemandInstructionDiscovery().orElse(true));
    assertFalse(config.isEnableMcpApps());
    assertSame(GitHubCopilotProvider.DENY_ALL, config.getOnPermissionRequest());
  }

  @Test
  void cliIsTheConfiguredPathThenTheEnvironmentThenCopilotOnThePath() {
    assertEquals(
        "/opt/copilot", GitHubCopilotProvider.cliCommand("/opt/copilot", "/usr/bin/copilot"));
    assertEquals("/usr/bin/copilot", GitHubCopilotProvider.cliCommand("", "/usr/bin/copilot"));
    assertEquals("copilot", GitHubCopilotProvider.cliCommand(null, null));
  }

  @Test
  void permissionRequestsAreRejected() throws Exception {
    PermissionRequestResult result =
        GitHubCopilotProvider.DENY_ALL.handle(null, null).get(5, TimeUnit.SECONDS);

    assertEquals(PermissionRequestResult.reject("x").getKind(), result.getKind());
  }

  @Test
  void modelListingUsesTheConfiguredTimeout() {
    assertEquals(17, GitHubCopilotProvider.modelListingTimeout(Duration.ofSeconds(17)));
    assertEquals(60, GitHubCopilotProvider.modelListingTimeout(null));
  }

  @Test
  void requestPromptIncludesConversationAndJsonInstruction() {
    var request =
        new AiChatRequest(
            "gpt-4.1",
            "Follow the system rules.",
            "Return the corrected response.",
            List.of(
                new AiChatMessage(AiChatMessage.Role.USER, "Original question"),
                new AiChatMessage(AiChatMessage.Role.ASSISTANT, "Original answer")),
            null,
            true);

    String prompt = GitHubCopilotProvider.prompt(request);

    assertTrue(prompt.contains("System instructions:\nFollow the system rules."));
    assertTrue(prompt.contains("User: Original question\nAssistant: Original answer"));
    assertTrue(prompt.contains("Current request:\nReturn the corrected response."));
    assertTrue(prompt.endsWith("Return only valid JSON with no surrounding prose or markdown."));
  }
}
