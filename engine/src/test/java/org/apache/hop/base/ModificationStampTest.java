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

package org.apache.hop.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Set;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The creation and last modification of a pipeline or workflow are recorded in the file so a
 * project can be reviewed for stale or unattended work.
 *
 * <p>Both are only recorded when there is something to record: the date of the last change, and the
 * user behind it whenever the session knows one. A desktop session has no authenticated user, and
 * neither that fact nor the operating system account says who edited the file, so the user elements
 * keep whatever the file already held rather than being filled with a placeholder.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ModificationStampTest {

  /** The placeholder written by every Hop release so far when no user is known. */
  private static final String NO_USER = "-";

  private static final String AUTHOR = "alice";
  private static final String EDITOR = "bob";

  /** A pipeline as written by an older release: dates filled in, users left at the placeholder. */
  private static final String PIPELINE_WITHOUT_USERS =
      """
      <pipeline>
        <info>
          <name>no-users</name>
          <created_user>-</created_user>
          <created_date>2023/09/16 22:31:19.820</created_date>
          <modified_user>-</modified_user>
          <modified_date>2023/09/16 22:31:19.820</modified_date>
        </info>
      </pipeline>
      """;

  /** A workflow as written by an older release, keeping its info elements at the root. */
  private static final String WORKFLOW_WITHOUT_USERS =
      """
      <workflow>
        <name>no-users</name>
        <created_user>-</created_user>
        <created_date>2023/09/16 22:31:19.820</created_date>
        <modified_user>-</modified_user>
        <modified_date>2023/09/16 22:31:19.820</modified_date>
      </workflow>
      """;

  private final IVariables variables = new Variables();
  private final IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

  @AfterEach
  void restoreSecurityContext() {
    HopSecurity.reset();
  }

  private void authenticateAs(String username) {
    HopSecurity.setProvider(() -> HopSecurityContext.forUser(username, Set.of(HopRole.USER)));
  }

  private PipelineMeta loadPipeline(String xml) throws Exception {
    return new PipelineMeta(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
        metadataProvider,
        variables);
  }

  private WorkflowMeta loadWorkflow(String xml) throws Exception {
    return new WorkflowMeta(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
        metadataProvider,
        variables);
  }

  @Test
  void pipelineModificationDateIsStampedAndSerialized() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_WITHOUT_USERS);
    Date loadedDate = pipelineMeta.getModifiedDate();

    pipelineMeta.stampModified();

    assertTrue(
        pipelineMeta.getModifiedDate().after(loadedDate),
        "the modification date has to move forward when the file is saved after an edit");
    String stamped = XmlHandler.date2string(pipelineMeta.getModifiedDate());
    assertTrue(
        pipelineMeta.getXml(variables).contains("<modified_date>" + stamped + "</modified_date>"),
        "the stamped date has to end up in the file");
  }

  @Test
  void workflowModificationDateIsStamped() throws Exception {
    WorkflowMeta workflowMeta = loadWorkflow(WORKFLOW_WITHOUT_USERS);
    Date loadedDate = workflowMeta.getModifiedDate();

    workflowMeta.stampModified();

    assertTrue(
        workflowMeta.getModifiedDate().after(loadedDate),
        "the modification date has to move forward when the file is saved after an edit");
  }

  @Test
  void authenticatedUserIsRecordedAsTheLastToModify() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_WITHOUT_USERS);
    authenticateAs(EDITOR);

    pipelineMeta.stampModified();

    assertEquals(EDITOR, pipelineMeta.getModifiedUser());
    assertTrue(
        pipelineMeta.getXml(variables).contains("<modified_user>" + EDITOR + "</modified_user>"));
  }

  @Test
  void aModificationNeverRewritesTheCreator() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_WITHOUT_USERS);
    Date createdDate = pipelineMeta.getCreatedDate();
    authenticateAs(EDITOR);

    pipelineMeta.stampModified();

    assertEquals(
        NO_USER,
        pipelineMeta.getCreatedUser(),
        "whoever saves a file is not its creator: an unknown creator stays unknown");
    assertEquals(createdDate, pipelineMeta.getCreatedDate());
  }

  @Test
  void aKnownCreatorSurvivesAModificationBySomeoneElse() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    authenticateAs(AUTHOR);
    pipelineMeta.stampCreated();

    authenticateAs(EDITOR);
    pipelineMeta.stampModified();

    assertEquals(AUTHOR, pipelineMeta.getCreatedUser());
    assertEquals(EDITOR, pipelineMeta.getModifiedUser());
  }

  @Test
  void creationRecordsTheAuthenticatedUserAsCreatorAndModifier() {
    authenticateAs(AUTHOR);
    PipelineMeta pipelineMeta = new PipelineMeta();
    WorkflowMeta workflowMeta = new WorkflowMeta();

    pipelineMeta.stampCreated();
    workflowMeta.stampCreated();

    assertEquals(AUTHOR, pipelineMeta.getCreatedUser());
    assertEquals(AUTHOR, pipelineMeta.getModifiedUser());
    assertEquals(AUTHOR, workflowMeta.getCreatedUser());
    assertEquals(AUTHOR, workflowMeta.getModifiedUser());
  }

  @Test
  void withoutAnAuthenticatedUserTheUserElementsAreLeftAlone() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_WITHOUT_USERS);
    WorkflowMeta workflowMeta = loadWorkflow(WORKFLOW_WITHOUT_USERS);

    // The desktop default: an unrestricted context with an anonymous placeholder for a name.
    pipelineMeta.stampCreated();
    pipelineMeta.stampModified();
    workflowMeta.stampCreated();
    workflowMeta.stampModified();

    assertEquals(NO_USER, pipelineMeta.getCreatedUser());
    assertEquals(NO_USER, pipelineMeta.getModifiedUser());
    assertEquals(NO_USER, workflowMeta.getCreatedUser());
    assertEquals(NO_USER, workflowMeta.getModifiedUser());
    assertTrue(
        pipelineMeta.getXml(variables).contains("<modified_user>-</modified_user>"),
        "a file saved on the desktop keeps the placeholder earlier releases wrote");
  }

  @Test
  void anUntouchedFileThatIsSavedAgainIsNotStamped() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_WITHOUT_USERS);

    assertFalse(
        pipelineMeta.needsModificationStamp(true),
        "saving an untouched file over the existing one has to leave the file as it is");

    pipelineMeta.setChanged();

    assertTrue(
        pipelineMeta.needsModificationStamp(true),
        "saving an edited file has to record the change");
  }

  @Test
  void aFileWrittenForTheFirstTimeIsAlwaysStamped() throws Exception {
    PipelineMeta pipelineMeta = loadPipeline(PIPELINE_WITHOUT_USERS);

    assertTrue(
        pipelineMeta.needsModificationStamp(false),
        "a file that is not there yet is being written, however the changed flag stands");
  }

  @Test
  void theAnonymousPlaceholderIsNeverWrittenAsAUser() {
    HopSecurity.setProvider(HopSecurityContext::unrestricted);
    PipelineMeta pipelineMeta = new PipelineMeta();

    pipelineMeta.stampCreated();
    pipelineMeta.stampModified();

    assertEquals(NO_USER, pipelineMeta.getCreatedUser());
    assertEquals(NO_USER, pipelineMeta.getModifiedUser());
  }
}
