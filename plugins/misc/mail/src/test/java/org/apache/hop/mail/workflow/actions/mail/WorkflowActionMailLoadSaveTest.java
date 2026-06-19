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
package org.apache.hop.mail.workflow.actions.mail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.jupiter.api.extension.RegisterExtension;

class WorkflowActionMailLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionMail> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Override
  protected Class<ActionMail> getActionClass() {
    return ActionMail.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList(
        "server",
        "destination",
        "destinationCc",
        "destinationBCc",
        "replyAddress",
        "replyName",
        "subject",
        "includeDate",
        "contactPerson",
        "contactPhone",
        "comment",
        "includingFiles",
        "fileTypes",
        "zipFiles",
        "zipFilename",
        "usingAuthentication",
        "usexoauth2",
        "authenticationUser",
        "authenticationPassword",
        "onlySendComment",
        "useHTML",
        "usingSecureAuthentication",
        "usePriority",
        "trustedHosts",
        "checkServerIdentity",
        "port",
        "priority",
        "importance",
        "sensitivity",
        "secureConnectionType",
        "encoding",
        "replyToAddresses",
        "embeddedimages",
        "connectionName");
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<>();
    validators.put("fileTypes", new ListLoadSaveValidator<>(new StringLoadSaveValidator(), 3));
    validators.put(
        "embeddedimages", new ListLoadSaveValidator<>(new EmbeddedImageFieldValidator(), 2));
    return validators;
  }

  /** Validator for the {@link MailEmbeddedImageField} list field. */
  public static class EmbeddedImageFieldValidator
      implements IFieldLoadSaveValidator<MailEmbeddedImageField> {
    @Override
    public MailEmbeddedImageField getTestObject() {
      MailEmbeddedImageField field = new MailEmbeddedImageField();
      field.setContentId(UUID.randomUUID().toString());
      field.setEmbeddedImage(UUID.randomUUID().toString());
      return field;
    }

    @Override
    public boolean validateTestObject(MailEmbeddedImageField expected, Object actual) {
      if (!(actual instanceof MailEmbeddedImageField got)) {
        return false;
      }
      return new EqualsBuilder()
          .append(expected.getContentId(), got.getContentId())
          .append(expected.getEmbeddedImage(), got.getEmbeddedImage())
          .isEquals();
    }
  }
}
