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
package org.apache.hop.mail.pipeline.transforms.mail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.mail.workflow.actions.mail.MailEmbeddedImageField;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

class MailMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<MailMeta> testMetaClass = MailMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes =
        Arrays.asList(
            "server",
            "destination",
            "destinationCc",
            "destinationBCc",
            "replyAddress",
            "replyName",
            "subject",
            "includeDate",
            "includeSubFolders",
            "zipFilenameDynamic",
            "filenameDynamic",
            "dynamicFieldname",
            "dynamicWildcard",
            "dynamicZipFilename",
            "sourcefilefoldername",
            "sourcewildcard",
            "contactPerson",
            "contactPhone",
            "comment",
            "includingFiles",
            "zipFiles",
            "zipFilename",
            "ziplimitsize",
            "usingAuthentication",
            "authenticationUser",
            "authenticationPassword",
            "onlySendComment",
            "useHTML",
            "usingSecureAuthentication",
            "usePriority",
            "port",
            "priority",
            "importance",
            "sensitivity",
            "secureConnectionType",
            "encoding",
            "replyToAddresses",
            "attachContentFromField",
            "attachContentField",
            "attachContentFileNameField",
            "embeddedImages");

    Map<String, String> getterMap =
        new HashMap<String, String>() {
          {
            put("isFilenameDynamic", "FilenameDynamic");
          }
        };
    Map<String, String> setterMap =
        new HashMap<String, String>() {
          {
            put("isFilenameDynamic", "setFilenameDynamic");
            put("embeddedImages", "setEmbeddedImages");
          }
        };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
        new ArrayLoadSaveValidator<>(new StringLoadSaveValidator(), 5);
    ListLoadSaveValidator<MailEmbeddedImageField> listLoadSaveValidator =
        new ListLoadSaveValidator<>(new ImageListLoadSaveValidator());

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put("embeddedImages", listLoadSaveValidator);
    attrValidatorMap.put("contentIds", stringArrayLoadSaveValidator);

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);
  }

  public class ImageListLoadSaveValidator
      implements IFieldLoadSaveValidator<MailEmbeddedImageField> {

    @Override
    public MailEmbeddedImageField getTestObject() {
      MailEmbeddedImageField mailEmbeddedImageField = new MailEmbeddedImageField();
      mailEmbeddedImageField.setContentId(UUID.randomUUID().toString());
      mailEmbeddedImageField.setEmbeddedimage(UUID.randomUUID().toString());
      return mailEmbeddedImageField;
    }

    @Override
    public boolean validateTestObject(MailEmbeddedImageField testObject, Object actual) {
      if (!(actual instanceof MailEmbeddedImageField)) {
        return false;
      }

      MailEmbeddedImageField mailEmbeddedImageField = (MailEmbeddedImageField) actual;
      return new EqualsBuilder()
          .append(mailEmbeddedImageField.getContentId(), testObject.getContentId())
          .append(mailEmbeddedImageField.getEmbeddedimage(), testObject.getEmbeddedimage())
          .isEquals();
    }
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof MailMeta) {
      //      ((MailMeta) someMeta).allocate(5);
    }
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }
}
