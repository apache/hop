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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The collection of MongoDB delete must survive a "Get collections" that can't reach the
 * collections, for example because the connection can't be loaded (follow-up of issue #5953).
 */
@Tag("uitest")
class MongoDbDeleteDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "mongodb delete";
  private static final String TITLE =
      BaseMessages.getString(MongoDbDeleteDialog.class, "MongoDbDeleteDialog.Shell.Title");
  private static final String GET_COLLECTIONS =
      BaseMessages.getString(
          MongoDbDeleteDialog.class, "MongoDbDeleteDialog.GetCollections.Button");

  @Test
  void collectionSurvivesUnknownConnection() {
    MongoDbDeleteMeta meta = collection("no-such-connection");

    getCollectionsAndPressOk(meta);

    assertEquals("people", meta.getCollection(), "OK must keep the configured collection");
  }

  @Test
  void collectionSurvivesMissingConnection() {
    MongoDbDeleteMeta meta = collection(null);

    getCollectionsAndPressOk(meta);

    assertEquals("people", meta.getCollection(), "OK must keep the configured collection");
  }

  private void getCollectionsAndPressOk(MongoDbDeleteMeta meta) {
    PipelineMeta pipelineMeta = UpstreamFixture.withoutUpstream(TRANSFORM_NAME, meta);
    withDialog(
        parent -> new MongoDbDeleteDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          Button getCollections = bot.shell(TITLE).activate().bot().button(GET_COLLECTIONS).widget;
          postEvent(getCollections, SWT.Selection);
          assertTrue(closeOtherShells(TITLE, 3000) > 0, "getting the collections should fail");
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static MongoDbDeleteMeta collection(String connectionName) {
    MongoDbDeleteMeta meta = new MongoDbDeleteMeta();
    meta.setDefault();
    meta.setConnectionName(connectionName);
    meta.setCollection("people");
    // A delete query, so OK doesn't warn about missing delete criteria.
    meta.setUseJsonQuery(true);
    meta.setJsonQuery("{\"id\": 1}");
    return meta;
  }
}
