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

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The stage name of the Snowflake bulk loader must survive when the list of stages can't be read,
 * for example because no connection is selected or the connection fails (follow-up of issue #5953).
 */
@Tag("uitest")
class SnowflakeBulkLoaderDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "snowflake";
  private static final String TITLE =
      BaseMessages.getString(SnowflakeBulkLoaderMeta.class, "SnowflakeBulkLoader.Dialog.Title");

  @Test
  void stageNameSurvivesMissingConnection() throws HopException {
    SnowflakeBulkLoaderMeta meta = internalStage(null);
    PipelineMeta pipelineMeta = UpstreamFixture.withoutUpstream(TRANSFORM_NAME, meta);
    // The dialog counts the connections of the pipeline, as it would in the GUI.
    pipelineMeta.setMetadataProvider(new MemoryMetadataProvider());

    focusStageNameAndPressOk(meta, pipelineMeta);

    assertEquals("my_stage", meta.getStageName(), "OK must keep the configured stage name");
  }

  @Test
  void stageNameSurvivesFailingConnection() throws HopException {
    SnowflakeBulkLoaderMeta meta = internalStage("broken");
    PipelineMeta pipelineMeta = UpstreamFixture.withoutUpstream(TRANSFORM_NAME, meta);
    // A connection without a JDBC driver: connecting fails right away, no network involved.
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(DatabaseMeta.class)
        .save(new DatabaseMeta("broken", "NONE", "JDBC", "127.0.0.1", "db", "1", "user", "pw"));
    pipelineMeta.setMetadataProvider(metadataProvider);

    focusStageNameAndPressOk(meta, pipelineMeta);

    assertEquals("broken", meta.getConnection());
    assertEquals("my_stage", meta.getStageName(), "OK must keep the configured stage name");
  }

  private void focusStageNameAndPressOk(SnowflakeBulkLoaderMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<SnowflakeBulkLoaderDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          dialog.set(new SnowflakeBulkLoaderDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          bot.shell(TITLE);
          ComboVar wStageName = readField(dialog.get(), "wStageName");
          // Focusing the stage name lists the stages of the connection, which fails.
          postEvent(wStageName.getCComboWidget(), SWT.FocusIn);
          closeOtherShells(TITLE, 1000);
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static SnowflakeBulkLoaderMeta internalStage(String connection) throws HopException {
    SnowflakeBulkLoaderMeta meta = new SnowflakeBulkLoaderMeta();
    meta.setDefault();
    meta.setConnection(connection);
    meta.setLocationType(
        SnowflakeBulkLoaderMeta.LOCATION_TYPE_CODES[
            SnowflakeBulkLoaderMeta.LOCATION_TYPE_INTERNAL_STAGE]);
    meta.setStageName("my_stage");
    return meta;
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(Object target, String name) {
    try {
      return (T) FieldUtils.readField(target, name, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
