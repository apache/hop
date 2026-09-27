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

package org.apache.hop.pipeline.transforms.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Kafka Producer must accept OK when the incoming fields can't be loaded (the field checks need
 * them), and a validation that blocks OK must not have saved half of the dialog already.
 */
@Tag("uitest")
class KafkaProducerOutputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "kafka";
  private static final String TITLE =
      BaseMessages.getString(
          KafkaProducerOutputMeta.class, "KafkaProducerOutputDialog.Shell.Title");

  @Test
  void okSavesEditsWithFailingUpstream() {
    KafkaProducerOutputMeta meta = configured();
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    boolean closedByOk = editAndPressOk(meta, pipelineMeta, "wKeyField", "key2");

    assertTrue(closedByOk, "OK must close the dialog when the incoming fields are unknown");
    assertEquals("key2", meta.getKeyField());
    assertEquals("topic_f", meta.getTopicField());
    assertEquals("msg_f", meta.getMessageField());
    assertEquals("hdr_f", meta.getHeadersField());
  }

  @Test
  void okSavesEditsWhenAllFieldsExist() {
    KafkaProducerOutputMeta meta = configured();
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(
            TRANSFORM_NAME, meta, "topic_f", "key2", "msg_f", "hdr_f");

    boolean closedByOk = editAndPressOk(meta, pipelineMeta, "wKeyField", "key2");

    assertTrue(closedByOk);
    assertEquals("key2", meta.getKeyField());
  }

  @Test
  void blockedOkDoesNotSaveAnything() {
    KafkaProducerOutputMeta meta = configured();
    // msg_f is missing upstream, so OK is refused.
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "topic_f", "key_f", "hdr_f");

    boolean closedByOk = editAndPressOk(meta, pipelineMeta, "wBootstrapServers", "new-host:9092");

    assertFalse(closedByOk, "OK must be refused for a missing message field");
    assertEquals("old-host:9092", meta.getDirectBootstrapServers());
  }

  /**
   * Sets a widget's text, presses OK and dismisses whatever message it shows. When the dialog is
   * still open after that, it is cancelled.
   *
   * @return true when OK closed the dialog
   */
  private boolean editAndPressOk(
      KafkaProducerOutputMeta meta, PipelineMeta pipelineMeta, String widget, String text) {
    AtomicReference<KafkaProducerOutputDialog> dialog = new AtomicReference<>();
    AtomicBoolean closedByOk = new AtomicBoolean();
    withDialog(
        parent -> {
          dialog.set(new KafkaProducerOutputDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          SWTBot dialogBot = shownDialog(bot);
          display.syncExec(() -> setText(dialog.get(), widget, text));
          postEvent(dialogBot.button(buttonLabel("System.Button.OK")).widget, SWT.Selection);
          closeOtherShells(TITLE, 2000);
          closedByOk.set(!dialogOpen());
          if (!closedByOk.get()) {
            dialogBot.button(buttonLabel("System.Button.Cancel")).click();
          }
        });
    return closedByOk.get();
  }

  private static boolean dialogOpen() {
    AtomicBoolean open = new AtomicBoolean();
    display.syncExec(
        () -> {
          for (Shell shell : display.getShells()) {
            if (!shell.isDisposed() && shell.isVisible() && TITLE.equals(shell.getText())) {
              open.set(true);
            }
          }
        });
    return open.get();
  }

  private static void setText(Object dialog, String name, String text) {
    try {
      MethodUtils.invokeMethod(FieldUtils.readField(dialog, name, true), "setText", text);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static KafkaProducerOutputMeta configured() {
    KafkaProducerOutputMeta meta = new KafkaProducerOutputMeta();
    meta.setDirectBootstrapServers("old-host:9092");
    meta.setClientId("client");
    meta.setTopicInField(true);
    meta.setTopicField("topic_f");
    meta.setKeyField("key_f");
    meta.setMessageField("msg_f");
    meta.setHeadersField("hdr_f");
    return meta;
  }

  /** The shell is found as soon as it is created; wait until open() has built and shown it. */
  private static SWTBot shownDialog(SWTBot bot) {
    SWTBotShell shell = bot.shell(TITLE);
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            return shell.isVisible();
          }

          @Override
          public String getFailureMessage() {
            return "Dialog " + TITLE + " was never shown";
          }
        });
    return shell.bot();
  }
}
