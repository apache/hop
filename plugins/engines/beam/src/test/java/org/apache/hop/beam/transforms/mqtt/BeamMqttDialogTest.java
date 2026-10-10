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

package org.apache.hop.beam.transforms.mqtt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class BeamMqttDialogTest extends SwtBotTestBase {
  @BeforeAll
  static void widgets() {
    for (Class<?> type : new Class<?>[] {BeamMqttInputMeta.class, BeamMqttOutputMeta.class})
      for (Field f : type.getDeclaredFields()) {
        GuiWidgetElement annotation = f.getAnnotation(GuiWidgetElement.class);
        if (annotation != null)
          GuiRegistry.getInstance().addGuiWidgetElement(type.getName(), annotation, f);
      }
  }

  @Test
  void inputOkPersistsAllOptionsAndMaskedPassword() {
    var m = new BeamMqttInputMeta();
    var p = new PipelineMeta();
    p.addTransform(new TransformMeta("BeamMqttInput", "mqtt", m));
    withDialog(
        parent -> new BeamMqttInputDialog(parent, new Variables(), m, p).open(),
        bot -> {
          var shell = bot.shell("Beam MQTT input").activate();
          var b = shell.bot();
          b.textWithLabel("Server URI").setText("ssl://localhost:8883");
          b.textWithLabel("Topic").setText("sensors/#");
          b.textWithLabel("Client ID prefix").setText("reader");
          b.textWithLabel("Username").setText("user");
          b.textWithLabel("Password").setText("secret");
          display.syncExec(
              () ->
                  assertTrue((b.textWithLabel("Password").widget.getStyle() & SWT.PASSWORD) != 0));
          b.cTabItem("Payload").activate();
          var payloadTypes = b.ccomboBoxWithLabel("Payload type");
          display.syncExec(
              () ->
                  assertArrayEquals(
                      new String[] {"String", "Binary"}, payloadTypes.widget.getItems()));
          b.textWithLabel("Payload field").setText("body");
          b.ccomboBoxWithLabel("Payload type").setText("Binary");
          b.cTabItem("Limits").activate();
          b.textWithLabel("Maximum records").setText("7");
          b.textWithLabel("Maximum read time (seconds)").setText("10");
          b.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("ssl://localhost:8883", m.getServerUri());
    assertEquals("sensors/#", m.getTopic());
    assertEquals("reader", m.getClientId());
    assertEquals("user", m.getUsername());
    assertEquals("secret", m.getPassword());
    assertEquals("body", m.getPayloadField());
    assertEquals("Binary", m.getPayloadType());
    assertEquals("7", m.getMaxNumRecords());
    assertEquals("10", m.getMaxReadTime());
  }

  @Test
  void outputOkPersistsRetainedPayloadAndConnection() {
    var m = new BeamMqttOutputMeta();
    var p = new PipelineMeta();
    p.addTransform(new TransformMeta("BeamMqttOutput", "mqtt", m));
    withDialog(
        parent -> new BeamMqttOutputDialog(parent, new Variables(), m, p).open(),
        bot -> {
          var b = bot.shell("Beam MQTT output").activate().bot();
          b.textWithLabel("Server URI").setText("tcp://localhost:1883");
          b.textWithLabel("Topic").setText("out");
          b.textWithLabel("Client ID prefix").setText("writer");
          b.textWithLabel("Username").setText("user");
          b.textWithLabel("Password").setText("${SECRET}");
          b.cTabItem("Payload").activate();
          var payloadTypes = b.ccomboBoxWithLabel("Payload type");
          display.syncExec(
              () ->
                  assertArrayEquals(
                      new String[] {"String", "Binary"}, payloadTypes.widget.getItems()));
          b.textWithLabel("Payload field").setText("bytes");
          b.ccomboBoxWithLabel("Payload type").setText("Binary");
          b.checkBox().select();
          b.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("tcp://localhost:1883", m.getServerUri());
    assertEquals("out", m.getTopic());
    assertEquals("writer", m.getClientId());
    assertEquals("user", m.getUsername());
    assertEquals("${SECRET}", m.getPassword());
    assertEquals("bytes", m.getPayloadField());
    assertEquals("Binary", m.getPayloadType());
    assertTrue(m.isRetained());
  }

  @Test
  void cancelPreservesValuesAndChangedState() {
    var m = new BeamMqttInputMeta();
    m.setServerUri("tcp://original:1883");
    m.setChanged(false);
    var p = new PipelineMeta();
    p.addTransform(new TransformMeta("BeamMqttInput", "mqtt", m));
    withDialog(
        parent -> new BeamMqttInputDialog(parent, new Variables(), m, p).open(),
        bot -> {
          var b = bot.shell("Beam MQTT input").activate().bot();
          b.textWithLabel("Server URI").setText("discarded");
          b.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals("tcp://original:1883", m.getServerUri());
    assertFalse(m.hasChanged());
  }

  @Test
  void groupedOptionsRemainAboveButtonsWhenResized() {
    var m = new BeamMqttInputMeta();
    var p = new PipelineMeta();
    p.addTransform(new TransformMeta("BeamMqttInput", "mqtt", m));
    withDialog(
        parent -> new BeamMqttInputDialog(parent, new Variables(), m, p).open(),
        bot -> {
          var s = bot.shell("Beam MQTT input").activate();
          display.syncExec(
              () -> {
                s.widget.setSize(660, 360);
                s.widget.layout(true, true);
                var folder = findTabs(s.widget);
                assertNotNull(folder);
                assertEquals(3, folder.getItemCount());
                Button ok = findOk(s.widget);
                // Inner tabs retain their minimum height and are clipped when the shell shrinks.
                // The outer viewport, not that off-screen content, must stay above the button bar.
                org.eclipse.swt.custom.ScrolledComposite viewport = null;
                for (Control child : s.widget.getChildren()) {
                  if (child instanceof org.eclipse.swt.custom.ScrolledComposite scrolled)
                    viewport = scrolled;
                }
                assertNotNull(viewport);
                var top =
                    viewport.getParent().toDisplay(viewport.getBounds().x, viewport.getBounds().y);
                var bottom = ok.getParent().toDisplay(ok.getBounds().x, ok.getBounds().y);
                assertTrue(
                    bottom.y >= top.y + viewport.getSize().y,
                    "viewport=" + top + " size=" + viewport.getSize() + " OK=" + bottom);
                assertTrue(viewport.getExpandVertical());
              });
          s.bot().button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  private CTabFolder findTabs(Composite c) {
    for (Control child : c.getChildren()) {
      if (child instanceof CTabFolder f) return f;
      if (child instanceof Composite nested) {
        var f = findTabs(nested);
        if (f != null) return f;
      }
    }
    return null;
  }

  private Button findOk(Composite c) {
    for (Control child : c.getChildren()) {
      if (child instanceof Button b
          && b.getText().replace("&", "").equals(buttonLabel("System.Button.OK"))) return b;
      if (child instanceof Composite nested) {
        var b = findOk(nested);
        if (b != null) return b;
      }
    }
    return null;
  }
}
