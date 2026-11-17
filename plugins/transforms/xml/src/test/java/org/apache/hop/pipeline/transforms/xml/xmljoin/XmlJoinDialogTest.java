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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.w3c.dom.Document;

/**
 * SWTBot coverage for {@link XmlJoinDialog} (#9515). The tests find each option by the title of its
 * box and the label in front of it: OK must store exactly what the transform had, edits must be
 * stored, and Cancel must leave the transform alone.
 */
@Tag("uitest")
class XmlJoinDialogTest extends SwtBotTestBase {

  private static final Class<?> PKG = XmlJoinMeta.class;
  private static final String TRANSFORM_NAME = "xml join";
  private static final String TARGET_TRANSFORM = "target";
  private static final String SOURCE_TRANSFORM = "source";
  private static final List<String> TARGET_FIELDS = List.of("id", "doc");
  private static final List<String> SOURCE_FIELDS = List.of("orderId", "line");

  /** An option of the dialog: the message keys of its box title and of its label. */
  private record Option(String box, String label) {}

  private static final String TARGET_BOX = "XmlJoin.TargetGroup.Label";
  private static final String SOURCE_BOX = "XmlJoin.SourceGroup.Label";
  private static final String JOIN_BOX = "XmlJoin.JoinGroup.Label";
  private static final String RESULT_BOX = "XmlJoin.ResultGroup.Label";

  private static final Option TARGET_XML_TRANSFORM =
      new Option(TARGET_BOX, "XmlJoin.TargetXMLTransform.Label");
  private static final Option TARGET_XML_FIELD =
      new Option(TARGET_BOX, "XmlJoin.TargetXMLField.Label");
  private static final Option SOURCE_XML_TRANSFORM =
      new Option(SOURCE_BOX, "XmlJoin.SourceXMLTransform.Label");
  private static final Option SOURCE_XML_FIELD =
      new Option(SOURCE_BOX, "XmlJoin.SourceXMLField.Label");
  private static final Option JOIN_MODE = new Option(JOIN_BOX, "XmlJoin.JoinMode.Label");
  private static final Option TARGET_XPATH = new Option(JOIN_BOX, "XmlJoin.TargetXPath.Label");
  private static final Option KEY_FIELD = new Option(JOIN_BOX, "XmlJoin.JoinCompareFiled.Label");
  private static final Option VALUE_XML_FIELD =
      new Option(RESULT_BOX, "XmlJoin.ValueXMLField.Label");
  private static final Option ENCODING = new Option(RESULT_BOX, "XmlJoin.Encoding.Label");
  private static final Option OMIT_XML_HEADER =
      new Option(RESULT_BOX, "XmlJoin.OmitXMLHeader.Label");
  private static final Option OMIT_NULL_VALUES =
      new Option(RESULT_BOX, "XmlJoin.OmitNullValues.Label");

  /**
   * The dialog builds its options from the annotated fields of {@link XmlJoinMeta}. Those are
   * looked up in the registry by class name, and the unit-test JVM does not always run the {@code
   * GuiPluginType} scan, so register them the way that scan does.
   */
  @BeforeAll
  static void registerMetaWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            XmlJoinMeta.class.getName(), XmlJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : XmlJoinMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(XmlJoinMeta.class.getName(), element, field);
      }
    }
  }

  static Stream<Arguments> transforms() {
    return Stream.of(
        Arguments.of("a new transform", new XmlJoinMeta()),
        Arguments.of(
            "a simple join with every option set",
            meta(
                m -> {
                  m.setTargetXmlTransform(TARGET_TRANSFORM);
                  m.setTargetXmlField("doc");
                  m.setSourceXmlTransform(SOURCE_TRANSFORM);
                  m.setSourceXmlField("line");
                  m.setValueXmlField("result");
                  m.setTargetXPath("//list");
                  m.setEncoding("ISO-8859-1");
                  m.setOmitXmlHeader(true);
                  m.setOmitNullValues(true);
                })),
        Arguments.of(
            "a complex join with variables and a blank encoding",
            meta(
                m -> {
                  m.setTargetXmlTransform(TARGET_TRANSFORM);
                  m.setTargetXmlField("${TARGET_FIELD}");
                  m.setSourceXmlTransform(SOURCE_TRANSFORM);
                  m.setSourceXmlField("${SOURCE_FIELD}");
                  m.setValueXmlField("${RESULT_FIELD}");
                  m.setTargetXPath("//order[@id='?']/lines");
                  m.setComplexJoin(true);
                  m.setJoinCompareField("${COMPARE_FIELD}");
                  m.setEncoding("");
                })),
        Arguments.of(
            "a comparison field kept while the complex join is off",
            meta(
                m -> {
                  m.setTargetXmlTransform(TARGET_TRANSFORM);
                  m.setTargetXmlField("doc");
                  m.setSourceXmlTransform(SOURCE_TRANSFORM);
                  m.setSourceXmlField("line");
                  m.setValueXmlField("result");
                  m.setTargetXPath("/root");
                  m.setJoinCompareField("orderId");
                })),
        Arguments.of(
            "transforms that are no longer connected",
            meta(
                m -> {
                  m.setTargetXmlTransform("removed target");
                  m.setTargetXmlField("doc");
                  m.setSourceXmlTransform("removed source");
                  m.setSourceXmlField("line");
                  m.setValueXmlField("result");
                  m.setTargetXPath("/root");
                })));
  }

  /**
   * OK without touching anything must store exactly what the transform had, and not flag it. An
   * option that was never set is stored as an empty string, which is how saved pipelines have
   * always looked after the dialog (an empty tag rather than no tag).
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("transforms")
  void okWithoutChangesKeepsEveryOption(String description, XmlJoinMeta meta) throws HopException {
    String expected = XmlMetadataUtil.serializeObjectToXml(withEmptyStringsForUnset(meta));
    meta.setChanged(false);

    withXmlJoinDialog(meta, dialog -> clickButton(dialog, "System.Button.OK"));

    assertEquals(expected, XmlMetadataUtil.serializeObjectToXml(meta), description);
    assertFalse(meta.hasChanged(), "OK without edits must not mark " + description + " changed");
  }

  private static XmlJoinMeta withEmptyStringsForUnset(XmlJoinMeta meta) {
    XmlJoinMeta copy = new XmlJoinMeta();
    copy.setTargetXmlTransform(Const.NVL(meta.getTargetXmlTransform(), ""));
    copy.setTargetXmlField(Const.NVL(meta.getTargetXmlField(), ""));
    copy.setSourceXmlTransform(Const.NVL(meta.getSourceXmlTransform(), ""));
    copy.setSourceXmlField(Const.NVL(meta.getSourceXmlField(), ""));
    copy.setValueXmlField(Const.NVL(meta.getValueXmlField(), ""));
    copy.setTargetXPath(Const.NVL(meta.getTargetXPath(), ""));
    copy.setComplexJoin(meta.isComplexJoin());
    copy.setJoinCompareField(Const.NVL(meta.getJoinCompareField(), ""));
    copy.setEncoding(Const.NVL(meta.getEncoding(), ""));
    copy.setOmitXmlHeader(meta.isOmitXmlHeader());
    copy.setOmitNullValues(meta.isOmitNullValues());
    return copy;
  }

  /** Every option edited in the dialog must reach the transform on OK. */
  @Test
  void okStoresEveryEditedOption() {
    XmlJoinMeta meta = new XmlJoinMeta();

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                setValue(shell, TARGET_XML_TRANSFORM, TARGET_TRANSFORM);
                setValue(shell, TARGET_XML_FIELD, "doc");
                setValue(shell, SOURCE_XML_TRANSFORM, SOURCE_TRANSFORM);
                setValue(shell, SOURCE_XML_FIELD, "line");
                setValue(shell, JOIN_MODE, XmlJoinMode.BY_KEY.getDescription());
                setValue(shell, TARGET_XPATH, "//order[@id='?']/lines");
                setValue(shell, KEY_FIELD, "orderId");
                setValue(shell, VALUE_XML_FIELD, "result");
                setValue(shell, ENCODING, "ISO-8859-1");
                setChecked(shell, OMIT_XML_HEADER, true);
                setChecked(shell, OMIT_NULL_VALUES, true);
              });
          clickButton(dialog, "System.Button.OK");
        });

    assertEquals(TARGET_TRANSFORM, meta.getTargetXmlTransform());
    assertEquals("doc", meta.getTargetXmlField());
    assertEquals(SOURCE_TRANSFORM, meta.getSourceXmlTransform());
    assertEquals("line", meta.getSourceXmlField());
    assertEquals("//order[@id='?']/lines", meta.getTargetXPath());
    assertTrue(meta.isComplexJoin());
    assertEquals("orderId", meta.getJoinCompareField());
    assertEquals("result", meta.getValueXmlField());
    assertEquals("ISO-8859-1", meta.getEncoding());
    assertTrue(meta.isOmitXmlHeader());
    assertTrue(meta.isOmitNullValues());
    assertTrue(meta.hasChanged(), "edits must mark the transform changed");
  }

  /** Clearing the encoding must store a blank encoding, not fall back to UTF-8. */
  @Test
  void okStoresABlankEncoding() {
    XmlJoinMeta meta = new XmlJoinMeta();

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(() -> setValue(dialogShell(), ENCODING, ""));
          clickButton(dialog, "System.Button.OK");
        });

    assertEquals("", meta.getEncoding());
  }

  @Test
  void cancelLeavesTheTransformAlone() throws HopException {
    XmlJoinMeta meta =
        meta(
            m -> {
              m.setTargetXmlTransform(TARGET_TRANSFORM);
              m.setTargetXmlField("doc");
              m.setTargetXPath("/root");
            });
    String before = XmlMetadataUtil.serializeObjectToXml(meta);
    meta.setChanged(false);

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                setValue(shell, TARGET_XML_FIELD, "other");
                setValue(shell, TARGET_XPATH, "//other");
                setValue(shell, JOIN_MODE, XmlJoinMode.BY_KEY.getDescription());
              });
          clickButton(dialog, "System.Button.Cancel");
        });

    assertEquals(before, XmlMetadataUtil.serializeObjectToXml(meta));
    assertFalse(meta.hasChanged(), "Cancel must restore the changed flag");
  }

  /** The join mode shows and stores the complex join flag of the transform. */
  @Test
  void theJoinModeShowsAndStoresTheComplexJoin() {
    XmlJoinMeta meta = meta(m -> m.setComplexJoin(true));
    for (XmlJoinMode mode : XmlJoinMode.values()) {
      String description = mode.getDescription();
      assertFalse(
          description.startsWith("!") && description.endsWith("!"), "no description " + mode);
    }

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                Control joinMode = inputFor(shell, JOIN_MODE);
                assertEquals(
                    List.of(
                        XmlJoinMode.ONE_NODE.getDescription(), XmlJoinMode.BY_KEY.getDescription()),
                    List.of(itemsOf(joinMode)));
                assertEquals(XmlJoinMode.BY_KEY.getDescription(), textOf(joinMode));
                setValue(shell, JOIN_MODE, XmlJoinMode.ONE_NODE.getDescription());
              });
          clickButton(dialog, "System.Button.OK");
        });

    assertFalse(meta.isComplexJoin(), "'Under one node' is a simple join");
  }

  /** The key field only means something when elements go under the node of their key. */
  @Test
  void theKeyFieldFollowsTheJoinMode() {
    XmlJoinMeta meta = new XmlJoinMeta();

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                assertFalse(
                    inputFor(shell, KEY_FIELD).isEnabled(), "disabled for elements under one node");
                setValue(shell, JOIN_MODE, XmlJoinMode.BY_KEY.getDescription());
                assertTrue(
                    inputFor(shell, KEY_FIELD).isEnabled(),
                    "enabled for elements under the node of their key");
                setValue(shell, JOIN_MODE, XmlJoinMode.ONE_NODE.getDescription());
                assertFalse(inputFor(shell, KEY_FIELD).isEnabled(), "disabled again");
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /** A line at the top says what the transform does. */
  @Test
  void theDialogExplainsTheTransform() {
    XmlJoinMeta meta = new XmlJoinMeta();
    String intro = message("XmlJoin.Intro");

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () ->
                  assertTrue(
                      controlsOf(dialogShell(), Label.class).stream()
                          .anyMatch(l -> intro.equals(((Label) l).getText())),
                      "the dialog shows '" + intro + "'"));
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /** Both transform combos offer the transforms that send rows to the join. */
  @Test
  void theTransformCombosListTheInputTransforms() {
    XmlJoinMeta meta = new XmlJoinMeta();

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                for (Option option : List.of(TARGET_XML_TRANSFORM, SOURCE_XML_TRANSFORM)) {
                  List<String> items = List.of(itemsOf(inputFor(shell, option)));
                  assertTrue(
                      items.contains(TARGET_TRANSFORM) && items.contains(SOURCE_TRANSFORM),
                      option + " lists " + items);
                }
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /**
   * With two transforms sending rows to the join, picking one as the target leaves the other for
   * the source, and the other way around. A transform that is already chosen stays.
   */
  @Test
  void pickingOneInputFillsTheOther() {
    withXmlJoinDialog(
        new XmlJoinMeta(),
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                setValue(shell, TARGET_XML_TRANSFORM, SOURCE_TRANSFORM);
                assertEquals(TARGET_TRANSFORM, textOf(inputFor(shell, SOURCE_XML_TRANSFORM)));
              });
          clickButton(dialog, "System.Button.Cancel");
        });

    withXmlJoinDialog(
        new XmlJoinMeta(),
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                setValue(shell, SOURCE_XML_TRANSFORM, SOURCE_TRANSFORM);
                assertEquals(TARGET_TRANSFORM, textOf(inputFor(shell, TARGET_XML_TRANSFORM)));
              });
          clickButton(dialog, "System.Button.Cancel");
        });

    withXmlJoinDialog(
        meta(m -> m.setSourceXmlTransform("chosen before")),
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                setValue(shell, TARGET_XML_TRANSFORM, TARGET_TRANSFORM);
                assertEquals("chosen before", textOf(inputFor(shell, SOURCE_XML_TRANSFORM)));
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /**
   * The field combos offer the fields of the transform chosen above them: the target XML field
   * those of the target transform, the source XML field and the key field those of the source
   * transform. They are looked up when a field combo gets focus, not when the dialog opens, so a
   * slow upstream transform does not hold up the dialog.
   */
  @Test
  void theFieldCombosListTheFieldsOfTheChosenTransform() {
    XmlJoinMeta meta =
        meta(
            m -> {
              m.setTargetXmlTransform(TARGET_TRANSFORM);
              m.setSourceXmlTransform(SOURCE_TRANSFORM);
              m.setSourceXmlField("line");
            });
    FIELD_LOOKUPS.set(0);

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                assertEquals(0, FIELD_LOOKUPS.get(), "opening the dialog looks up no fields");

                assertEquals(TARGET_FIELDS, List.of(focusedItems(shell, TARGET_XML_FIELD)));
                assertEquals(SOURCE_FIELDS, List.of(focusedItems(shell, SOURCE_XML_FIELD)));
                assertEquals(SOURCE_FIELDS, List.of(focusedItems(shell, KEY_FIELD)));

                setValue(shell, SOURCE_XML_TRANSFORM, TARGET_TRANSFORM);
                assertEquals(TARGET_FIELDS, List.of(focusedItems(shell, SOURCE_XML_FIELD)));
                assertEquals(TARGET_FIELDS, List.of(focusedItems(shell, KEY_FIELD)));
                assertEquals(
                    "line",
                    textOf(inputFor(shell, SOURCE_XML_FIELD)),
                    "refreshing the list keeps the chosen field");

                setValue(shell, TARGET_XML_TRANSFORM, "not an input");
                assertEquals(0, focusedItems(shell, TARGET_XML_FIELD).length);
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /** Gives the option's combo focus the way a click does, then returns what it lists. */
  private static String[] focusedItems(Shell shell, Option option) {
    Control control = inputFor(shell, option);
    control.notifyListeners(SWT.FocusIn, new Event());
    return itemsOf(control);
  }

  /** The options scroll between the transform name and the buttons, never under the buttons. */
  @Test
  void theButtonsStayBelowTheOptionsInASmallDialog() {
    XmlJoinMeta meta = new XmlJoinMeta();

    withXmlJoinDialog(
        meta,
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                Point size = shell.getSize();
                shell.setSize(size.x, 300);
                shell.layout(true, true);
                Control ok = buttonOf(shell, buttonLabel("System.Button.OK"));
                Rectangle button = display.map(ok.getParent(), null, ok.getBounds());
                // The scrolled area that holds every option sits on the left of the sash. The
                // boxes inside it may scroll their own content, which can run past what is visible.
                List<Control> scrolled =
                    controlsOf(shell, ScrolledComposite.class).stream()
                        .filter(c -> c.getParent().getParent() instanceof SashForm)
                        .toList();
                assertEquals(1, scrolled.size(), "one scrolled area for the options");
                Control options = scrolled.get(0);
                Rectangle area = display.map(options.getParent(), null, options.getBounds());
                assertTrue(
                    area.y + area.height <= button.y,
                    "the options end at "
                        + (area.y + area.height)
                        + ", below the top of the OK button at "
                        + button.y);
                Group document = documentBoxOf(shell);
                Rectangle documentArea =
                    display.map(document.getParent(), null, document.getBounds());
                assertTrue(
                    documentArea.y + documentArea.height <= button.y,
                    "the target document ends above the OK button");

                // The last option is out of sight at this size; scrolling down shows it.
                ScrolledComposite scrolledComposite = (ScrolledComposite) options;
                Rectangle visible =
                    display.map(scrolledComposite, null, scrolledComposite.getClientArea());
                Control last = inputFor(shell, OMIT_NULL_VALUES);
                Rectangle hidden = display.map(last.getParent(), null, last.getBounds());
                assertTrue(
                    hidden.y >= visible.y + visible.height,
                    "the last option starts out of sight: " + hidden + " in " + visible);
                scrolledComposite.setOrigin(0, Integer.MAX_VALUE);
                Rectangle lastBounds = display.map(last.getParent(), null, last.getBounds());
                assertTrue(
                    lastBounds.y >= visible.y
                        && lastBounds.y + lastBounds.height <= visible.y + visible.height,
                    "after scrolling down, the last option at "
                        + lastBounds
                        + " is inside the visible area "
                        + visible);

                shell.setSize(size);
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /** The target document sits beside the options, both between the intro and the buttons. */
  @Test
  void theDocumentSitsBesideTheOptions() {
    withXmlJoinDialog(
        new XmlJoinMeta(),
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                Group document = documentBoxOf(shell);
                Control xpath = inputFor(shell, TARGET_XPATH);
                Rectangle d = display.map(document.getParent(), null, document.getBounds());
                Rectangle x = display.map(xpath.getParent(), null, xpath.getBounds());
                assertTrue(d.x >= x.x + x.width, "the document is right of the options");
                assertTrue(
                    TargetDocumentPaneTest.labelTexts(document)
                        .contains(message("XmlJoin.Document.Empty")),
                    "it says how to load the document");
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /**
   * Before a document is loaded, the line under the XPath statement says what the statement does
   * and what is missing to check it, and changes with the statement, the join mode and the key
   * field. The key field row moves down to make room for it.
   */
  @Test
  void withoutADocumentTheLineUnderTheXPathSaysWhatItNeeds() {
    withXmlJoinDialog(
        new XmlJoinMeta(),
        dialog -> {
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                GuiResource resource = GuiResource.getInstance();
                assertCheck(shell, resource.getImageInfo(), message("XmlJoin.XPathHint.Empty"));

                setValue(shell, TARGET_XPATH, "/orders");
                assertCheck(
                    shell, resource.getImageInfo(), message("XmlJoin.XPathHint.NotChecked"));

                setValue(shell, JOIN_MODE, XmlJoinMode.BY_KEY.getDescription());
                assertCheck(
                    shell, resource.getImageWarning(), message("XmlJoin.XPathHint.NoPlaceholder"));

                setValue(shell, TARGET_XPATH, "/orders/order[@id='?']");
                assertCheck(
                    shell, resource.getImageWarning(), message("XmlJoin.XPathHint.ByKeyNoField"));

                setValue(shell, KEY_FIELD, "orderId");
                assertCheck(
                    shell,
                    resource.getImageInfo(),
                    message("XmlJoin.XPathHint.NotChecked.ByKey", "orderId"));

                setValue(shell, TARGET_XPATH, "?");
                assertCheck(
                    shell, resource.getImageInfo(), message("XmlJoin.XPathHint.FromKeyField"));

                Control xpath = inputFor(shell, TARGET_XPATH);
                Control keyField = inputFor(shell, KEY_FIELD);
                Composite line = checkLineOf(shell);
                Rectangle x = display.map(xpath.getParent(), null, xpath.getBounds());
                Rectangle h = display.map(line.getParent(), null, line.getBounds());
                Rectangle k = display.map(keyField.getParent(), null, keyField.getBounds());
                assertTrue(h.y >= x.y + x.height, "the line is under the statement");
                assertTrue(k.y >= h.y + h.height, "the key field is under the line");
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /**
   * With the target document loaded, the line under the XPath statement checks it as it is typed:
   * in green the node the elements go under and its parent, in red no node or why the statement is
   * not valid XPath.
   */
  @Test
  void theLineUnderTheXPathChecksTheStatementAgainstTheDocument() throws Exception {
    XmlJoinMeta meta = targetAndSourceByKey();
    meta.setComplexJoin(false);
    withXmlJoinDialog(
        meta,
        TargetDocumentPaneTest.parse(ORDERS),
        dialog -> {
          dialog.button(message("XmlJoin.Document.Load.Button")).click();
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                GuiResource resource = GuiResource.getInstance();
                String highlighted = message("XmlJoin.XPathHint.Highlighted");

                setValue(shell, TARGET_XPATH, "/orders/order[2]/lines");
                assertCheck(
                    shell,
                    resource.getImageSuccess(),
                    message(
                            "XmlJoin.XPathHint.Found",
                            message(
                                "XmlJoin.XPathHint.Node.InParent",
                                "<lines/>",
                                "<order orderId=\"8\">"))
                        + " "
                        + highlighted);
                assertEquals(resource.getColorDarkGreen(), checkTextOf(shell).getForeground());

                setValue(shell, TARGET_XPATH, "/orders");
                assertCheck(
                    shell,
                    resource.getImageSuccess(),
                    message(
                            "XmlJoin.XPathHint.Found",
                            message("XmlJoin.XPathHint.Node.Root", "<orders>"))
                        + " "
                        + highlighted);

                setValue(shell, TARGET_XPATH, "/orders/nothing");
                assertCheck(shell, resource.getImageError(), message("XmlJoin.XPathHint.NotFound"));
                assertEquals(resource.getColorRed(), checkTextOf(shell).getForeground());

                setValue(shell, TARGET_XPATH, "/orders/");
                assertCheck(
                    shell,
                    resource.getImageError(),
                    message(
                        "XmlJoin.XPathHint.Invalid",
                        "A location step was expected following the '/' or '//' token."));
                assertEquals(resource.getColorRed(), checkTextOf(shell).getForeground());

                setValue(shell, TARGET_XPATH, "");
                assertCheck(shell, resource.getImageInfo(), message("XmlJoin.XPathHint.Empty"));
                assertFalse(
                    resource.getColorRed().equals(checkTextOf(shell).getForeground()),
                    "no colour without a result");
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /**
   * Load shows the first target document. The statement for the node clicked in it goes in the
   * XPath statement, an empty key field gets the source field named like the key, and the line
   * under the statement shows it for the key of that node.
   */
  @Test
  void clickingANodeInTheDocumentFillsTheXPathAndTheKeyField() throws Exception {
    XmlJoinMeta meta = targetAndSourceByKey();

    withXmlJoinDialog(
        meta,
        TargetDocumentPaneTest.parse(ORDERS),
        dialog -> {
          dialog.button(message("XmlJoin.Document.Load.Button")).click();
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                TargetDocumentPaneTest.select(
                    TargetDocumentPaneTest.treeOf(documentBoxOf(shell)),
                    "<orders>",
                    "<order orderId=\"8\">",
                    "<lines/>");
                assertEquals(
                    "/orders/order[@orderId='?']/lines", textOf(inputFor(shell, TARGET_XPATH)));
                assertEquals("orderId", textOf(inputFor(shell, KEY_FIELD)));
                assertCheck(
                    shell,
                    GuiResource.getInstance().getImageSuccess(),
                    message(
                            "XmlJoin.XPathHint.Found.ByKey",
                            "8",
                            message(
                                "XmlJoin.XPathHint.Node.InParent",
                                "<lines/>",
                                "<order orderId=\"8\">"))
                        + " "
                        + message("XmlJoin.XPathHint.KeyNodes.Many", 2)
                        + " "
                        + message("XmlJoin.XPathHint.Highlighted"));
                assertEquals(
                    "For key 8, the elements go under <lines/> in <order orderId=\"8\">. It is one"
                        + " of 2 nodes with a key. It is highlighted in the target document.",
                    checkTextOf(shell).getText(),
                    "as a person reads it");
                assertTrue(
                    checkTextOf(shell)
                        .getToolTipText()
                        .contains(
                            message(
                                "XmlJoin.XPathHint.Example",
                                "8",
                                "/orders/order[@orderId='8']/lines")),
                    "the tooltip has the statement for the key");
              });
          clickButton(dialog, "System.Button.OK");
        });

    assertEquals("/orders/order[@orderId='?']/lines", meta.getTargetXPath());
    assertEquals("orderId", meta.getJoinCompareField());
    assertTrue(meta.hasChanged());
  }

  /**
   * A statement typed in the dialog highlights the node it finds. Switching the join mode writes a
   * statement the document wrote again for the new mode, but leaves one typed by hand alone.
   */
  @Test
  void theDocumentFollowsTheStatementAndTheJoinMode() throws Exception {
    withXmlJoinDialog(
        targetAndSourceByKey(),
        TargetDocumentPaneTest.parse(ORDERS),
        dialog -> {
          dialog.button(message("XmlJoin.Document.Load.Button")).click();
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                Group document = documentBoxOf(shell);

                setValue(shell, TARGET_XPATH, "/orders/order[@orderId='?']/lines");
                TreeItem[] selection = TargetDocumentPaneTest.treeOf(document).getSelection();
                assertEquals(1, selection.length);
                assertEquals("<lines/>", selection[0].getText());

                setValue(shell, TARGET_XPATH, "/orders/nothing");
                assertEquals(0, TargetDocumentPaneTest.treeOf(document).getSelectionCount());
                assertCheck(
                    shell,
                    GuiResource.getInstance().getImageError(),
                    message("XmlJoin.XPathHint.NotFound"));

                TargetDocumentPaneTest.select(
                    TargetDocumentPaneTest.treeOf(document),
                    "<orders>",
                    "<order orderId=\"8\">",
                    "<lines/>");
                setValue(shell, JOIN_MODE, XmlJoinMode.ONE_NODE.getDescription());
                assertEquals(
                    "/orders/order[2]/lines",
                    textOf(inputFor(shell, TARGET_XPATH)),
                    "the statement the document wrote follows the join mode");

                setValue(shell, TARGET_XPATH, "//lines");
                setValue(shell, JOIN_MODE, XmlJoinMode.BY_KEY.getDescription());
                assertEquals(
                    "//lines",
                    textOf(inputFor(shell, TARGET_XPATH)),
                    "a statement typed by hand stays");
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /**
   * A check line over several lines pushes the key field down, and neither it nor a long statement
   * makes the options wider than they are.
   */
  @Test
  void aLongCheckLineWrapsWithoutWideningTheOptions() throws Exception {
    StringBuilder order = new StringBuilder("<order");
    for (int i = 1; i <= 6; i++) {
      order.append(" attribute").append(i).append("=\"").append("v".repeat(40)).append('"');
    }
    XmlJoinMeta meta = targetAndSourceByKey();
    meta.setComplexJoin(false);
    withXmlJoinDialog(
        meta,
        TargetDocumentPaneTest.parse("<orders>" + order + "><lines/></order></orders>"),
        dialog -> {
          dialog.button(message("XmlJoin.Document.Load.Button")).click();
          // A short line first, and the dialog laid out with it, as when someone starts typing
          // In a small dialog the options scroll: the box must grow, and what its content scrolls
          // over too, for the key field to stay in reach
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                shell.setSize(shell.getSize().x, 360);
                setValue(shell, TARGET_XPATH, "/orders");
              });
          dialog.sleep(500);
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                ScrolledComposite options = optionsOf(shell);
                int minWidth = options.getMinWidth();

                // The line grows from one line to several while the statement gets long
                setValue(
                    shell,
                    TARGET_XPATH,
                    "/orders/order/lines[name() != '" + "x".repeat(300) + "']");

                Composite line = checkLineOf(shell);
                Control icon = line.getChildren()[0];
                assertTrue(checkTextOf(shell).getText().contains("attribute6"), "the whole parent");
                assertTrue(line.getSize().y > 2 * icon.getSize().y, "the line wraps");
                Rectangle l = display.map(line.getParent(), null, line.getBounds());
                Control keyField = inputFor(shell, KEY_FIELD).getParent();
                Rectangle k = display.map(keyField.getParent(), null, keyField.getBounds());
                assertTrue(k.y >= l.y + l.height, "the key field is under the longer line");
                // Each box scrolls on its own: what it shows must hold the key field
                ScrolledComposite box = (ScrolledComposite) line.getParent().getParent();
                Rectangle shown = display.map(box, null, box.getClientArea());
                assertTrue(k.y + k.height <= shown.y + shown.height, "the box grows with the line");
                Rectangle inBox = keyField.getBounds();
                assertTrue(
                    inBox.y + inBox.height <= box.getContent().getSize().y,
                    "the box scrolls over the key field");
                assertEquals(minWidth, options.getMinWidth(), "the options are as wide as before");
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /**
   * Under the node of their key, the line under the statement warns when other nodes have the key
   * of the clicked node, and says which node the transform takes. A ? it can't follow is not
   * checked.
   */
  @Test
  void theLineUnderTheXPathWarnsAboutAKeyOtherNodesShare() throws Exception {
    XmlJoinMeta meta = targetAndSourceByKey();
    meta.setJoinCompareField("currency");
    withXmlJoinDialog(
        meta,
        TargetDocumentPaneTest.parse(
            "<orders><order currency=\"EUR\" no=\"1\"><lines/></order>"
                + "<order currency=\"EUR\" no=\"2\"><lines/></order></orders>"),
        dialog -> {
          dialog.button(message("XmlJoin.Document.Load.Button")).click();
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                TargetDocumentPaneTest.select(
                    TargetDocumentPaneTest.treeOf(documentBoxOf(shell)),
                    "<orders>",
                    "<order currency=\"EUR\" no=\"2\">",
                    "<lines/>");
                assertEquals(
                    "/orders/order[@currency='?']/lines", textOf(inputFor(shell, TARGET_XPATH)));
                assertCheck(
                    shell,
                    GuiResource.getInstance().getImageWarning(),
                    message(
                        "XmlJoin.XPathHint.KeyNotUnique",
                        "EUR",
                        message(
                            "XmlJoin.XPathHint.Node.InParent",
                            "<lines/>",
                            "<order currency=\"EUR\" no=\"1\">")));

                setValue(shell, TARGET_XPATH, "/orders/order[@no=?]/lines");
                assertCheck(
                    shell,
                    GuiResource.getInstance().getImageInfo(),
                    message("XmlJoin.XPathHint.NotFollowed"));
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  /** Another target transform or field means another document: the one shown goes away. */
  @Test
  void anotherTargetClearsTheDocument() throws Exception {
    withXmlJoinDialog(
        targetAndSourceByKey(),
        TargetDocumentPaneTest.parse(ORDERS),
        dialog -> {
          dialog.button(message("XmlJoin.Document.Load.Button")).click();
          display.syncExec(
              () -> {
                Shell shell = dialogShell();
                Group document = documentBoxOf(shell);
                assertEquals(1, TargetDocumentPaneTest.treeOf(document).getItemCount());
                setValue(shell, TARGET_XML_FIELD, "id");
                assertEquals(0, TargetDocumentPaneTest.treeOf(document).getItemCount());
                assertTrue(
                    TargetDocumentPaneTest.labelTexts(document)
                        .contains(message("XmlJoin.Document.Empty")));
              });
          clickButton(dialog, "System.Button.Cancel");
        });
  }

  private static final String ORDERS =
      "<orders><order orderId=\"7\"><lines/></order><order orderId=\"8\"><lines/></order>"
          + "</orders>";

  private static XmlJoinMeta targetAndSourceByKey() {
    return meta(
        m -> {
          m.setTargetXmlTransform(TARGET_TRANSFORM);
          m.setTargetXmlField("doc");
          m.setSourceXmlTransform(SOURCE_TRANSFORM);
          m.setSourceXmlField("line");
          m.setComplexJoin(true);
        });
  }

  /** The box with the target document beside the options. */
  private static Group documentBoxOf(Shell shell) {
    String title = message("XmlJoin.Document.Group.Label");
    return controlsOf(shell, Group.class).stream()
        .map(Group.class::cast)
        .filter(g -> title.equals(g.getText()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no box titled '" + title + "'"));
  }

  private static String message(String key, Object... arguments) {
    return TargetDocumentPaneTest.message(key, arguments);
  }

  /** The scrolled options left of the target document. */
  private static ScrolledComposite optionsOf(Shell shell) {
    return (ScrolledComposite)
        controlsOf(shell, ScrolledComposite.class).stream()
            .filter(c -> c.getParent().getParent() instanceof SashForm)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no scrolled options"));
  }

  /** The check line: the icon and text attached under the text box with variables. */
  private static Composite checkLineOf(Shell shell) {
    Control xpath = inputFor(shell, TARGET_XPATH).getParent();
    return controlsOf(xpath.getParent(), Composite.class).stream()
        .map(Composite.class::cast)
        .filter(
            c ->
                c.getLayoutData() instanceof FormData fd
                    && fd.top != null
                    && fd.top.control == xpath)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no line under the XPath statement"));
  }

  private static Label checkTextOf(Shell shell) {
    return (Label) checkLineOf(shell).getChildren()[1];
  }

  private static void assertCheck(Shell shell, Image icon, String text) {
    // BaseMessages shows a message it can't find or format as !key!
    assertFalse(text.contains("!XmlJoin."), "a message is missing: " + text);
    assertEquals(text, checkTextOf(shell).getText());
    assertSame(icon, ((Label) checkLineOf(shell).getChildren()[0]).getImage(), text);
  }

  // ---------------------------------------------------------------------------------------------

  private void withXmlJoinDialog(
      XmlJoinMeta meta, Document targetDocument, Consumer<SWTBot> interactions) {
    PipelineMeta pipelineMeta = pipelineWith(meta);
    withDialog(
        parent ->
            new XmlJoinDialog(parent, new Variables(), meta, pipelineMeta) {
              @Override
              Document firstTargetDocument(String targetTransform, String targetField) {
                assertEquals(TARGET_TRANSFORM, targetTransform);
                return targetDocument;
              }
            }.open(),
        bot -> interactions.accept(bot.shell(shellTitle()).activate().bot()));
  }

  private void withXmlJoinDialog(XmlJoinMeta meta, Consumer<SWTBot> interactions) {
    PipelineMeta pipelineMeta = pipelineWith(meta);
    withDialog(
        parent -> new XmlJoinDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(shellTitle()).activate().bot();
          interactions.accept(dialog);
        });
  }

  private static XmlJoinMeta meta(Consumer<XmlJoinMeta> setup) {
    XmlJoinMeta meta = new XmlJoinMeta();
    setup.accept(meta);
    return meta;
  }

  private static String shellTitle() {
    return message("XmlJoin.DialogTitle");
  }

  private static void clickButton(SWTBot dialog, String key) {
    dialog.button(buttonLabel(key)).click();
  }

  /** A pipeline where "target" and "source" both have a hop into the XML join. */
  private static PipelineMeta pipelineWith(XmlJoinMeta meta) {
    PipelineMeta pipelineMeta = new XmlJoinTestPipeline();
    pipelineMeta.setMetadataProvider(new MemoryMetadataProvider());
    pipelineMeta.addTransform(new TransformMeta("XMLJoin", TRANSFORM_NAME, meta));
    return pipelineMeta;
  }

  /** How often the dialog asked the pipeline for the fields of a transform. */
  private static final AtomicInteger FIELD_LOOKUPS = new AtomicInteger();

  /** Reports two input transforms and their fields without needing their plugins here. */
  private static final class XmlJoinTestPipeline extends PipelineMeta {
    @Override
    public List<TransformMeta> findPreviousTransforms(TransformMeta transformMeta, boolean info) {
      List<TransformMeta> previous = new ArrayList<>();
      previous.add(new TransformMeta(TARGET_TRANSFORM, null));
      previous.add(new TransformMeta(SOURCE_TRANSFORM, null));
      return previous;
    }

    @Override
    public IRowMeta getTransformFields(IVariables variables, String transformName) {
      FIELD_LOOKUPS.incrementAndGet();
      List<String> names;
      if (TARGET_TRANSFORM.equals(transformName)) {
        names = TARGET_FIELDS;
      } else if (SOURCE_TRANSFORM.equals(transformName)) {
        names = SOURCE_FIELDS;
      } else {
        return null;
      }
      RowMeta rowMeta = new RowMeta();
      for (String name : names) {
        rowMeta.addValueMeta(new ValueMetaString(name));
      }
      return rowMeta;
    }
  }

  private static Control buttonOf(Shell shell, String text) {
    for (Control control : controlsOf(shell, Button.class)) {
      if (text.equals(((Button) control).getText().replace("&", ""))) {
        return control;
      }
    }
    throw new AssertionError("no button '" + text + "'");
  }

  private static List<Control> controlsOf(Composite parent, Class<? extends Control> type) {
    List<Control> all = new ArrayList<>();
    collect(parent, all);
    return all.stream().filter(type::isInstance).toList();
  }

  private static String textOf(Control control) {
    if (control instanceof CCombo combo) {
      return combo.getText();
    } else if (control instanceof Combo combo) {
      return combo.getText();
    } else if (control instanceof Text text) {
      return text.getText();
    }
    throw new AssertionError("no text in " + control);
  }

  private static Shell dialogShell() {
    for (Shell shell : display.getShells()) {
      if (!shell.isDisposed() && shellTitle().equals(shell.getText())) {
        return shell;
      }
    }
    throw new AssertionError("the '" + shellTitle() + "' dialog is not open");
  }

  /**
   * The input control of an option: inside the box with the option's title, the first text box,
   * combo or checkbox after the label with the option's text, in widget tree order. A checkbox may
   * also carry the label itself.
   */
  private static Control inputFor(Shell shell, Option option) {
    String boxTitle = message(option.box());
    String label = message(option.label());
    Group box =
        controlsOf(shell, Group.class).stream()
            .map(Group.class::cast)
            .filter(g -> boxTitle.equals(g.getText()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no box titled '" + boxTitle + "'"));
    List<Control> controls = new ArrayList<>();
    collect(box, controls);
    for (int i = 0; i < controls.size(); i++) {
      Control control = controls.get(i);
      if (control instanceof Button button
          && (button.getStyle() & SWT.CHECK) != 0
          && label.equals(button.getText())) {
        return button;
      }
      if (control instanceof Label l && label.equals(l.getText())) {
        for (int j = i + 1; j < controls.size(); j++) {
          if (isInput(controls.get(j))) {
            return controls.get(j);
          }
        }
      }
    }
    throw new AssertionError("no input found for '" + label + "' in '" + boxTitle + "'");
  }

  private static boolean isInput(Control control) {
    return control instanceof Text
        || control instanceof CCombo
        || control instanceof Combo
        || (control instanceof Button button && (button.getStyle() & SWT.CHECK) != 0);
  }

  private static void collect(Control parent, List<Control> controls) {
    controls.add(parent);
    if (parent instanceof Composite composite) {
      for (Control child : composite.getChildren()) {
        collect(child, controls);
      }
    }
  }

  private static void setValue(Shell shell, Option option, String value) {
    Control control = inputFor(shell, option);
    if (control instanceof Text text) {
      text.setText(value);
    } else if (control instanceof CCombo combo) {
      combo.setText(value);
    } else if (control instanceof Combo combo) {
      combo.setText(value);
    } else {
      throw new AssertionError(option + " is not a text box or combo: " + control);
    }
  }

  private static void setChecked(Shell shell, Option option, boolean checked) {
    Control control = inputFor(shell, option);
    assertTrue(control instanceof Button, option + " is not a checkbox: " + control);
    Button button = (Button) control;
    button.setSelection(checked);
    button.notifyListeners(SWT.Selection, new Event());
  }

  private static String[] itemsOf(Control control) {
    assertNotNull(control);
    if (control instanceof CCombo combo) {
      return combo.getItems();
    } else if (control instanceof Combo combo) {
      return combo.getItems();
    }
    throw new AssertionError("not a combo: " + control);
  }
}
