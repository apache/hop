/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.xml.xmljoin.TargetDocumentPane.Check;
import org.apache.hop.pipeline.transforms.xml.xmljoin.TargetDocumentPane.Outcome;
import org.apache.hop.pipeline.transforms.xml.xmljoin.TargetXPathBuilder.KeyCandidate;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * SWTBot coverage for {@link TargetDocumentPane} (#9515): clicking a node writes the statement for
 * it, with a key under the node of their key, and the node a statement finds is highlighted.
 */
@Tag("uitest")
class TargetDocumentPaneTest extends SwtBotTestBase {

  private static final Class<?> PKG = XmlJoinMeta.class;

  private static final String ORDERS =
      """
      <OrderList>
        <OrderHeader orderNumber="1001"><customer>A</customer><OrderLines/></OrderHeader>
        <OrderHeader orderNumber="1002"><customer>B</customer><OrderLines/></OrderHeader>
      </OrderList>""";

  private static final String NESTED =
      """
      <OrderList>
        <OrderHeader orderNumber="1001"><OrderLines><line sku="A"/><line sku="B"/></OrderLines></OrderHeader>
        <OrderHeader orderNumber="1002"><OrderLines><line sku="C"/></OrderLines></OrderHeader>
      </OrderList>""";

  static final String ROOT = "<OrderList>";
  static final String FIRST_HEADER = "<OrderHeader orderNumber=\"1001\">";
  static final String SECOND_HEADER = "<OrderHeader orderNumber=\"1002\">";
  static final String LINES = "<OrderLines/>";

  private final AtomicReference<TargetDocumentPane> pane = new AtomicReference<>();
  private final List<String> written = new ArrayList<>();
  private final AtomicReference<KeyCandidate> writtenKey = new AtomicReference<>();
  private final AtomicInteger loads = new AtomicInteger();

  /** Before a document is loaded, the pane says how to get one; Load asks for it. */
  @Test
  void withoutADocumentThePaneSaysHowToLoadOne() {
    withPane(
        () -> {
          Shell shell = sceneShell();
          assertTrue(labelTexts(shell).contains(message("XmlJoin.Document.Empty")));
          Button load = buttonOf(shell, message("XmlJoin.Document.Load.Button"));
          load.notifyListeners(SWT.Selection, new Event());
          assertEquals(1, loads.get());
        });
  }

  /** Under one node: clicking a node writes the statement that leads to exactly that node. */
  @Test
  void clickingANodeWritesItsStatement() throws Exception {
    Document doc = parse(ORDERS);
    withPane(
        () -> {
          Shell shell = sceneShell();
          pane.get().setDocument(doc, "target");
          Tree tree = treeOf(shell);
          assertEquals(
              List.of(FIRST_HEADER, SECOND_HEADER),
              texts(tree.getItem(0).getItems()),
              "the root is open");
          assertTrue(labelTexts(shell).contains(message("XmlJoin.Document.Click", "target")));

          select(tree, ROOT, SECOND_HEADER, LINES);

          assertEquals(List.of("/OrderList/OrderHeader[2]/OrderLines"), written);
          assertNull(writtenKey.get());
        });
  }

  /**
   * Under the node of their key: the key named like the key names is chosen, and choosing another
   * key writes the statement again. A node without a key writes nothing and says why.
   */
  @Test
  void underTheNodeOfTheirKeyTheKeyIsChosenToo() throws Exception {
    Document doc = parse(ORDERS);
    withPane(
        () -> {
          Shell shell = sceneShell();
          pane.get().setByKey(true);
          pane.get().setKeyNames(() -> List.of("line", "OrderNumber"));
          pane.get().setDocument(doc, "target");
          Tree tree = treeOf(shell);

          select(tree, ROOT);
          assertTrue(written.isEmpty(), "the root has no key");
          assertTrue(labelTexts(shell).contains(message("XmlJoin.Document.NoKey")));

          select(tree, ROOT, SECOND_HEADER, LINES);
          CCombo key = keyOf(shell);
          assertEquals(
              List.of("@orderNumber of OrderHeader = 1002", "customer of OrderHeader = B"),
              List.of(key.getItems()));
          assertEquals("@orderNumber of OrderHeader = 1002", key.getText());
          assertEquals(
              "/OrderList/OrderHeader[@orderNumber='?']/OrderLines",
              written.get(written.size() - 1));
          assertEquals("1002", writtenKey.get().value());

          key.select(1);
          key.notifyListeners(SWT.Selection, new Event());
          assertEquals(
              "/OrderList/OrderHeader[customer='?']/OrderLines", written.get(written.size() - 1));
          assertEquals("customer", writtenKey.get().name());
        });
  }

  /**
   * The node a statement typed in the transform finds is highlighted, with the key it uses, and the
   * check says what the statement finds: the node and its parent, no node, or why it is not valid.
   */
  @Test
  void theNodeTheStatementFindsIsHighlightedAndChecked() throws Exception {
    Document doc = parse(ORDERS);
    withPane(
        () -> {
          Shell shell = sceneShell();
          TargetDocumentPane documentPane = pane.get();
          assertEquals(Outcome.NO_DOCUMENT, documentPane.check().outcome());
          documentPane.setDocument(doc, "target");
          Tree tree = treeOf(shell);

          documentPane.show("//OrderHeader[2]/OrderLines");
          assertSelected(tree, SECOND_HEADER, LINES);
          assertEquals(
              new Check(Outcome.FOUND, LINES, SECOND_HEADER, null, 1, null), documentPane.check());

          documentPane.show("/OrderList");
          assertEquals(new Check(Outcome.FOUND, ROOT, null, null, 1, null), documentPane.check());

          documentPane.show("/OrderList/Nothing");
          assertEquals(0, tree.getSelectionCount());
          assertEquals(Check.of(Outcome.NOT_FOUND), documentPane.check());

          documentPane.show("/OrderList/");
          Check invalid = documentPane.check();
          assertEquals(Outcome.INVALID, invalid.outcome());
          assertEquals(
              "A location step was expected following the '/' or '//' token.", invalid.error());

          documentPane.setByKey(true);
          documentPane.show("?");
          assertEquals(Check.of(Outcome.NOT_CHECKED), documentPane.check());

          documentPane.show("/OrderList/OrderHeader[customer='?']/OrderLines");
          assertSelected(tree, FIRST_HEADER, LINES);
          assertEquals("customer of OrderHeader = A", keyOf(shell).getText());
          assertEquals(
              new Check(Outcome.FOUND, LINES, FIRST_HEADER, "A", 2, null), documentPane.check());
          assertTrue(written.isEmpty(), "showing a statement writes nothing");
        });
  }

  /**
   * Expand all opens every node under the selected node, or the whole document when no node is
   * selected. Before a document is loaded there is nothing to open.
   */
  @Test
  void expandAllOpensTheSelectedNodeOrTheWholeDocument() throws Exception {
    Document doc = parse(NESTED);
    withPane(
        () -> {
          Shell shell = sceneShell();
          ToolItem expandAll = toolOf(shell, "XmlJoin.Document.ExpandAll.Tooltip");
          assertFalse(expandAll.getEnabled(), "no document yet");
          pane.get().setDocument(doc, "target");
          assertTrue(expandAll.getEnabled());
          Tree tree = treeOf(shell);
          TreeItem root = tree.getItem(0);

          select(tree, ROOT, FIRST_HEADER);
          click(expandAll);
          TreeItem firstLines = root.getItem(0).getItem(0);
          assertTrue(firstLines.getExpanded(), "the lines of the selected order are open");
          assertEquals(
              List.of("<line sku=\"A\"/>", "<line sku=\"B\"/>"), texts(firstLines.getItems()));
          assertFalse(root.getItem(1).getExpanded(), "the other order stays closed");

          tree.deselectAll();
          click(expandAll);
          TreeItem secondLines = root.getItem(1).getItem(0);
          assertTrue(root.getItem(1).getExpanded());
          assertTrue(secondLines.getExpanded());
          assertEquals(List.of("<line sku=\"C\"/>"), texts(secondLines.getItems()));
        });
  }

  /**
   * Collapse all closes the selected node and every node under it, and leaves the rest open. With
   * no node selected it closes the whole document.
   */
  @Test
  void collapseAllClosesTheSelectedNodeOrTheWholeDocument() throws Exception {
    Document doc = parse(NESTED);
    withPane(
        () -> {
          Shell shell = sceneShell();
          pane.get().setDocument(doc, "target");
          Tree tree = treeOf(shell);
          TreeItem root = tree.getItem(0);
          click(toolOf(shell, "XmlJoin.Document.ExpandAll.Tooltip"));
          ToolItem collapseAll = toolOf(shell, "XmlJoin.Document.CollapseAll.Tooltip");

          select(tree, ROOT, FIRST_HEADER);
          click(collapseAll);
          TreeItem first = root.getItem(0);
          assertFalse(first.getExpanded(), "the selected order is closed");
          assertFalse(first.getItem(0).getExpanded(), "and so are its lines");
          assertEquals(first, tree.getSelection()[0], "it stays selected");
          assertTrue(root.getExpanded());
          assertTrue(root.getItem(1).getItem(0).getExpanded(), "the other order stays open");

          tree.deselectAll();
          click(collapseAll);
          assertFalse(root.getExpanded());
          assertFalse(root.getItem(1).getExpanded());
          assertFalse(root.getItem(1).getItem(0).getExpanded());
        });
  }

  /** In a large document, Expand all opens level by level until enough nodes show. */
  @Test
  void expandAllOpensALargeDocumentLevelByLevel() throws Exception {
    int orders = 100;
    int linesPerOrder = 100;
    StringBuilder xml = new StringBuilder("<orders>");
    for (int i = 0; i < orders; i++) {
      xml.append("<order>").append("<line><sku/></line>".repeat(linesPerOrder)).append("</order>");
    }
    Document doc = parse(xml.append("</orders>").toString());
    withPane(
        () -> {
          Shell shell = sceneShell();
          pane.get().setDocument(doc, "target");
          click(toolOf(shell, "XmlJoin.Document.ExpandAll.Tooltip"));

          TreeItem root = treeOf(shell).getItem(0);
          int openOrders = 0;
          int shown = orders;
          for (TreeItem order : root.getItems()) {
            if (order.getExpanded()) {
              openOrders++;
              shown += order.getItemCount();
              for (TreeItem line : order.getItems()) {
                assertFalse(line.getExpanded(), "the lines open after every order");
              }
            }
          }
          assertEquals((TargetDocumentPane.MAX_EXPANDED - orders) / linesPerOrder, openOrders);
          assertEquals(TargetDocumentPane.MAX_EXPANDED, shown);
        });
  }

  /**
   * Under one node the transform takes the first node the statement finds, so that is the node
   * highlighted, also after a click on another node the statement finds.
   */
  @Test
  void underOneNodeTheFirstNodeTheStatementFindsIsHighlighted() throws Exception {
    Document doc = parse(ORDERS);
    withPane(
        () -> {
          Shell shell = sceneShell();
          pane.get().setDocument(doc, "target");
          Tree tree = treeOf(shell);
          select(tree, ROOT, SECOND_HEADER, LINES);

          pane.get().show("/OrderList/OrderHeader/OrderLines");

          assertSelected(tree, FIRST_HEADER, LINES);
          assertEquals(FIRST_HEADER, pane.get().check().parent());
        });
  }

  /** A node past the children the tree lists is found, just not shown: that is not "no node". */
  @Test
  void aNodeTheTreeDoesNotListIsNotReportedAsMissing() throws Exception {
    StringBuilder xml = new StringBuilder("<rows>");
    for (int i = 0; i < TargetDocumentPane.MAX_CHILDREN + 50; i++) {
      xml.append("<row/>");
    }
    Document doc = parse(xml.append("</rows>").toString());
    withPane(
        () -> {
          Shell shell = sceneShell();
          pane.get().setDocument(doc, "target");

          pane.get().show("/rows/row[" + (TargetDocumentPane.MAX_CHILDREN + 10) + "]");
          assertEquals(0, treeOf(shell).getSelectionCount());
          assertEquals(
              new Check(Outcome.FOUND_NOT_LISTED, "<row/>", "<rows>", null, 1, null),
              pane.get().check());

          pane.get().show("/rows/nothing");
          assertEquals(Outcome.NOT_FOUND, pane.get().check().outcome());
        });
  }

  /**
   * A node and its parent can have a key with the same name. A key chosen on the parent stays
   * chosen when the dialog shows the statement again, and the next click uses it too. In a typed
   * statement the key is the one right after the name of its node.
   */
  @Test
  void aKeyChosenOnTheParentStaysChosen() throws Exception {
    Document doc =
        parse("<orders><order id=\"2\"><line id=\"1\"/><line id=\"2\"/></order></orders>");
    String order = "<order id=\"2\">";
    withPane(
        () -> {
          Shell shell = sceneShell();
          TargetDocumentPane documentPane = pane.get();
          documentPane.setByKey(true);
          documentPane.setDocument(doc, "target");
          Tree tree = treeOf(shell);
          CCombo key = keyOf(shell);

          select(tree, "<orders>", order, "<line id=\"2\"/>");
          assertEquals("@id of line = 2", key.getText(), "the nearest key first");
          key.select(List.of(key.getItems()).indexOf("@id of order = 2"));
          key.notifyListeners(SWT.Selection, new Event());
          String statement = written.get(written.size() - 1);
          assertEquals("/orders/order[@id='?']/line[2]", statement);

          // The dialog shows every statement written into the transform
          documentPane.show(statement);
          assertEquals("@id of order = 2", key.getText());

          select(tree, "<orders>", order, "<line id=\"1\"/>");
          assertEquals("/orders/order[@id='?']/line[1]", written.get(written.size() - 1));

          documentPane.show("//order[@id='?']/line[1]");
          assertEquals("@id of order = 2", key.getText());
          documentPane.show("//line[@id='?']");
          assertEquals("@id of line = 1", key.getText());
        });
  }

  /**
   * GTK selects the first row when the tree gets the focus with nothing selected. That is not a
   * click: it writes no statement. A click after it does.
   */
  @Test
  void theTreeGettingTheFocusWritesNothing() throws Exception {
    Document doc = parse(ORDERS);
    withPane(
        () -> {
          pane.get().setDocument(doc, "target");
          pane.get().show("//nothing");
          Tree tree = treeOf(sceneShell());
          assertEquals(0, tree.getSelectionCount());

          tree.setFocus();
          while (display.readAndDispatch()) {
            // the focus and what follows it
          }

          assertTrue(written.isEmpty(), "the focus writes no statement");
          assertEquals(0, tree.getSelectionCount(), "and selects nothing");
          select(tree, ROOT, SECOND_HEADER, LINES);
          assertEquals(List.of("/OrderList/OrderHeader[2]/OrderLines"), written);
        });
  }

  /**
   * Under the node of their key, the key chosen for a click is one that tells the node apart. A key
   * other nodes share is reported: the transform puts the elements under the first of them.
   */
  @Test
  void aKeyOtherNodesShareIsNotChosenAndIsReported() throws Exception {
    Document doc =
        parse(
            "<orders><order currency=\"EUR\" no=\"1\"><lines/></order>"
                + "<order currency=\"EUR\" no=\"2\"><lines/></order></orders>");
    String second = "<order currency=\"EUR\" no=\"2\">";
    withPane(
        () -> {
          Shell shell = sceneShell();
          TargetDocumentPane documentPane = pane.get();
          documentPane.setByKey(true);
          documentPane.setKeyNames(() -> List.of("order_no"));
          documentPane.setDocument(doc, "target");
          Tree tree = treeOf(shell);

          select(tree, "<orders>", second, "<lines/>");
          CCombo key = keyOf(shell);
          assertEquals("@no of order = 2", key.getText(), "not the currency the orders share");
          assertEquals("/orders/order[@no='?']/lines", written.get(written.size() - 1));
          documentPane.show(written.get(written.size() - 1));
          assertEquals(Outcome.FOUND, documentPane.check().outcome());

          key.select(List.of(key.getItems()).indexOf("@currency of order = EUR"));
          key.notifyListeners(SWT.Selection, new Event());
          documentPane.show(written.get(written.size() - 1));
          assertEquals(
              new Check(
                  Outcome.KEY_NOT_UNIQUE,
                  "<lines/>",
                  "<order currency=\"EUR\" no=\"1\">",
                  "EUR",
                  2,
                  null),
              documentPane.check());
          assertSelected(tree, second, "<lines/>");
        });
  }

  /**
   * The check only follows ? written as ='?'. A statement that uses it otherwise is not checked,
   * rather than reported as wrong; one that is not valid with a key filled in is. Under one node, ?
   * is not replaced, and alone it is not a valid statement.
   */
  @Test
  void aStatementTheCheckCannotFollowIsNotReportedAsWrong() throws Exception {
    Document doc = parse(ORDERS);
    withPane(
        () -> {
          TargetDocumentPane documentPane = pane.get();
          documentPane.setByKey(true);
          documentPane.setDocument(doc, "target");

          for (String statement :
              List.of(
                  "/OrderList/OrderHeader[@orderNumber=?]/OrderLines",
                  "/OrderList/OrderHeader[contains(@orderNumber, '?')]/OrderLines",
                  "/OrderList/OrderHeader[@orderNumber!='?']/OrderLines")) {
            documentPane.show(statement);
            assertEquals(Check.of(Outcome.NOT_FOLLOWED), documentPane.check(), statement);
          }
          documentPane.show("/OrderList/OrderHeader[@orderNumber='?'/OrderLines");
          assertEquals(Outcome.INVALID, documentPane.check().outcome());

          documentPane.setByKey(false);
          documentPane.show("?");
          assertEquals(Outcome.INVALID, documentPane.check().outcome());
        });
  }

  /** A large document lists the first children of a node and counts the rest. */
  @Test
  void aLargeDocumentListsTheFirstChildren() throws Exception {
    StringBuilder xml = new StringBuilder("<rows>");
    for (int i = 0; i < TargetDocumentPane.MAX_CHILDREN + 50; i++) {
      xml.append("<row/>");
    }
    Document doc = parse(xml.append("</rows>").toString());
    withPane(
        () -> {
          pane.get().setDocument(doc, "target");
          TreeItem[] items = treeOf(sceneShell()).getItem(0).getItems();
          assertEquals(TargetDocumentPane.MAX_CHILDREN + 1, items.length);
          assertEquals(message("XmlJoin.Document.More", 50), items[items.length - 1].getText());
        });
  }

  // ---------------------------------------------------------------------------------------------

  private void withPane(Runnable checks) {
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          TargetDocumentPane documentPane = new TargetDocumentPane(shell, loads::incrementAndGet);
          documentPane.setPickListener(
              (xpath, key) -> {
                written.add(xpath);
                writtenKey.set(key);
              });
          pane.set(documentPane);
          shell.setSize(600, 500);
        },
        bot -> display.syncExec(checks));
  }

  private static Shell sceneShell() {
    for (Shell shell : display.getShells()) {
      if (!shell.isDisposed() && "Hop SWTBot test".equals(shell.getText())) {
        return shell;
      }
    }
    throw new AssertionError("the scene is not open");
  }

  private static void assertSelected(Tree tree, String parentText, String text) {
    TreeItem[] selection = tree.getSelection();
    assertEquals(1, selection.length);
    assertEquals(text, selection[0].getText());
    assertEquals(parentText, selection[0].getParentItem().getText());
  }

  /**
   * A message as the pane shows it. BaseMessages shows a message it can't find or format as !key!,
   * which an expected text built the same way would match.
   */
  static String message(String key, Object... arguments) {
    String text = BaseMessages.getString(PKG, key, arguments);
    assertFalse(text.startsWith("!") && text.endsWith("!"), "no message " + key + ": " + text);
    return text;
  }

  /**
   * Opens the items along a path of texts, the way a click on their arrows does, and clicks the
   * last one.
   */
  static void select(Tree tree, String... path) {
    TreeItem item = null;
    for (String text : path) {
      TreeItem[] items = item == null ? tree.getItems() : item.getItems();
      TreeItem next = null;
      for (TreeItem candidate : items) {
        if (text.equals(candidate.getText())) {
          next = candidate;
        }
      }
      if (next == null) {
        throw new AssertionError("no item '" + text + "' in " + texts(items));
      }
      item = next;
      Event expand = new Event();
      expand.item = item;
      tree.notifyListeners(SWT.Expand, expand);
      item.setExpanded(true);
    }
    // As a click arrives: the mouse button goes down, then the row is selected
    tree.notifyListeners(SWT.MouseDown, new Event());
    tree.setSelection(item);
    tree.notifyListeners(SWT.Selection, new Event());
  }

  static List<String> texts(TreeItem[] items) {
    List<String> texts = new ArrayList<>();
    for (TreeItem item : items) {
      texts.add(item.getText());
    }
    return texts;
  }

  static Tree treeOf(Composite parent) {
    return (Tree) controlsOf(parent, Tree.class).get(0);
  }

  static CCombo keyOf(Composite parent) {
    return (CCombo) controlsOf(parent, CCombo.class).get(0);
  }

  static List<String> labelTexts(Composite parent) {
    return controlsOf(parent, Label.class).stream().map(l -> ((Label) l).getText()).toList();
  }

  /** The tool with the tooltip of the message key. */
  private static ToolItem toolOf(Composite parent, String key) {
    String toolTip = message(key, TargetDocumentPane.MAX_EXPANDED);
    for (Control control : controlsOf(parent, ToolBar.class)) {
      for (ToolItem item : ((ToolBar) control).getItems()) {
        if (toolTip.equals(item.getToolTipText())) {
          return item;
        }
      }
    }
    throw new AssertionError("no tool '" + toolTip + "'");
  }

  private static void click(ToolItem item) {
    item.notifyListeners(SWT.Selection, new Event());
  }

  private static Button buttonOf(Composite parent, String text) {
    return (Button)
        controlsOf(parent, Button.class).stream()
            .filter(b -> text.equals(((Button) b).getText().replace("&", "")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no button '" + text + "'"));
  }

  private static List<Control> controlsOf(Composite parent, Class<? extends Control> type) {
    List<Control> all = new ArrayList<>();
    collect(parent, all);
    return all.stream().filter(type::isInstance).toList();
  }

  private static void collect(Control parent, List<Control> controls) {
    controls.add(parent);
    if (parent instanceof Composite composite) {
      for (Control child : composite.getChildren()) {
        collect(child, controls);
      }
    }
  }

  static Document parse(String xml) throws Exception {
    return XmlParserFactoryProducer.createSecureDocBuilderFactory()
        .newDocumentBuilder()
        .parse(new InputSource(new StringReader(xml)));
  }
}
