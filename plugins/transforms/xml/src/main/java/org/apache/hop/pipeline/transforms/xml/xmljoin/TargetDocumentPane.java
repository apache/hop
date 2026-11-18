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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.xml.xmljoin.TargetXPathBuilder.KeyCandidate;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * The first target document as a tree, beside the options of the XML join. Clicking a node writes
 * the XPath statement that leads the transform to it; when elements go under the node of their key,
 * the key of that node is chosen under the tree. The node the statement finds is highlighted, and
 * {@link #check()} says what it finds, so a statement typed by hand can be checked against the
 * document.
 */
final class TargetDocumentPane {
  private static final Class<?> PKG = XmlJoinMeta.class;

  /** Children listed under one node. The rest are counted: a document can be large. */
  static final int MAX_CHILDREN = 200;

  /** Nodes one Expand all shows at most: drawing the whole tree of a large document takes long. */
  static final int MAX_EXPANDED = 5000;

  private static final int MAX_VALUE_LENGTH = 40;

  /** Whether and how the XPath statement finds a node in the document. */
  enum Outcome {
    /** No document is loaded. */
    NO_DOCUMENT,
    /** There is no statement to check, or it comes from the key field. */
    NOT_CHECKED,
    /**
     * Under the node of their key: the statement uses ? other than in ='?', the check can't follow.
     */
    NOT_FOLLOWED,
    /** The statement is not valid XPath. */
    INVALID,
    /** The statement finds no node. */
    NOT_FOUND,
    /**
     * Under the node of their key: with the key of the node filled in, the statement finds another
     * node first, the one the transform takes. {@link Check#node()} is that node.
     */
    KEY_NOT_UNIQUE,
    /** Under the node of their key: with the key of the node filled in, it finds no node. */
    KEY_FINDS_NO_NODE,
    /** The statement finds a node, highlighted in the tree. */
    FOUND,
    /** The statement finds a node the tree does not list. */
    FOUND_NOT_LISTED
  }

  /**
   * What the XPath statement finds in the document.
   *
   * @param outcome whether and how it finds a node
   * @param node the node it finds, as the tree shows it
   * @param parent the parent of that node as the tree shows it, null for the root
   * @param keyValue under the node of their key: the key of that node, null when the statement uses
   *     none of its keys
   * @param keyNodes under the node of their key: the nodes with a key the statement finds
   * @param error why the statement is not valid
   */
  record Check(
      Outcome outcome, String node, String parent, String keyValue, int keyNodes, String error) {
    static Check of(Outcome outcome) {
      return new Check(outcome, null, null, null, 0, null);
    }
  }

  private final Group group;
  private final ToolItem wExpandAll;
  private final ToolItem wCollapseAll;
  private final Label wStatus;
  private final Tree wTree;
  private final Label wlKey;
  private final CCombo wKey;

  private Document document;
  private String transformName;
  private boolean byKey;
  private Supplier<List<String>> keyNames = List::of;
  private BiConsumer<String, KeyCandidate> pickListener = (xpath, key) -> {};

  /** The node clicked, or found by the statement. */
  private Element picked;

  /** What the statement finds, see {@link #check()}. */
  private Check check = Check.of(Outcome.NO_DOCUMENT);

  /** Set by {@link #find()}: why the statement is not valid XPath. */
  private String findError;

  /** Set by {@link #find()}: the elements the statement finds, for any key. */
  private int foundCount;

  /** Set by {@link #find()}: the statement uses ? in a way the check can't follow. */
  private boolean notFollowed;

  /** A mouse button or key went down in the tree: the next selection is someone's click. */
  private boolean userInput;

  private List<KeyCandidate> candidates = List.of();

  /** The statement in the transform, variables resolved. */
  private String xpath = "";

  /** Evaluates the statements; creating an XPath factory scans the classpath. */
  private XPath evaluator;

  /**
   * @param load loads the document, see {@link #setDocument}
   */
  TargetDocumentPane(Composite parent, Runnable load) {
    group = new Group(parent, SWT.SHADOW_ETCHED_IN);
    PropsUi.setLook(group);
    group.setText(BaseMessages.getString(PKG, "XmlJoin.Document.Group.Label"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    group.setLayout(layout);
    int margin = PropsUi.getMargin();
    double zoom = PropsUi.getInstance().getZoomFactor();

    Composite header = new Composite(group, SWT.NONE);
    PropsUi.setLook(header);
    GridLayout headerLayout = new GridLayout(3, false);
    headerLayout.marginWidth = 0;
    headerLayout.marginHeight = 0;
    headerLayout.horizontalSpacing = margin;
    header.setLayout(headerLayout);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(0, 0);
    fdHeader.right = new FormAttachment(100, 0);
    fdHeader.top = new FormAttachment(0, 0);
    header.setLayoutData(fdHeader);

    Button wLoad = new Button(header, SWT.PUSH);
    PropsUi.setLook(wLoad);
    wLoad.setText(BaseMessages.getString(PKG, "XmlJoin.Document.Load.Button"));
    wLoad.setToolTipText(BaseMessages.getString(PKG, "XmlJoin.Document.Load.Tooltip"));
    wLoad.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
    wLoad.addListener(SWT.Selection, e -> load.run());

    ToolBar wTools = new ToolBar(header, SWT.HORIZONTAL | SWT.FLAT);
    PropsUi.setLook(wTools);
    wTools.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
    wExpandAll = new ToolItem(wTools, SWT.PUSH);
    wExpandAll.setImage(GuiResource.getInstance().getImageExpandAll());
    wExpandAll.setToolTipText(
        BaseMessages.getString(PKG, "XmlJoin.Document.ExpandAll.Tooltip", MAX_EXPANDED));
    wExpandAll.addListener(SWT.Selection, e -> expandAll());
    wCollapseAll = new ToolItem(wTools, SWT.PUSH);
    wCollapseAll.setImage(GuiResource.getInstance().getImageCollapseAll());
    wCollapseAll.setToolTipText(
        BaseMessages.getString(PKG, "XmlJoin.Document.CollapseAll.Tooltip"));
    wCollapseAll.addListener(SWT.Selection, e -> collapseAll());

    wStatus = new Label(header, SWT.WRAP | SWT.LEFT);
    PropsUi.setLook(wStatus);
    GridData gdStatus = new GridData(SWT.FILL, SWT.CENTER, true, false);
    gdStatus.widthHint = (int) (250 * zoom);
    wStatus.setLayoutData(gdStatus);

    wKey = new CCombo(group, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wKey);
    wKey.setToolTipText(BaseMessages.getString(PKG, "XmlJoin.Document.Key.Tooltip"));
    wKey.addListener(SWT.Selection, e -> notifyPick());
    wlKey = new Label(group, SWT.LEFT);
    PropsUi.setLook(wlKey);
    wlKey.setText(BaseMessages.getString(PKG, "XmlJoin.Document.Key.Label"));
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(0, 0);
    fdlKey.top = new FormAttachment(wKey, 0, SWT.CENTER);
    wlKey.setLayoutData(fdlKey);
    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(wlKey, margin);
    fdKey.right = new FormAttachment(100, 0);
    fdKey.bottom = new FormAttachment(100, 0);
    wKey.setLayoutData(fdKey);

    wTree = new Tree(group, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wTree);
    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment(0, 0);
    fdTree.right = new FormAttachment(100, 0);
    fdTree.top = new FormAttachment(header, margin);
    fdTree.bottom = new FormAttachment(wKey, -margin);
    fdTree.width = (int) (350 * zoom);
    fdTree.height = (int) (250 * zoom);
    wTree.setLayoutData(fdTree);
    wTree.addListener(SWT.Expand, e -> fill((TreeItem) e.item));
    wTree.addListener(SWT.MouseDown, e -> userInput = true);
    wTree.addListener(SWT.KeyDown, e -> userInput = true);
    wTree.addListener(SWT.Selection, e -> clicked());

    setByKey(false);
    enableTools();
  }

  void setPickListener(BiConsumer<String, KeyCandidate> pickListener) {
    this.pickListener = pickListener;
  }

  /** The names to choose a key by, in order: the key field, or else the source fields. */
  void setKeyNames(Supplier<List<String>> keyNames) {
    this.keyNames = keyNames;
  }

  /** Shows a document, opened at the node the statement finds. */
  void setDocument(Document document, String transformName) {
    this.document = document;
    this.transformName = transformName;
    wTree.removeAll();
    picked = null;
    if (document != null) {
      TreeItem root = addItem(null, document.getDocumentElement());
      fill(root);
      root.setExpanded(true);
    }
    enableTools();
    show(xpath);
  }

  /** Under one node, or under the node of each key: the key row is only used for the latter. */
  void setByKey(boolean byKey) {
    this.byKey = byKey;
    wlKey.setEnabled(byKey);
    wKey.setEnabled(byKey);
    chooseKey(null);
    updateStatus();
  }

  /** Highlights the node the statement finds, and works out what it finds, see {@link #check()}. */
  void show(String statement) {
    xpath = Const.NVL(statement, "").trim();
    if (document == null) {
      check = Check.of(Outcome.NO_DOCUMENT);
    } else {
      Element found = find();
      TreeItem item = found == null ? null : itemOf(found);
      if (item == null) {
        wTree.deselectAll();
        picked = null;
      } else {
        wTree.setSelection(item);
        wTree.showItem(item);
        picked = found;
      }
      KeyCandidate key = keyInStatement(found);
      chooseKey(key);
      check = checkOf(found, item != null, key);
    }
    updateStatus();
  }

  /** What the statement finds in the document, as of the last {@link #show}. */
  Check check() {
    return check;
  }

  private Check checkOf(Element found, boolean listed, KeyCandidate key) {
    if (xpath.isEmpty() || byKey && TargetXPathBuilder.KEY_PLACEHOLDER.equals(xpath)) {
      return Check.of(Outcome.NOT_CHECKED);
    }
    if (findError != null) {
      return new Check(Outcome.INVALID, null, null, null, 0, findError);
    }
    if (notFollowed) {
      return Check.of(Outcome.NOT_FOLLOWED);
    }
    if (found == null) {
      return Check.of(Outcome.NOT_FOUND);
    }
    if (byKey && key != null) {
      // What the transform does with the key of this node: it takes the first node it finds
      Node taken = nodeOf(TargetXPathBuilder.forKeyValue(xpath, key.value()));
      if (!(taken instanceof Element takenElement)) {
        return new Check(
            Outcome.KEY_FINDS_NO_NODE,
            describe(found),
            parentOf(found),
            key.value(),
            foundCount,
            null);
      }
      if (takenElement != found) {
        return new Check(
            Outcome.KEY_NOT_UNIQUE,
            describe(takenElement),
            parentOf(takenElement),
            key.value(),
            foundCount,
            null);
      }
    }
    return new Check(
        listed ? Outcome.FOUND : Outcome.FOUND_NOT_LISTED,
        describe(found),
        parentOf(found),
        byKey && key != null ? key.value() : null,
        byKey ? foundCount : 1,
        null);
  }

  private static String parentOf(Element element) {
    return element.getParentNode() instanceof Element parent ? describe(parent) : null;
  }

  /** The node a statement finds the way the transform evaluates it: the first one. */
  private Node nodeOf(String statement) {
    try {
      return (Node) evaluator().evaluate(statement, document, XPathConstants.NODE);
    } catch (XPathExpressionException e) {
      return null;
    }
  }

  private XPath evaluator() {
    if (evaluator == null) {
      evaluator = XPathFactory.newInstance().newXPath();
    }
    return evaluator;
  }

  /** Writes the statement for the node again, after the join mode changed. */
  void pickAgain() {
    notifyPick();
  }

  /**
   * The key names changed, the key field for instance. When the node has a key with one of the new
   * names, that key is chosen and the statement for it written.
   */
  void keyNamesChanged() {
    if (!byKey || picked == null || candidates.isEmpty()) {
      return;
    }
    KeyCandidate named = namedKey();
    if (named != null && named != selectedKey()) {
      wKey.select(candidates.indexOf(named));
      notifyPick();
    }
  }

  /** The first key of the node with one of the key names, in their order. */
  private KeyCandidate namedKey() {
    for (String name : keyNames.get()) {
      KeyCandidate candidate = TargetXPathBuilder.findByName(candidates, name);
      if (candidate != null) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Opens every node under the selected node, or under the root when no node is selected. It opens
   * level by level and stops once {@link #MAX_EXPANDED} nodes show, so a large document opens its
   * upper levels.
   */
  private void expandAll() {
    TreeItem start = selectedOrRoot();
    if (start == null) {
      return;
    }
    wTree.setRedraw(false);
    try {
      Deque<TreeItem> next = new ArrayDeque<>(List.of(start));
      int shown = 0;
      while (!next.isEmpty() && shown < MAX_EXPANDED) {
        TreeItem item = next.poll();
        fill(item);
        if (item.getItemCount() == 0) {
          continue;
        }
        item.setExpanded(true);
        for (TreeItem child : item.getItems()) {
          shown++;
          if (child.getData() instanceof Element) {
            next.add(child);
          }
        }
      }
    } finally {
      wTree.setRedraw(true);
    }
  }

  /** Closes the selected node and every node under it, or the whole document. */
  private void collapseAll() {
    TreeItem start = selectedOrRoot();
    if (start == null) {
      return;
    }
    List<TreeItem> open = new ArrayList<>();
    Deque<TreeItem> next = new ArrayDeque<>(List.of(start));
    while (!next.isEmpty()) {
      TreeItem item = next.pop();
      if (item.getItemCount() > 0) {
        open.add(item);
        for (TreeItem child : item.getItems()) {
          next.push(child);
        }
      }
    }
    wTree.setRedraw(false);
    try {
      // The deepest nodes first, so every node is closed when its parent closes
      for (int i = open.size() - 1; i >= 0; i--) {
        open.get(i).setExpanded(false);
      }
    } finally {
      wTree.setRedraw(true);
    }
  }

  /** The selected node, or the root when no node is selected. */
  private TreeItem selectedOrRoot() {
    TreeItem[] selection = wTree.getSelection();
    if (selection.length == 1 && selection[0].getData() instanceof Element) {
      return selection[0];
    }
    return wTree.getItemCount() == 0 ? null : wTree.getItem(0);
  }

  private void enableTools() {
    wExpandAll.setEnabled(document != null);
    wCollapseAll.setEnabled(document != null);
  }

  private void clicked() {
    TreeItem[] selection = wTree.getSelection();
    boolean byInput = userInput;
    userInput = false;
    if (!byInput
        && picked == null
        && selection.length == 1
        && wTree.getItemCount() > 0
        && selection[0] == wTree.getItem(0)) {
      // Nobody clicked: GTK selects the first row when the tree gets the focus with nothing
      // selected. That must not write over the statement.
      wTree.deselectAll();
      return;
    }
    picked =
        selection.length == 1 && selection[0].getData() instanceof Element element ? element : null;
    chooseKey(selectedKey());
    notifyPick();
    updateStatus();
  }

  private void notifyPick() {
    if (picked == null) {
      return;
    }
    KeyCandidate key = null;
    String statement;
    if (byKey) {
      key = selectedKey();
      if (key == null) {
        updateStatus();
        return;
      }
      statement = TargetXPathBuilder.xpath(picked, key);
    } else {
      statement = TargetXPathBuilder.xpath(picked);
    }
    xpath = statement;
    pickListener.accept(statement, key);
    updateStatus();
  }

  /**
   * Fills the key row for the picked node. The key to select is the preferred one, or one with the
   * same name as before, or one named like the key names, or the first.
   */
  private void chooseKey(KeyCandidate preferred) {
    KeyCandidate previous = selectedKey();
    candidates = byKey && picked != null ? TargetXPathBuilder.keyCandidates(picked) : List.of();
    wKey.setItems(candidates.stream().map(TargetDocumentPane::describe).toArray(String[]::new));
    KeyCandidate choice = sameKey(preferred);
    if (choice == null) {
      choice = sameKey(previous);
    }
    if (choice == null && !candidates.isEmpty()) {
      choice = namedKey();
    }
    if (choice == null && !candidates.isEmpty()) {
      // A key that tells this node apart, so the transform finds it with its value
      choice =
          candidates.stream().filter(this::leadsToPicked).findFirst().orElse(candidates.get(0));
    }
    if (choice != null) {
      wKey.select(candidates.indexOf(choice));
    }
  }

  /** Whether the statement for the picked node with this key, its value filled in, finds it. */
  private boolean leadsToPicked(KeyCandidate key) {
    String statement = TargetXPathBuilder.xpath(picked, key);
    return nodeOf(TargetXPathBuilder.forKeyValue(statement, key.value())) == picked;
  }

  private KeyCandidate selectedKey() {
    int index = wKey.getSelectionIndex();
    return index >= 0 && index < candidates.size() ? candidates.get(index) : null;
  }

  /** The candidate with the same name on a node with the same name. */
  private KeyCandidate sameKey(KeyCandidate key) {
    if (key == null) {
      return null;
    }
    for (KeyCandidate candidate : candidates) {
      if (candidate.name().equals(key.name())
          && candidate.holder().getNodeName().equals(key.holder().getNodeName())) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * The key of a node the statement uses, if any: the key a click wrote the statement for, or else
   * the key whose predicate follows the name of its node, as in /order[@id='?']. The predicate
   * alone is not enough: a node and its parent can have a key of the same name.
   */
  private KeyCandidate keyInStatement(Element node) {
    if (!byKey || node == null) {
      return null;
    }
    List<KeyCandidate> keys = TargetXPathBuilder.keyCandidates(node);
    for (KeyCandidate candidate : keys) {
      if (TargetXPathBuilder.xpath(node, candidate).equals(xpath)) {
        return candidate;
      }
    }
    for (KeyCandidate candidate : keys) {
      if (xpath.contains(candidate.stepWithPredicate())) {
        return candidate;
      }
    }
    for (KeyCandidate candidate : keys) {
      if (xpath.contains(candidate.predicate())) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * The node the statement finds. Under one node that is the first node in the document, as the
   * transform takes it. Under the node of their key, '?' stands for any key, so the picked node
   * stays highlighted when the statement finds it among the others.
   */
  private Element find() {
    findError = null;
    foundCount = 0;
    notFollowed = false;
    if (xpath.isEmpty() || byKey && TargetXPathBuilder.KEY_PLACEHOLDER.equals(xpath)) {
      return null;
    }
    String statement = xpath;
    if (byKey) {
      // Valid XPath once the transform fills in a key?
      try {
        evaluator()
            .evaluate(TargetXPathBuilder.forKeyValue(xpath, "0"), document, XPathConstants.NODESET);
      } catch (XPathExpressionException e) {
        findError = messageOf(e);
        return null;
      }
      // Only = '?': not the = of !=, <= or >=
      statement =
          xpath
              .replaceAll("(?<![!<>])\\s*=\\s*'\\?'", "")
              .replaceAll("(?<![!<>])\\s*=\\s*\"\\?\"", "");
      if (statement.contains(TargetXPathBuilder.KEY_PLACEHOLDER)) {
        notFollowed = true;
        return null;
      }
    }
    NodeList nodes;
    try {
      nodes = (NodeList) evaluator().evaluate(statement, document, XPathConstants.NODESET);
    } catch (XPathExpressionException e) {
      if (byKey) {
        // Valid with a key filled in: taking out ='?' is what broke it
        notFollowed = true;
      } else {
        findError = messageOf(e);
      }
      return null;
    }
    Element first = null;
    boolean pickedFound = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      if (nodes.item(i) instanceof Element element) {
        foundCount++;
        pickedFound |= byKey && element == picked;
        if (first == null) {
          first = element;
        }
      }
    }
    return pickedFound ? picked : first;
  }

  /** The reason an XPath statement is not valid, without the exception classes around it. */
  private static String messageOf(XPathExpressionException e) {
    Throwable cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return Const.NVL(cause.getMessage(), Const.NVL(e.getMessage(), "")).trim();
  }

  private void updateStatus() {
    String status;
    if (document == null) {
      status = BaseMessages.getString(PKG, "XmlJoin.Document.Empty");
    } else if (byKey && picked != null && candidates.isEmpty()) {
      status = BaseMessages.getString(PKG, "XmlJoin.Document.NoKey");
    } else {
      // What the statement finds is said under the statement, where it is typed
      status =
          BaseMessages.getString(
              PKG,
              byKey ? "XmlJoin.Document.Click.ByKey" : "XmlJoin.Document.Click",
              transformName);
    }
    wStatus.setText(status);
    group.layout(true, true);
  }

  private TreeItem addItem(TreeItem parentItem, Element element) {
    TreeItem item =
        parentItem == null ? new TreeItem(wTree, SWT.NONE) : new TreeItem(parentItem, SWT.NONE);
    item.setText(describe(element));
    item.setData(element);
    if (firstChildElement(element) != null) {
      // A placeholder until the node is opened
      new TreeItem(item, SWT.NONE);
    }
    return item;
  }

  /** Lists the child elements of a node the first time it is opened. */
  private void fill(TreeItem item) {
    if (item == null || !(item.getData() instanceof Element element) || !hasPlaceholder(item)) {
      return;
    }
    item.getItem(0).dispose();
    int listed = 0;
    int skipped = 0;
    for (Node child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element childElement) {
        if (listed < MAX_CHILDREN) {
          addItem(item, childElement);
          listed++;
        } else {
          skipped++;
        }
      }
    }
    if (skipped > 0) {
      new TreeItem(item, SWT.NONE)
          .setText(BaseMessages.getString(PKG, "XmlJoin.Document.More", skipped));
    }
  }

  private static boolean hasPlaceholder(TreeItem item) {
    return item.getItemCount() == 1
        && item.getItem(0).getData() == null
        && item.getItem(0).getText().isEmpty();
  }

  /** The tree item of an element, opening its parents. Null when it is not listed. */
  private TreeItem itemOf(Element element) {
    if (element == document.getDocumentElement()) {
      return wTree.getItemCount() == 0 ? null : wTree.getItem(0);
    }
    if (!(element.getParentNode() instanceof Element parentElement)) {
      return null;
    }
    TreeItem parentItem = itemOf(parentElement);
    if (parentItem == null) {
      return null;
    }
    fill(parentItem);
    parentItem.setExpanded(true);
    for (TreeItem child : parentItem.getItems()) {
      if (child.getData() == element) {
        return child;
      }
    }
    return null;
  }

  /** An element as a start tag with its attributes, and its text when that is all it holds. */
  private static String describe(Element element) {
    StringBuilder text = new StringBuilder("<").append(element.getNodeName());
    NamedNodeMap attributes = element.getAttributes();
    for (int i = 0; i < attributes.getLength(); i++) {
      Node attribute = attributes.item(i);
      text.append(' ')
          .append(attribute.getNodeName())
          .append("=\"")
          .append(shorten(attribute.getNodeValue()))
          .append('"');
    }
    if (TargetXPathBuilder.holdsOnlyText(element)) {
      text.append('>')
          .append(shorten(element.getTextContent()))
          .append("</")
          .append(element.getNodeName())
          .append('>');
    } else if (firstChildElement(element) != null) {
      text.append('>');
    } else {
      text.append("/>");
    }
    return text.toString();
  }

  /** A key as the attribute or element with its value, and the node that holds it. */
  private static String describe(KeyCandidate candidate) {
    String name = candidate.key() instanceof Element ? candidate.name() : "@" + candidate.name();
    return BaseMessages.getString(
        PKG,
        "XmlJoin.Document.KeyItem",
        name,
        shorten(candidate.value()),
        candidate.holder().getNodeName());
  }

  private static String shorten(String value) {
    String oneLine = value.strip().replaceAll("\\s+", " ");
    return oneLine.length() <= MAX_VALUE_LENGTH
        ? oneLine
        : oneLine.substring(0, MAX_VALUE_LENGTH) + "...";
  }

  private static Element firstChildElement(Element element) {
    for (Node child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element childElement) {
        return childElement;
      }
    }
    return null;
  }
}
