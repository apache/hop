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

package org.apache.hop.pipeline.transforms.fake;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import net.datafaker.Faker;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.fake.FakerCatalog.Generator;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/**
 * A searchable browser for the {@link FakerCatalog}. The user filters thousands of DataFaker
 * generators with a live search box, picks one from a category tree, fills in any parameters it
 * needs and sees a sample value before confirming.
 */
public class FakerBrowserDialog extends Dialog {
  private static final Class<?> PKG = FakerBrowserDialog.class;

  /** Marker stored on the throwaway child that gives a collapsed category its expand arrow. */
  private static final Object PLACEHOLDER = new Object();

  private final IVariables variables;
  private final Faker faker;
  private final FakeField current;
  private final Consumer<FakeField> onAddField;

  private Shell shell;
  private Text wSearch;
  private Text wName;
  private Tree wTree;
  private Group gParams;
  private ScrolledComposite scParams;
  private Composite paramsContent;
  private Label wPreview;
  private Label wStatus;

  private final List<Generator> allGenerators;

  /** The featured subset of {@link #allGenerators} - what the tree shows until "Show all" is on. */
  private final List<Generator> commonGenerators;

  /** When {@code false} the tree lists only {@link #commonGenerators}; the toggle flips it. */
  private boolean showAll;

  /** The generators surviving the current search, grouped by (and ordered by) category label. */
  private Map<String, List<Generator>> shownByCategory = new LinkedHashMap<>();

  private Generator selected;
  private final List<Control> parameterControls = new ArrayList<>();
  private int addedCount;

  private FakeField result;

  /**
   * @param current the field currently configured for the originating grid row (may be empty)
   * @param onAddField callback that appends a new field to the main dialog's grid; invoked by the
   *     "Add field" button so several fields can be added without closing this dialog
   */
  public FakerBrowserDialog(
      Shell parent,
      IVariables variables,
      Faker faker,
      FakeField current,
      Consumer<FakeField> onAddField) {
    super(parent, SWT.NONE);
    this.variables = variables;
    this.faker = faker;
    this.current = current;
    this.onAddField = onAddField;
    this.allGenerators = FakerCatalog.getGenerators();
    this.commonGenerators =
        allGenerators.stream().filter(g -> FakerCatalog.isFeaturedCategory(g.category())).toList();
  }

  /** The generator list the tree currently draws from: featured-only, or everything. */
  private List<Generator> activeGenerators() {
    return showAll ? allGenerators : commonGenerators;
  }

  /** True when the pre-configured generator exists but is not one of the featured providers. */
  private boolean currentIsHidden() {
    if (current == null || Utils.isEmpty(current.getType()) || Utils.isEmpty(current.getTopic())) {
      return false;
    }
    String accessor = FakerCatalog.resolveGenerator(current.getType(), current.getTopic())[0];
    return !FakerCatalog.isFeaturedCategory(accessor);
  }

  /**
   * Open the dialog modally.
   *
   * @return the chosen generator as a {@link FakeField}, or {@code null} when cancelled
   */
  public FakeField open() {
    Shell parent = getParent();
    shell =
        new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX | SWT.APPLICATION_MODAL);
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "FakerBrowserDialog.DialogTitle"));
    // Inset the whole dialog from the shell edges the same way the standard Hop dialogs do
    // (BaseTransformDialog), so the left/right/top/bottom margins line up with every other dialog.
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    // A shared, right-aligned label column wide enough for the widest label, so the search and
    // field-name inputs line up underneath each other instead of colliding with their labels.
    String searchLabel = BaseMessages.getString(PKG, "FakerBrowserDialog.Search.Label");
    String nameLabel = BaseMessages.getString(PKG, "FakerBrowserDialog.Name.Label");
    int labelWidth;
    GC gc = new GC(shell);
    try {
      labelWidth = Math.max(gc.textExtent(searchLabel).x, gc.textExtent(nameLabel).x);
    } finally {
      gc.dispose();
    }
    int fieldLeft = labelWidth + 2 * margin;

    // Bottom buttons
    Button wAdd = new Button(shell, SWT.PUSH);
    wAdd.setText(BaseMessages.getString(PKG, "FakerBrowserDialog.AddField.Button"));
    wAdd.setToolTipText(BaseMessages.getString(PKG, "FakerBrowserDialog.AddField.Tooltip"));
    wAdd.addListener(SWT.Selection, e -> addField());
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    Button[] buttons = new Button[] {wAdd, wOk, wCancel};
    // positionBottomButtons anchors the row to the bottom (100%); the shell's form margin provides
    // the gap to the window frame, exactly as in the standard transform dialogs.
    BaseTransformDialog.positionBottomButtons(shell, buttons, margin, null);

    // Status line - how many fields have been added during this session
    wStatus = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, 0);
    fdStatus.bottom = new FormAttachment(wOk, 0, SWT.CENTER);
    wStatus.setLayoutData(fdStatus);

    // "Show all" toggle: the tree lists only the featured providers until this is checked. If the
    // pre-configured generator isn't one of the featured providers, start with everything shown so
    // it can be focused.
    showAll = currentIsHidden();
    Button wShowAll = new Button(shell, SWT.CHECK);
    wShowAll.setText(BaseMessages.getString(PKG, "FakerBrowserDialog.ShowAll.Label"));
    wShowAll.setToolTipText(BaseMessages.getString(PKG, "FakerBrowserDialog.ShowAll.Tooltip"));
    wShowAll.setSelection(showAll);
    PropsUi.setLook(wShowAll);
    FormData fdShowAll = new FormData();
    fdShowAll.right = new FormAttachment(100, 0);
    fdShowAll.top = new FormAttachment(0, margin);
    wShowAll.setLayoutData(fdShowAll);
    wShowAll.addListener(
        SWT.Selection,
        e -> {
          showAll = wShowAll.getSelection();
          applyFilter();
        });

    // Search box
    wSearch = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.SEARCH | SWT.ICON_SEARCH);
    PropsUi.setLook(wSearch);
    wSearch.setToolTipText(BaseMessages.getString(PKG, "FakerBrowserDialog.Search.Tooltip"));
    FormData fdSearch = new FormData();
    fdSearch.left = new FormAttachment(0, fieldLeft);
    fdSearch.top = new FormAttachment(wShowAll, 0, SWT.CENTER);
    fdSearch.right = new FormAttachment(wShowAll, -margin);
    wSearch.setLayoutData(fdSearch);
    wSearch.addModifyListener(e -> applyFilter());

    Label wlSearch = new Label(shell, SWT.RIGHT);
    wlSearch.setText(searchLabel);
    PropsUi.setLook(wlSearch);
    FormData fdlSearch = new FormData();
    fdlSearch.left = new FormAttachment(0, 0);
    fdlSearch.right = new FormAttachment(0, labelWidth);
    fdlSearch.top = new FormAttachment(wSearch, 0, SWT.CENTER);
    wlSearch.setLayoutData(fdlSearch);

    // Output field name (used by "Add field" and written back on OK)
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.setToolTipText(BaseMessages.getString(PKG, "FakerBrowserDialog.Name.Tooltip"));
    if (current != null) {
      wName.setText(Const.NVL(current.getName(), ""));
    }
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(0, fieldLeft);
    fdName.top = new FormAttachment(wSearch, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(nameLabel);
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(0, labelWidth);
    fdlName.top = new FormAttachment(wName, 0, SWT.CENTER);
    wlName.setLayoutData(fdlName);

    // Parameter editors: a height-bounded panel that scrolls when a generator has more parameters
    // than fit (some have five). The Group holds a ScrolledComposite whose content is the 2-column
    // label/editor grid rebuilt in buildParameterEditors.
    gParams = new Group(shell, SWT.SHADOW_ETCHED_IN);
    gParams.setText(BaseMessages.getString(PKG, "FakerBrowserDialog.Parameters.Group"));
    PropsUi.setLook(gParams);
    gParams.setLayout(new FillLayout());
    scParams = new ScrolledComposite(gParams, SWT.V_SCROLL);
    scParams.setExpandHorizontal(true);
    scParams.setExpandVertical(true);
    PropsUi.setLook(scParams);
    paramsContent = new Composite(scParams, SWT.NONE);
    paramsContent.setLayout(new GridLayout(2, false));
    PropsUi.setLook(paramsContent);
    scParams.setContent(paramsContent);

    // Preview line
    wPreview = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wPreview);
    Button wRegenerate = new Button(shell, SWT.PUSH);
    wRegenerate.setText(BaseMessages.getString(PKG, "FakerBrowserDialog.Regenerate.Button"));
    wRegenerate.addListener(SWT.Selection, e -> updatePreview());
    FormData fdRegen = new FormData();
    fdRegen.right = new FormAttachment(100, 0);
    fdRegen.bottom = new FormAttachment(wOk, -margin);
    wRegenerate.setLayoutData(fdRegen);
    FormData fdPreview = new FormData();
    fdPreview.left = new FormAttachment(0, 0);
    fdPreview.right = new FormAttachment(wRegenerate, -margin);
    fdPreview.bottom = new FormAttachment(wOk, -2 * margin);
    wPreview.setLayoutData(fdPreview);

    FormData fdParams = new FormData();
    fdParams.left = new FormAttachment(0, 0);
    fdParams.right = new FormAttachment(100, 0);
    fdParams.bottom = new FormAttachment(wPreview, -2 * margin);
    fdParams.top = new FormAttachment(wPreview, -2 * margin - 200);
    gParams.setLayoutData(fdParams);

    // Generator tree: categories expand to their functions.
    wTree = new Tree(shell, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL);
    PropsUi.setLook(wTree);
    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment(0, 0);
    fdTree.right = new FormAttachment(100, 0);
    fdTree.top = new FormAttachment(wName, margin);
    fdTree.bottom = new FormAttachment(gParams, -margin);
    wTree.setLayoutData(fdTree);
    wTree.addListener(SWT.Expand, e -> populateCategory((TreeItem) e.item));
    wTree.addListener(SWT.Selection, e -> onTreeSelection());
    wTree.addListener(
        SWT.DefaultSelection,
        e -> {
          if (selected != null) {
            ok();
          }
        });

    shell.setSize(950, 680);
    shell.setMinimumSize(640, 480);

    applyFilter();
    selectInitialGenerator();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return result;
  }

  /**
   * The label shown for a generator leaf: its friendly title plus, for parameterized generators,
   * the curated parameter names (falling back to types) in parentheses - e.g. "Number Between
   * (Minimum, Maximum)" rather than "Number Between (int, int)".
   */
  private static String treeLabel(Generator g) {
    return g.hasParameters()
        ? g.displayTitle() + " (" + g.parameterDisplaySignature() + ")"
        : g.displayTitle();
  }

  /**
   * Populate a collapsed category node's children on first expansion (replacing its placeholder).
   */
  private void populateCategory(TreeItem parent) {
    TreeItem[] children = parent.getItems();
    if (children.length != 1 || children[0].getData() != PLACEHOLDER) {
      return; // already populated (or genuinely empty)
    }
    children[0].dispose();
    addChildren(parent);
  }

  /** Add a function child for every generator in the parent category. */
  private void addChildren(TreeItem parent) {
    for (Generator g : shownByCategory.getOrDefault(parent.getData(), List.of())) {
      TreeItem child = new TreeItem(parent, SWT.NONE);
      child.setText(treeLabel(g));
      child.setData(g);
    }
  }

  /** Re-filter the generators against the search box and rebuild the category tree. */
  private void applyFilter() {
    String[] tokens = wSearch == null ? new String[0] : splitSearch(wSearch.getText());
    boolean searching = tokens.length > 0;

    shownByCategory = new LinkedHashMap<>();
    for (Generator g : activeGenerators()) {
      if (matches(g, tokens)) {
        shownByCategory.computeIfAbsent(g.categoryLabel(), k -> new ArrayList<>()).add(g);
      }
    }

    wTree.setRedraw(false);
    wTree.removeAll();
    for (String categoryLabel : shownByCategory.keySet()) {
      TreeItem parent = new TreeItem(wTree, SWT.NONE);
      parent.setText(categoryLabel);
      parent.setData(categoryLabel);
      if (searching) {
        // Reveal every match straight away when the user is searching.
        addChildren(parent);
        parent.setExpanded(true);
      } else {
        // Lazy: a placeholder gives the collapsed category its expand arrow.
        new TreeItem(parent, SWT.NONE).setData(PLACEHOLDER);
      }
    }
    wTree.setRedraw(true);

    // Keep the current selection visible if it survived the filter.
    if (selected != null) {
      selectInTree(selected);
    }
  }

  private static String[] splitSearch(String text) {
    String trimmed = text == null ? "" : text.trim().toLowerCase();
    return trimmed.isEmpty() ? new String[0] : trimmed.split("\\s+");
  }

  private static boolean matches(Generator g, String[] tokens) {
    if (tokens.length == 0) {
      return true;
    }
    String haystack =
        (g.categoryLabel()
                + ' '
                + g.category()
                + ' '
                + g.functionName()
                + ' '
                + g.displayTitle()
                + ' '
                + g.description()
                + ' '
                + g.parameterSignature())
            .toLowerCase();
    for (String token : tokens) {
      if (!haystack.contains(token)) {
        return false;
      }
    }
    return true;
  }

  private void onTreeSelection() {
    TreeItem[] items = wTree.getSelection();
    Object data = items.length == 1 ? items[0].getData() : null;
    selected = data instanceof Generator g ? g : null;
    buildParameterEditors(null);
    updatePreview();
  }

  /**
   * Locate the tree node for a generator, expanding and populating its category on the way, and
   * return it (or {@code null} when the generator is not in the current filter).
   */
  private TreeItem locateItem(Generator g) {
    for (TreeItem parent : wTree.getItems()) {
      if (g.categoryLabel().equals(parent.getData())) {
        populateCategory(parent);
        parent.setExpanded(true);
        for (TreeItem child : parent.getItems()) {
          if (child.getData() == g) {
            return child;
          }
        }
      }
    }
    return null;
  }

  /** Highlight a generator in the tree without disturbing the parameter editors. */
  private void selectInTree(Generator g) {
    TreeItem item = locateItem(g);
    if (item != null) {
      wTree.setSelection(item);
      wTree.showSelection();
    }
  }

  /** Select the generator the field was already configured with, if it still exists. */
  private void selectInitialGenerator() {
    if (current == null || Utils.isEmpty(current.getType()) || Utils.isEmpty(current.getTopic())) {
      return;
    }
    // Honor the legacy alias table so a generator DataFaker renamed/moved still highlights.
    String[] resolved = FakerCatalog.resolveGenerator(current.getType(), current.getTopic());
    Generator g = findGenerator(resolved[0], resolved[1], current.getArguments());
    if (g == null) {
      return;
    }
    TreeItem item = locateItem(g);
    if (item != null) {
      wTree.setSelection(item);
      selected = g;
      buildParameterEditors(current.getArguments());
      updatePreview();
      // The tree has no size yet (the shell isn't open), so defer scrolling the active generator
      // into view until the first layout pass, otherwise it stays scrolled to the top.
      wTree
          .getDisplay()
          .asyncExec(
              () -> {
                if (!wTree.isDisposed()) {
                  wTree.showSelection();
                }
              });
    }
  }

  /** Best match for a stored generator: an exact-signature overload if possible, else by name. */
  private Generator findGenerator(String accessor, String topic, List<FakeArgument> arguments) {
    Generator fallback = null;
    for (List<Generator> generators : shownByCategory.values()) {
      for (Generator g : generators) {
        if (g.category().equals(accessor) && g.functionName().equals(topic)) {
          if (fallback == null) {
            fallback = g;
          }
          if (signatureMatches(g, arguments)) {
            return g;
          }
        }
      }
    }
    return fallback;
  }

  private static boolean signatureMatches(Generator g, List<FakeArgument> arguments) {
    Class<?>[] types = g.parameterTypes();
    if (types.length != arguments.size()) {
      return false;
    }
    for (int i = 0; i < types.length; i++) {
      if (!FakerCatalog.parameterTypeName(types[i]).equals(arguments.get(i).getType())) {
        return false;
      }
    }
    return true;
  }

  /** Rebuild the parameter editor widgets for the selected generator. */
  private void buildParameterEditors(List<FakeArgument> presets) {
    for (Control child : paramsContent.getChildren()) {
      child.dispose();
    }
    parameterControls.clear();

    // A curated description for the generator, when one has been written, spanning both columns.
    if (selected != null && !selected.description().isEmpty()) {
      Label description = new Label(paramsContent, SWT.WRAP);
      description.setText(selected.description());
      GridData fdDescription = new GridData(SWT.FILL, SWT.TOP, true, false, 2, 1);
      fdDescription.widthHint = 820;
      description.setLayoutData(fdDescription);
      PropsUi.setLook(description);
    }

    if (selected == null || !selected.hasParameters()) {
      Label label = new Label(paramsContent, SWT.LEFT);
      label.setText(
          BaseMessages.getString(
              PKG,
              selected == null
                  ? "FakerBrowserDialog.Parameters.None"
                  : "FakerBrowserDialog.Parameters.NoneNeeded"));
      label.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
      PropsUi.setLook(label);
    } else {
      Class<?>[] parameterTypes = selected.parameterTypes();
      boolean usePresets = presets != null && presets.size() == parameterTypes.length;
      String[] defaults = FakerCatalog.defaultArgumentValues(parameterTypes);
      for (int i = 0; i < parameterTypes.length; i++) {
        Label label = new Label(paramsContent, SWT.RIGHT);
        // Curated parameter label when available, reflected type-based label otherwise.
        label.setText(selected.parameterLabel(i));
        label.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false));
        PropsUi.setLook(label);

        String value = usePresets ? presets.get(i).getValue() : defaults[i];
        Control control = createParameterControl(parameterTypes[i], value);
        control.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
        parameterControls.add(control);
      }
    }
    paramsContent.layout(true, true);
    // Resize the scrolled content so the vertical scrollbar kicks in once the editors overflow.
    scParams.setMinSize(paramsContent.computeSize(SWT.DEFAULT, SWT.DEFAULT));
  }

  private Control createParameterControl(Class<?> parameterType, String value) {
    if (parameterType.isEnum()) {
      Combo combo = new Combo(paramsContent, SWT.READ_ONLY | SWT.BORDER);
      for (Object constant : parameterType.getEnumConstants()) {
        combo.add(((Enum<?>) constant).name());
      }
      combo.setText(Const.NVL(value, ""));
      if (combo.getSelectionIndex() < 0 && combo.getItemCount() > 0) {
        combo.select(0);
      }
      combo.addModifyListener(e -> updatePreview());
      PropsUi.setLook(combo);
      return combo;
    }
    if (parameterType == boolean.class || parameterType == Boolean.class) {
      Combo combo = new Combo(paramsContent, SWT.READ_ONLY | SWT.BORDER);
      combo.setItems("true", "false");
      combo.setText("false".equalsIgnoreCase(value) ? "false" : "true");
      combo.addModifyListener(e -> updatePreview());
      PropsUi.setLook(combo);
      return combo;
    }
    TextVar text = new TextVar(variables, paramsContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    text.setText(Const.NVL(value, ""));
    text.addModifyListener(e -> updatePreview());
    PropsUi.setLook(text);
    return text;
  }

  private static String controlValue(Control control) {
    if (control instanceof TextVar textVar) {
      return textVar.getText();
    }
    if (control instanceof Combo combo) {
      return combo.getText();
    }
    return "";
  }

  /** Build a {@link FakeField} from the field name, the current selection and its parameters. */
  private FakeField buildSelectedField() {
    FakeField field = new FakeField();
    field.setName(wName.getText());
    field.setType(selected.category());
    field.setTopic(selected.functionName());
    List<FakeArgument> arguments = new ArrayList<>();
    Class<?>[] parameterTypes = selected.parameterTypes();
    for (int i = 0; i < parameterTypes.length; i++) {
      arguments.add(
          new FakeArgument(
              FakerCatalog.parameterTypeName(parameterTypes[i]),
              controlValue(parameterControls.get(i))));
    }
    field.setArguments(arguments);
    return field;
  }

  private void updatePreview() {
    if (selected == null) {
      wPreview.setText("");
      return;
    }
    try {
      Object value = FakerCatalog.bind(faker, buildSelectedField(), variables).produce();
      wPreview.setText(
          BaseMessages.getString(PKG, "FakerBrowserDialog.Preview.Label") + " " + value);
    } catch (Exception e) {
      // Some generators can only be sampled with specific parameters (a real ZIP code, a valid
      // range, a coarse enough time unit, ...). Never surface a raw error/stack trace here - just
      // tell the user the sample needs valid parameters.
      wPreview.setText(BaseMessages.getString(PKG, "FakerBrowserDialog.Preview.Unavailable"));
    }
  }

  /** Append the current selection to the main dialog's grid and keep this dialog open. */
  private void addField() {
    if (selected == null || onAddField == null) {
      return;
    }
    onAddField.accept(buildSelectedField());
    addedCount++;
    wStatus.setText(
        BaseMessages.getString(PKG, "FakerBrowserDialog.Status.Added", String.valueOf(addedCount)));
    wName.setText("");
    wName.setFocus();
  }

  private void ok() {
    if (selected == null) {
      return;
    }
    result = buildSelectedField();
    dispose();
  }

  private void cancel() {
    result = null;
    dispose();
  }

  private void dispose() {
    shell.dispose();
  }
}
