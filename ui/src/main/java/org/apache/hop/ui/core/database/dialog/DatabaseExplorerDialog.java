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

package org.apache.hop.ui.core.database.dialog;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.database.Catalog;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaInformation;
import org.apache.hop.core.database.Schema;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineProfileFactory;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.TransformFieldsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import java.util.ArrayList;
import java.util.List;

/**
 * This dialog represents an explorer type of interface on a given database connection. It shows the
 * tables defined in the visible schemas or catalogs on that connection. The interface also allows
 * you to get all kinds of information on those tables.
 *
 * @author Matt
 * @since 18-05-2003
 *     <p>
 */
public class DatabaseExplorerDialog extends Dialog {
  private static final Class<?> PKG = DatabaseExplorerDialog.class; // For Translator

  private ILogChannel log;
  private PropsUi props;
  private DatabaseMeta dbMeta;
  private IVariables variables;
  private DbCache dbcache;
  private ILoggingObject loggingObject;

  private static final String STRING_CATALOG =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Catalogs.Label");
  private static final String STRING_SCHEMAS =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Schemas.Label");
  private static final String STRING_TABLES =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Tables.Label");
  private static final String STRING_VIEWS =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Views.Label");
  private static final String STRING_SYNONYMS =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Synonyms.Label");

  private Shell parent, shell;
  private Tree wTree;
  private TreeItem tiTree;

  private Button wOk;
  private Button wRefresh;
  private Button wCancel;

  private String tableName;

  private boolean justLook;
  private String selectedSchema;
  private String selectedTable;
  private List<DatabaseMeta> databases;
  private boolean splitSchemaAndTable;
  private String schemaName;
  private Composite buttonsComposite;
  private Button bPrev;
  private Button bPrevN;
  private Button bCount;
  private Button bShow;
  private Button bDDL;
  private Button bDDL2;
  private Button bSql;
  private String activeSchemaTable;
  private Button bTruncate;
  private FormData fdExpandAll;
  private ToolItem expandAll, collapseAll;

  public DatabaseExplorerDialog(
      Shell parent,
      int style,
      IVariables variables,
      DatabaseMeta conn,
      List<DatabaseMeta> databases) {
    this(parent, style, variables, conn, databases, false, false);
  }

  public DatabaseExplorerDialog(
      Shell parent,
      int style,
      IVariables variables,
      DatabaseMeta conn,
      List<DatabaseMeta> databases,
      boolean look,
      boolean splitSchemaAndTable) {
    super(parent, style);
    this.parent = parent;
    this.dbMeta = conn;
    this.variables = variables;
    this.databases = databases;
    this.justLook = look;
    this.splitSchemaAndTable = splitSchemaAndTable;
    this.loggingObject = new LoggingObject( "Database Explorer" );

    selectedSchema = null;
    selectedTable = null;

    props = PropsUi.getInstance();
    log = new LogChannel("DBExplorer");
    dbcache = DbCache.getInstance();
  }

  public void setSelectedTable(String selectedTable) {
    this.selectedTable = selectedTable;
  }

  public boolean open() {
    tableName = null;

    if (Const.isLinux()) {
      shell =
          new Shell(
              parent, SWT.APPLICATION_MODAL | SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    } else {
      shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    }
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageConnection());

    shell.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Title", dbMeta.toString()));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);

    addButtons();
    refreshButtons(null);

    // Tree
    wTree = new Tree(shell, SWT.SINGLE | SWT.BORDER /*| (multiple?SWT.CHECK:SWT.NONE)*/);
    props.setLook(wTree);

    if (!getData()) {
      return false;
    }

    // Buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));

    wRefresh = new Button(shell, SWT.PUSH);
    wRefresh.setText(BaseMessages.getString(PKG, "System.Button.Refresh"));

    if (!justLook) {
      wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    }

    FormData fdTree = new FormData();

    int margin = 10;

    fdTree.left = new FormAttachment(0, 0); // To the right of the label
    fdTree.top = new FormAttachment(0, 0);
    fdTree.right = new FormAttachment(buttonsComposite, -margin);
    fdTree.bottom = new FormAttachment(100, -50);
    wTree.setLayoutData(fdTree);

    if (!justLook) {
      BaseTransformDialog.positionBottomButtons(
          shell, new Button[] {wOk, wCancel, wRefresh}, margin, null);

      // Add listeners
      wCancel.addListener(
          SWT.Selection,
          e -> {
            log.logBasic("SelectTableDialog", "CANCEL SelectTableDialog", null);
            dbMeta = null;
            dispose();
          });
    } else {
      BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wRefresh}, margin, null);
    }

    // Add listeners
    wOk.addListener(SWT.Selection, e -> handleOK());
    wRefresh.addListener(SWT.Selection, e -> getData());
    SelectionAdapter selAdapter =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            refreshButtons(getSchemaTable());
          }

          public void widgetDefaultSelected(SelectionEvent e) {
            openSchema(e);
          }
        };
    wTree.addSelectionListener(selAdapter);

    wTree.addMouseListener(
        new MouseAdapter() {
          public void mouseDown(MouseEvent e) {
            if (e.button == 3) // right click!
            {
              setTreeMenu();
            }
          }
        });

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            dispose();
          }
        });

    BaseTransformDialog.setSize(shell, 320, 480, true);

    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return tableName != null;
  }

  private void addButtons() {
    buttonsComposite = new Composite(shell, SWT.NONE);
    props.setLook(buttonsComposite);
    buttonsComposite.setLayout(new FormLayout());

    activeSchemaTable = null;

    ToolBar treeTb = new ToolBar(shell, SWT.HORIZONTAL | SWT.FLAT);
    expandAll = new ToolItem(treeTb, SWT.PUSH);
    expandAll.setImage(GuiResource.getInstance().getImageExpandAll());
    collapseAll = new ToolItem(treeTb, SWT.PUSH);
    collapseAll.setImage(GuiResource.getInstance().getImageCollapseAll());
    fdExpandAll = new FormData();
    fdExpandAll.right = new FormAttachment(100, 0);
    fdExpandAll.top = new FormAttachment(0, 0);
    treeTb.setLayoutData(fdExpandAll);

    expandAll.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent event) {
            expandAllItems(wTree.getItems(), true);
          }
        });

    collapseAll.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent event) {
            expandAllItems(wTree.getItems(), false);
          }
        });

    bPrev = new Button(buttonsComposite, SWT.PUSH);
    bPrev.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.Preview100", Const.NVL(activeSchemaTable, "?")));
    bPrev.setEnabled(activeSchemaTable != null);
    bPrev.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            previewTable(activeSchemaTable, false);
          }
        });
    FormData prevData = new FormData();
    prevData.left = new FormAttachment(0, 0);
    prevData.right = new FormAttachment(100, 0);
    prevData.top = new FormAttachment(0, 0);
    bPrev.setLayoutData(prevData);

    bPrevN = new Button(buttonsComposite, SWT.PUSH);
    bPrevN.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.PreviewN", Const.NVL(activeSchemaTable, "?")));
    bPrevN.setEnabled(activeSchemaTable != null);
    bPrevN.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            previewTable(activeSchemaTable, true);
          }
        });
    FormData prevNData = new FormData();
    prevNData.left = new FormAttachment(0, 0);
    prevNData.right = new FormAttachment(100, 0);
    prevNData.top = new FormAttachment(bPrev, props.getMargin());
    bPrevN.setLayoutData(prevNData);

    bCount = new Button(buttonsComposite, SWT.PUSH);
    bCount.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.ShowSize", Const.NVL(activeSchemaTable, "?")));
    bCount.setEnabled(activeSchemaTable != null);
    bCount.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            showCount(activeSchemaTable);
          }
        });
    FormData countData = new FormData();
    countData.left = new FormAttachment(0, 0);
    countData.right = new FormAttachment(100, 0);
    countData.top = new FormAttachment(bPrevN, props.getMargin());
    bCount.setLayoutData(countData);

    bShow = new Button(buttonsComposite, SWT.PUSH);
    bShow.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.ShowLayout", Const.NVL(activeSchemaTable, "?")));
    bShow.setEnabled(activeSchemaTable != null);
    bShow.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            showTable(activeSchemaTable);
          }
        });
    FormData showData = new FormData();
    showData.left = new FormAttachment(0, 0);
    showData.right = new FormAttachment(100, 0);
    showData.top = new FormAttachment(bCount, props.getMargin() * 7);
    bShow.setLayoutData(showData);

    bDDL = new Button(buttonsComposite, SWT.PUSH);
    bDDL.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.GenDDL"));
    bDDL.setEnabled(activeSchemaTable != null);
    bDDL.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getDDL(activeSchemaTable);
          }
        });
    FormData ddlData = new FormData();
    ddlData.left = new FormAttachment(0, 0);
    ddlData.right = new FormAttachment(100, 0);
    ddlData.top = new FormAttachment(bShow, props.getMargin());
    bDDL.setLayoutData(ddlData);

    bDDL2 = new Button(buttonsComposite, SWT.PUSH);
    bDDL2.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.GenDDLOtherConn"));
    bDDL2.setEnabled(activeSchemaTable != null);
    bDDL2.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getDDLForOther(activeSchemaTable);
          }
        });
    bDDL2.setEnabled(databases != null);
    FormData ddl2Data = new FormData();
    ddl2Data.left = new FormAttachment(0, 0);
    ddl2Data.right = new FormAttachment(100, 0);
    ddl2Data.top = new FormAttachment(bDDL, props.getMargin());
    bDDL2.setLayoutData(ddl2Data);

    bSql = new Button(buttonsComposite, SWT.PUSH);
    bSql.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.OpenSQL", Const.NVL(activeSchemaTable, "?")));
    bSql.setEnabled(activeSchemaTable != null);
    bSql.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getSql(activeSchemaTable);
          }
        });
    FormData sqlData = new FormData();
    sqlData.left = new FormAttachment(0, 0);
    sqlData.right = new FormAttachment(100, 0);
    sqlData.top = new FormAttachment(bDDL2, props.getMargin());
    bSql.setLayoutData(sqlData);

    bTruncate = new Button(buttonsComposite, SWT.PUSH);
    bTruncate.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.Truncate", Const.NVL(activeSchemaTable, "?")));
    bTruncate.setEnabled(activeSchemaTable != null);
    bTruncate.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            getTruncate(activeSchemaTable);
          }
        });
    FormData truncateData = new FormData();
    truncateData.left = new FormAttachment(0, 0);
    truncateData.right = new FormAttachment(100, 0);
    truncateData.top = new FormAttachment(bSql, props.getMargin() * 7);
    bTruncate.setLayoutData(truncateData);

    FormData fdComposite = new FormData();
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.top = new FormAttachment(0, 20);
    buttonsComposite.setLayoutData(fdComposite);
  }

  private void expandAllItems(TreeItem[] treeitems, boolean expand) {
    for (TreeItem item : treeitems) {
      item.setExpanded(expand);
      if (item.getItemCount() > 0) {
        expandAllItems(item.getItems(), expand);
      }
    }
  }

  private void refreshButtons(String table) {
    activeSchemaTable = table;
    bPrev.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.Preview100", Const.NVL(table, "?")));
    bPrev.setEnabled(table != null);

    bPrevN.setText(
        BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.PreviewN", Const.NVL(table, "?")));
    bPrevN.setEnabled(table != null);

    bCount.setText(
        BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.ShowSize", Const.NVL(table, "?")));
    bCount.setEnabled(table != null);

    bShow.setText(
        BaseMessages.getString(
            PKG, "DatabaseExplorerDialog.Menu.ShowLayout", Const.NVL(table, "?")));
    bShow.setEnabled(table != null);

    bDDL.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.GenDDL"));
    bDDL.setEnabled(table != null);

    bDDL2.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.GenDDLOtherConn"));
    bDDL2.setEnabled(table != null);

    bSql.setText(
        BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.OpenSQL", Const.NVL(table, "?")));
    bSql.setEnabled(table != null);

    bTruncate.setText(
        BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.Truncate", Const.NVL(table, "?")));
    bTruncate.setEnabled(table != null);

    shell.layout(true, true);
  }

  private boolean getData() {
    GetDatabaseInfoProgressDialog gdipd = new GetDatabaseInfoProgressDialog(shell, variables, dbMeta);
    DatabaseMetaInformation dmi = gdipd.open();
    if (dmi != null) {
      // Clear the tree top entry
      if (tiTree != null && !tiTree.isDisposed()) {
        tiTree.dispose();
      }

      // New entry in the tree
      tiTree = new TreeItem(wTree, SWT.NONE);
      tiTree.setImage(GuiResource.getInstance().getImageFolderConnections());
      tiTree.setText(dbMeta == null ? "" : dbMeta.getName());

      // Show the catalogs...
      Catalog[] catalogs = dmi.getCatalogs();
      if (catalogs != null) {
        TreeItem tiCat = new TreeItem(tiTree, SWT.NONE);
        tiCat.setImage(GuiResource.getInstance().getImageFolder());
        tiCat.setText(STRING_CATALOG);

        for (int i = 0; i < catalogs.length; i++) {
          TreeItem newCat = new TreeItem(tiCat, SWT.NONE);
          newCat.setImage(GuiResource.getInstance().getImageConnection());
          newCat.setText(catalogs[i].getCatalogName());

          for (int j = 0; j < catalogs[i].getItems().length; j++) {
            String tableName = catalogs[i].getItems()[j];

            TreeItem ti = new TreeItem(newCat, SWT.NONE);
            ti.setImage(GuiResource.getInstance().getImageTable());
            ti.setText(tableName);
          }
        }
      }

      // The schema's
      Schema[] schemas = dmi.getSchemas();
      if (schemas != null) {
        TreeItem tiSch = new TreeItem(tiTree, SWT.NONE);
        tiSch.setImage(GuiResource.getInstance().getImageFolder());
        tiSch.setText(STRING_SCHEMAS);

        for (int i = 0; i < schemas.length; i++) {
          TreeItem newSch = new TreeItem(tiSch, SWT.NONE);
          newSch.setImage(GuiResource.getInstance().getImageSchema());
          newSch.setText(schemas[i].getSchemaName());

          for (int j = 0; j < schemas[i].getItems().length; j++) {
            String tableName = schemas[i].getItems()[j];

            TreeItem ti = new TreeItem(newSch, SWT.NONE);
            ti.setImage(GuiResource.getInstance().getImageTable());
            ti.setText(tableName);
          }
        }
      }

      // The tables in general...
      TreeItem tiTab = null;
      String tabnames[] = dmi.getTables();
      if (tabnames != null) {
        tiTab = new TreeItem(tiTree, SWT.NONE);
        tiTab.setImage(GuiResource.getInstance().getImageFolder());
        tiTab.setText(STRING_TABLES);
        tiTab.setExpanded(true);

        for (int i = 0; i < tabnames.length; i++) {
          TreeItem newTab = new TreeItem(tiTab, SWT.NONE);
          newTab.setImage(GuiResource.getInstance().getImageTable());
          newTab.setText(tabnames[i]);
        }
      }

      // The views...
      TreeItem tiView = null;
      String views[] = dmi.getViews();
      if (views != null) {
        tiView = new TreeItem(tiTree, SWT.NONE);
        tiView.setImage(GuiResource.getInstance().getImageFolder());
        tiView.setText(STRING_VIEWS);
        for (int i = 0; i < views.length; i++) {
          TreeItem newView = new TreeItem(tiView, SWT.NONE);
          newView.setImage(GuiResource.getInstance().getImageView());
          newView.setText(views[i]);
        }
      }

      // The synonyms
      TreeItem tiSyn = null;
      String[] syn = dmi.getSynonyms();
      if (syn != null) {
        tiSyn = new TreeItem(tiTree, SWT.NONE);
        tiSyn.setImage(GuiResource.getInstance().getImageFolder());
        tiSyn.setText(STRING_SYNONYMS);
        for (int i = 0; i < syn.length; i++) {
          TreeItem newSyn = new TreeItem(tiSyn, SWT.NONE);
          newSyn.setImage(GuiResource.getInstance().getImageSynonym());
          newSyn.setText(syn[i]);
        }
      }

      // Make sure the selected table is shown...
      if (!StringUtils.isEmpty(selectedTable)) {
        TreeItem ti = null;
        if (ti == null && tiTab != null) {
          ti = ConstUi.findTreeItem(tiTab, selectedSchema, selectedTable);
        }
        if (ti == null && tiView != null) {
          ti = ConstUi.findTreeItem(tiView, selectedSchema, selectedTable);
        }
        if (ti == null && tiTree != null) {
          ti = ConstUi.findTreeItem(tiTree, selectedSchema, selectedTable);
        }
        if (ti == null && tiSyn != null) {
          ti = ConstUi.findTreeItem(tiSyn, selectedSchema, selectedTable);
        }

        if (ti != null) {
          wTree.setSelection(new TreeItem[] {ti});
          wTree.showSelection();
          refreshButtons(dbMeta.getQuotedSchemaTableCombination(variables, selectedSchema, selectedTable));
        }

        selectedTable = null;
      }

      tiTree.setExpanded(true);
    } else {
      return false;
    }

    return true;
  }

  private String getSchemaTable() {
    TreeItem ti[] = wTree.getSelection();
    if (ti.length == 1) {
      // Get the parent.
      TreeItem parent = ti[0].getParentItem();
      if (parent != null) {
        String schemaName = parent.getText();
        String tableName = ti[0].getText();

        if (ti[0].getItemCount() == 0) // No children, only the tables themselves...
        {
          String tab = null;
          if (schemaName.equalsIgnoreCase(STRING_TABLES)
              || schemaName.equalsIgnoreCase(STRING_VIEWS)
              || schemaName.equalsIgnoreCase(STRING_SYNONYMS)
              || (schemaName != null && schemaName.length() == 0)) {
            tab = tableName;
          } else {
            tab = dbMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          }
          return tab;
        }
      }
    }
    return null;
  }

  public void setTreeMenu() {
    final String table = getSchemaTable();
    if (table != null) {
      Menu mTree = new Menu(shell, SWT.POP_UP);

      MenuItem miPrev = new MenuItem(mTree, SWT.PUSH);
      miPrev.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.Preview100", table));
      miPrev.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              previewTable(table, false);
            }
          });
      MenuItem miPrevN = new MenuItem(mTree, SWT.PUSH);
      miPrevN.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.PreviewN", table));
      miPrevN.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              previewTable(table, true);
            }
          });
      // MenuItem miEdit   = new MenuItem(mTree, SWT.PUSH); miEdit.setText("Open ["+table+"] for
      // editing");
      // miEdit.addSelectionListener( new SelectionAdapter() { public void
      // widgetSelected(SelectionEvent e) { editTable(table); }});
      MenuItem miCount = new MenuItem(mTree, SWT.PUSH);
      miCount.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.ShowSize", table));
      miCount.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              showCount(table);
            }
          });

      new MenuItem(mTree, SWT.SEPARATOR);

      MenuItem miShow = new MenuItem(mTree, SWT.PUSH);
      miShow.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.ShowLayout", table));
      miShow.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              showTable(table);
            }
          });
      MenuItem miDDL = new MenuItem(mTree, SWT.PUSH);
      miDDL.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.GenDDL"));
      miDDL.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              getDDL(table);
            }
          });
      MenuItem miDDL2 = new MenuItem(mTree, SWT.PUSH);
      miDDL2.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.GenDDLOtherConn"));
      miDDL2.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              getDDLForOther(table);
            }
          });
      miDDL2.setEnabled(databases != null);
      MenuItem miSql = new MenuItem(mTree, SWT.PUSH);
      miSql.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.OpenSQL", table));
      miSql.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              getSql(table);
            }
          });

      new MenuItem(mTree, SWT.SEPARATOR);

      MenuItem miProfile = new MenuItem(mTree, SWT.PUSH);
      miProfile.setText(
          BaseMessages.getString(PKG, "DatabaseExplorerDialog.Menu.ProfileTable", table));
      miProfile.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              profileTable(table);
            }
          });

      wTree.setMenu(mTree);
    } else {
      wTree.setMenu(null);
    }
  }

  public void previewTable(String tableName, boolean asklimit) {
    int limit = 100;
    if (asklimit) {
      // Ask how many lines we should preview.
      String shellText = BaseMessages.getString(PKG, "DatabaseExplorerDialog.PreviewTable.Title");
      String lineText = BaseMessages.getString(PKG, "DatabaseExplorerDialog.PreviewTable.Message");
      EnterNumberDialog end = new EnterNumberDialog(shell, limit, shellText, lineText);
      int samples = end.open();
      if (samples >= 0) {
        limit = samples;
      }
    }

    GetPreviewTableProgressDialog pd =
        new GetPreviewTableProgressDialog(shell, variables, dbMeta, null, tableName, limit);
    List<Object[]> rows = pd.open();
    if (rows != null) // otherwise an already shown error...
    {
      if (rows.size() > 0) {
        PreviewRowsDialog prd =
            new PreviewRowsDialog(shell, variables, SWT.NONE, tableName, pd.getRowMeta(), rows);
        prd.open();
      } else {
        MessageBox mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        mb.setMessage(BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoRows.Message"));
        mb.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoRows.Title"));
        mb.open();
      }
    }
  }

  public void showTable(String tableName) {
    String sql = dbMeta.getSqlQueryFields(tableName);
    GetQueryFieldsProgressDialog pd = new GetQueryFieldsProgressDialog(shell, variables, dbMeta, sql);
    IRowMeta result = pd.open();
    if (result != null) {
      TransformFieldsDialog sfd =
          new TransformFieldsDialog(shell, variables, SWT.NONE, tableName, result);
      sfd.open();
    }
  }

  public void showCount(String tableName) {
    GetTableSizeProgressDialog pd = new GetTableSizeProgressDialog(shell, variables, dbMeta, tableName);
    Long size = pd.open();
    if (size != null) {
      MessageBox mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "DatabaseExplorerDialog.TableSize.Message", tableName, size.toString()));
      mb.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.TableSize.Title"));
      mb.open();
    }
  }

  public void getDDL(String tableName) {
    Database db = new Database(loggingObject, variables, dbMeta );
    try {
      db.connect();
      IRowMeta r = db.getTableFields(tableName);
      String sql = db.getCreateTableStatement(tableName, r, null, false, null, true);
      SqlEditor se = new SqlEditor( shell, SWT.NONE, variables, dbMeta, dbcache, sql);
      se.open();
    } catch (HopDatabaseException dbe) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "Dialog.Error.Header"),
          BaseMessages.getString(PKG, "DatabaseExplorerDialog.Error.RetrieveLayout"),
          dbe);
    } finally {
      db.disconnect();
    }
  }

  public void getDDLForOther(String tableName) {
    if (databases != null) {
      Database database = new Database(loggingObject, variables, dbMeta );
      try {
        database.connect();

        IRowMeta rowMeta = database.getTableFields(tableName);

        // Now select the other connection...

        // Only take non-SAP ERP connections....
        List<DatabaseMeta> databaseMetaList = new ArrayList<>();
        for (int i = 0; i < databases.size(); i++) {
          databaseMetaList.add(databases.get(i));
        }

        String connectionNames[] = new String[databaseMetaList.size()];
        for (int i = 0; i < connectionNames.length; i++) {
          connectionNames[i] = (databaseMetaList.get(i)).getName();
        }

        EnterSelectionDialog enterSelectionDialog =
            new EnterSelectionDialog(
                shell,
                connectionNames,
                BaseMessages.getString(PKG, "DatabaseExplorerDialog.TargetDatabase.Title"),
                BaseMessages.getString(PKG, "DatabaseExplorerDialog.TargetDatabase.Message"));
        String target = enterSelectionDialog.open();
        if (target != null) {
          DatabaseMeta targetDatabaseMeta = DatabaseMeta.findDatabase(databaseMetaList, target);
          Database targetDatabase = new Database(loggingObject, variables, targetDatabaseMeta );

          String sql = targetDatabase.getCreateTableStatement(tableName, rowMeta, null, false, null, true);
          SqlEditor sqlEditor = new SqlEditor(shell, SWT.NONE, variables, dbMeta, dbcache, sql);
          sqlEditor.open();
        }
      } catch (HopDatabaseException dbe) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "Dialog.Error.Header"),
            BaseMessages.getString(PKG, "DatabaseExplorerDialog.Error.GenDDL"),
            dbe);
      } finally {
        database.disconnect();
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.NONE | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoConnectionsKnown.Message"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoConnectionsKnown.Title"));
      mb.open();
    }
  }

  public void getSql(String tableName) {
    SqlEditor sqlEditor =
        new SqlEditor(shell, SWT.NONE, variables, dbMeta, dbcache, "SELECT * FROM " + tableName);
    sqlEditor.open();
  }

  /**
   * Fire off a pipeline that data profiles the specified table...<br>
   *
   * @param tableName
   */
  public void profileTable(String tableName) {
    try {
      PipelineProfileFactory profileFactory = new PipelineProfileFactory(variables, dbMeta, tableName);
      PipelineMeta pipelineMeta = profileFactory.generatePipeline(new LoggingObject(tableName));
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              pipelineMeta,
              new String[] {
                PipelineProfileFactory.RESULT_TRANSFORM_NAME,
              },
              new int[] {
                25000,
              });
      progressDialog.open();

      if (!progressDialog.isCancelled()) {
        Pipeline pipeline = progressDialog.getPipeline();
        String loggingText = progressDialog.getLoggingText();

        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        }

        PreviewRowsDialog prd =
            new PreviewRowsDialog(
                shell,
                variables,
                SWT.NONE,
                PipelineProfileFactory.RESULT_TRANSFORM_NAME,
                progressDialog.getPreviewRowsMeta(PipelineProfileFactory.RESULT_TRANSFORM_NAME),
                progressDialog.getPreviewRows(PipelineProfileFactory.RESULT_TRANSFORM_NAME),
                loggingText);
        prd.open();
      }

    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseExplorerDialog.UnexpectedProfilingError.Title"),
          BaseMessages.getString(PKG, "DatabaseExplorerDialog.UnexpectedProfilingError.Message"),
          e);
    }
  }

  public void getTruncate(String activeSchemaTable) {
    SqlEditor sql =
        new SqlEditor(
          shell, SWT.NONE, variables, dbMeta, dbcache, "-- TRUNCATE TABLE " + activeSchemaTable);
    sql.open();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void handleOK() {
    if (justLook) {
      dispose();
      return;
    }
    TreeItem ti[] = wTree.getSelection();
    if (ti.length == 1) {
      // Get the parent.
      String table = ti[0].getText();
      String[] path = ConstUi.getTreeStrings(ti[0]);
      if (path.length == 3) {
        if (STRING_TABLES.equalsIgnoreCase(path[1])
            || STRING_VIEWS.equalsIgnoreCase(path[1])
            || STRING_SYNONYMS.equalsIgnoreCase(path[1])) {
          schemaName = null;
          tableName = table;
          if (dbMeta.getIDatabase().isMsSqlServerVariant()) {
            String st[] = tableName.split("\\.", 2);
            if (st.length > 1) { // we have a dot in there and need to separate
              schemaName = st[0];
              tableName = st[1];
            }
          }
          dispose();
        }
      }
      if (path.length == 4) {
        if (STRING_SCHEMAS.equals(path[1]) || STRING_CATALOG.equals(path[1])) {
          if (splitSchemaAndTable) {
            schemaName = path[2];
            tableName = path[3];
          } else {
            schemaName = null;
            tableName = dbMeta.getQuotedSchemaTableCombination(variables, path[2], path[3]);
          }
          dispose();
        }
      }
    }
  }

  public void openSchema(SelectionEvent e) {
    TreeItem sel = (TreeItem) e.item;

    TreeItem up1 = sel.getParentItem();
    if (up1 != null) {
      TreeItem up2 = up1.getParentItem();
      if (up2 != null) {
        TreeItem up3 = up2.getParentItem();
        if (up3 != null) {
          tableName = sel.getText();
          if (!justLook) {
            handleOK();
          } else {
            previewTable(tableName, false);
          }
        }
      }
    }
  }

  /** @return the schemaName */
  public String getSchemaName() {
    return schemaName;
  }

  /** @param schemaName the schemaName to set */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /** @return the tableName */
  public String getTableName() {
    return tableName;
  }

  /** @param tableName the tableName to set */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @return the splitSchemaAndTable */
  public boolean isSplitSchemaAndTable() {
    return splitSchemaAndTable;
  }

  /** @param splitSchemaAndTable the splitSchemaAndTable to set */
  public void setSplitSchemaAndTable(boolean splitSchemaAndTable) {
    this.splitSchemaAndTable = splitSchemaAndTable;
  }

  /** @return the selectSchema */
  public String getSelectedSchema() {
    return selectedSchema;
  }

  /** @param selectSchema the selectSchema to set */
  public void setSelectedSchema(String selectSchema) {
    this.selectedSchema = selectSchema;
  }

  public void setSelectedSchemaAndTable(String schemaName, String tableName) {
    this.selectedSchema = schemaName;
    this.selectedTable = tableName;
  }
}
