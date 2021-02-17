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

package org.apache.hop.ui.i18n.editor;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.i18n.BundleFile;
import org.apache.hop.ui.i18n.BundlesStore;
import org.apache.hop.ui.i18n.KeyOccurrence;
import org.apache.hop.ui.i18n.MessagesSourceCrawler;
import org.apache.hop.ui.i18n.TranslationsStore;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Class to allow non-developers to edit translation messages files.
 *
 * @author matt
 */
public class Translator {
  private static final Class<?> PKG = Translator.class; // For Translator

  private static final String TRANSLATOR_NAMESPACE = "hop-translator";

  public static final String APP_NAME = BaseMessages.getString(PKG, "i18nDialog.ApplicationName");

  private Display display;
  private Shell shell;
  private ILogChannel log;
  private PropsUi props;

  /** The crawler that can find and contain all the keys in the source code */
  private MessagesSourceCrawler crawler;

  /** The translations store containing all the translations for all keys, locale, packages */
  private TranslationsStore store;

  private SashForm sashform;
  private List wLocale;
  private TableView wPackages;
  private List wTodo;

  private String selectedLocale;
  private String selectedMessagesPackage;

  private Text wReferenceLocale;
  private Text wSelectedSourceFolder;
  private Text wSelectedPackage;

  private Text wKey;
  private Text wMain;
  private Text wValue;
  private Text wSourceFile;
  private Text wBundleFile;
  private Text wSource;

  private Button wReload;
  private Button wClose;
  private Button wApply;
  private Button wRevert;
  private Button wSave;
  private Button wZip;

  private Button wSearch;
  private Button wNext;

  private Button wSearchV;
  private Button wNextV;

  /*
   * private Button wSearchG; private Button wNextG;
   */

  private Button wAll;

  private String referenceLocale;
  private String rootFolder;
  private java.util.List<String> sourceDirectories;
  private java.util.List<String> localeList;

  protected String lastValue;
  protected boolean lastValueChanged;
  protected String selectedKey;
  protected String searchString;
  protected String lastFoundKey;
  private String singleMessagesFile;

  private String selectedSourceFolder;
  private java.util.List<String> filesToAvoid;

  public Translator(Display display) {
    this.display = display;
    this.log = new LogChannel(APP_NAME);
    this.props = PropsUi.getInstance();
    this.filesToAvoid = new ArrayList<>();
  }

  public boolean showKey(String key, String messagesPackage) {
    return !key.startsWith("System.")
        || messagesPackage.equals(BaseMessages.class.getPackage().getName());
  }

  public void readFiles() throws HopFileException {
    log.logBasic(BaseMessages.getString(PKG, "i18n.Log.ScanningSourceDirectories"));
    try {
      // Find and read read all the messages files in the root folder
      //
      BundlesStore bundlesStore = new BundlesStore(rootFolder);
      bundlesStore.findAllMessagesBundles();

      // Find all the source directories in the root folder and crawl through them
      //
      crawler = new MessagesSourceCrawler(log, rootFolder, bundlesStore);
      crawler.setFilesToAvoid(filesToAvoid);
      crawler.crawl();

      // Convenience class to look up and manage things
      //
      store =
          new TranslationsStore(
              log,
              localeList,
              referenceLocale,
              crawler.getSourcePackageOccurrences(),
              bundlesStore);

      // Keep some statistics
      //
      Map<String, int[]> folderKeyCounts = new HashMap<>();
      Map<String, Integer> nrKeysPerFolder = new HashMap<>();

      for (String sourceFolder : sourceDirectories) {
        folderKeyCounts.put(sourceFolder, new int[localeList.size()]);
      }

      for (String sourceFolder : sourceDirectories) {

        int[] keyCounts = folderKeyCounts.get(sourceFolder);
        int nrKeys = 0;

        for (int i = 0; i < localeList.size(); i++) {
          String locale = localeList.get(i);

          // Count the number of keys available in that locale...
          //
          for (KeyOccurrence keyOccurrence : crawler.getKeyOccurrences(sourceFolder)) {
            // We don't want the system keys, just the regular ones.
            //
            if (showKey(keyOccurrence.getKey(), keyOccurrence.getMessagesPackage())) {
              String value =
                  store.lookupKeyValue(
                      locale, keyOccurrence.getMessagesPackage(), keyOccurrence.getKey());
              if (!Utils.isEmpty(value)) {
                keyCounts[i]++;
              }
              if (locale.equals(referenceLocale)) {
                nrKeys++;
              }
            }
          }
        }
        nrKeysPerFolder.put(sourceFolder, new Integer(nrKeys));
      }

      DecimalFormat pctFormat = new DecimalFormat("#00.00");
      DecimalFormat nrFormat = new DecimalFormat("00");

      for (String sourceFolder : sourceDirectories) {
        System.out.println("-------------------------------------");
        System.out.println("Folder: " + sourceFolder);
        System.out.println("-------------------------------------");

        int nrKeys = nrKeysPerFolder.get(sourceFolder);
        System.out.println(BaseMessages.getString(PKG, "i18n.Log.NumberOfKeysFound", "" + nrKeys));

        int[] keyCounts = folderKeyCounts.get(sourceFolder);

        String[] locales = localeList.toArray(new String[localeList.size()]);
        for (int i = 0; i < locales.length; i++) {
          for (int j = 0; j < locales.length - 1; j++) {
            if (keyCounts[j] < keyCounts[j + 1]) {
              int c = keyCounts[j];
              keyCounts[j] = keyCounts[j + 1];
              keyCounts[j + 1] = c;

              String l = locales[j];
              locales[j] = locales[j + 1];
              locales[j + 1] = l;
            }
          }
        }

        for (int i = 0; i < locales.length; i++) {
          double donePct = 100 * (double) keyCounts[i] / nrKeys;
          int missingKeys = nrKeys - keyCounts[i];
          String statusKeys =
              "# "
                  + nrFormat.format(i + 1)
                  + " : "
                  + locales[i]
                  + " : "
                  + pctFormat.format(donePct)
                  + "% "
                  + BaseMessages.getString(PKG, "i18n.Log.CompleteKeys", keyCounts[i])
                  + (missingKeys != 0
                      ? BaseMessages.getString(PKG, "i18n.Log.MissingKeys", missingKeys)
                      : "");
          System.out.println(statusKeys);
        }
      }

    } catch (Exception e) {
      throw new HopFileException(
          BaseMessages.getString(PKG, "i18n.Log.UnableToGetFiles", sourceDirectories.toString()),
          e);
    }
  }

  public void loadConfiguration(String configFile, String sourceFolder) throws Exception {
    // What are the locale to handle?
    //
    this.rootFolder = sourceFolder;

    localeList = new ArrayList<>();
    sourceDirectories = new ArrayList<>();

    FileObject file = HopVfs.getFileObject(configFile);
    if (file.exists()) {

      try {
        Document doc = XmlHandler.loadXmlFile(file);
        Node configNode = XmlHandler.getSubNode(doc, "translator-config");

        referenceLocale = XmlHandler.getTagValue(configNode, "reference-locale");
        singleMessagesFile = XmlHandler.getTagValue(configNode, "single-messages-file");

        Node localeListNode = XmlHandler.getSubNode(configNode, "locale-list");
        int nrLocale = XmlHandler.countNodes(localeListNode, "locale");
        if (nrLocale > 0) {
          localeList.clear();
        }
        for (int i = 0; i < nrLocale; i++) {
          Node localeNode = XmlHandler.getSubNodeByNr(localeListNode, "locale", i);
          String locale = XmlHandler.getNodeValue(localeNode);
          localeList.add(locale);
        }

        Node filesToAvoidNode = XmlHandler.getSubNode(configNode, "files-to-avoid");
        java.util.List<Node> filesNodes = XmlHandler.getNodes(filesToAvoidNode, "filename");
        filesToAvoid = new ArrayList<>();
        for (Node fileNode : filesNodes) {
          filesToAvoid.add(XmlHandler.getNodeValue(fileNode));
        }
      } catch (Exception e) {
        log.logError("Translator", "Error reading translator.xml", e);
      }
    }
  }

  public void open() {
    shell = new Shell(display);
    shell.setLayout(new FillLayout());
    shell.setText(APP_NAME);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    try {
      readFiles();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          "Error reading translations",
          "There was an unexpected error reading the translations",
          e);
    }

    // Put something on the screen
    sashform = new SashForm(shell, SWT.HORIZONTAL);
    sashform.setLayout(new FormLayout());

    addLists();
    addGrid();
    addListeners();

    sashform.setWeights(new int[] {30, 70});
    sashform.setVisible(true);

    shell.pack();

    refresh();

    wPackages.optWidth(true);
    wPackages.getTable().getColumn(1).setWidth(1);

    BaseTransformDialog.setSize(shell);

    shell.open();
  }

  private void addListeners() {
    // In case someone dares to press the [X] in the corner ;-)
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            e.doit = quitFile();
          }
        });

    wReload.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            int[] idx = wPackages.table.getSelectionIndices();
            reload();
            wPackages.table.setSelection(idx);
          }
        });

    wZip.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            saveFilesToZip();
          }
        });

    wClose.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            quitFile();
          }
        });
  }

  public void reload() {
    try {
      shell.setCursor(display.getSystemCursor(SWT.CURSOR_WAIT));
      readFiles();
      shell.setCursor(null);

      refresh();
    } catch (Exception e) {
      new ErrorDialog(
          shell, "Error loading data", "There was an unexpected error re-loading the data", e);
    }
  }

  public boolean quitFile() {
    java.util.List<BundleFile> changedBundleFiles = store.getChangedBundleFiles();
    if (changedBundleFiles.size() > 0) {
      MessageBox mb = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_WARNING);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "i18nDialog.ChangedFilesWhenExit", changedBundleFiles.size() + ""));
      mb.setText(BaseMessages.getString(PKG, "i18nDialog.Warning"));

      int answer = mb.open();
      if (answer == SWT.NO) {
        return false;
      }
    }

    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);

    shell.dispose();
    display.dispose();
    return true;
  }

  private void addLists() {
    Composite composite = new Composite(sashform, SWT.NONE);
    props.setLook(composite);
    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = Const.FORM_MARGIN;
    formLayout.marginWidth = Const.FORM_MARGIN;
    composite.setLayout(formLayout);

    wLocale = new List(composite, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    FormData fdLocale = new FormData();
    fdLocale.left = new FormAttachment(0, 0);
    fdLocale.right = new FormAttachment(100, 0);
    fdLocale.top = new FormAttachment(0, 0);
    fdLocale.bottom = new FormAttachment(10, 0);
    wLocale.setLayoutData(fdLocale);

    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "i18nDialog.SourceFolder"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "i18nDialog.Packagename"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
        };

    wPackages =
        new TableView(
            new Variables(),
            composite,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            1,
            true,
            null,
            props);
    FormData fdPackages = new FormData();
    fdPackages.left = new FormAttachment(0, 0);
    fdPackages.right = new FormAttachment(100, 0);
    fdPackages.top = new FormAttachment(wLocale, props.getMargin());
    fdPackages.bottom = new FormAttachment(100, 0);
    wPackages.setLayoutData(fdPackages);
    wPackages.setSortable(false);

    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.bottom = new FormAttachment(100, 0);
    composite.setLayoutData(fdComposite);

    // Add a selection listener.
    wLocale.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            refreshGrid();
          }
        });
    wPackages.table.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            refreshGrid();
          }
        });

    composite.layout();
  }

  private void addGrid() {
    Composite composite = new Composite(sashform, SWT.NONE);
    props.setLook(composite);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    composite.setLayout(formLayout);

    wReload = new Button(composite, SWT.NONE);
    wReload.setText(BaseMessages.getString(PKG, "i18nDialog.Reload"));
    wSave = new Button(composite, SWT.NONE);
    wSave.setText(BaseMessages.getString(PKG, "i18nDialog.Save"));
    wZip = new Button(composite, SWT.NONE);
    wZip.setText(BaseMessages.getString(PKG, "i18nDialog.Zip"));
    wZip.setToolTipText(BaseMessages.getString(PKG, "i18nDialog.Zip.Tip"));
    wClose = new Button(composite, SWT.NONE);
    wClose.setText(BaseMessages.getString(PKG, "i18nDialog.Close"));

    BaseTransformDialog.positionBottomButtons(
        composite,
        new Button[] {
          wReload, wSave, wZip, wClose,
        },
        props.getMargin() * 3,
        null);

    /*
     * wSearchG = new Button(composite, SWT.PUSH); wSearchG.setText("   Search &key  "); FormData fdSearchG = new
     * FormData(); fdSearchG.left = new FormAttachment(0, 0); fdSearchG.bottom = new FormAttachment(100, 0);
     * wSearchG.setLayoutData(fdSearchG);
     *
     * wNextG = new Button(composite, SWT.PUSH); wNextG.setText("   Next ke&y  "); FormData fdNextG = new FormData();
     * fdNextG.left = new FormAttachment(wSearchG, props.getMargin()); fdNextG.bottom = new FormAttachment(100, 0);
     * wNextG.setLayoutData(fdNextG);
     */

    int left = 25;
    int middle = 40;

    wAll = new Button(composite, SWT.CHECK);
    wAll.setText(BaseMessages.getString(PKG, "i18nDialog.ShowAllkeys"));
    props.setLook(wAll);
    FormData fdAll = new FormData();
    fdAll.left = new FormAttachment(0, 0);
    fdAll.right = new FormAttachment(left, 0);
    fdAll.bottom = new FormAttachment(wClose, -props.getMargin());
    wAll.setLayoutData(fdAll);

    Label wlTodo = new Label(composite, SWT.LEFT);
    props.setLook(wlTodo);
    wlTodo.setText(BaseMessages.getString(PKG, "i18nDialog.ToDoList"));
    FormData fdlTodo = new FormData();
    fdlTodo.left = new FormAttachment(0, 0);
    fdlTodo.right = new FormAttachment(left, 0);
    fdlTodo.top = new FormAttachment(0, 0);
    wlTodo.setLayoutData(fdlTodo);

    wTodo = new List(composite, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    FormData fdTodo = new FormData();
    fdTodo.left = new FormAttachment(0, 0);
    fdTodo.right = new FormAttachment(left, 0);
    fdTodo.top = new FormAttachment(wlTodo, props.getMargin());
    fdTodo.bottom = new FormAttachment(wAll, -props.getMargin());
    wTodo.setLayoutData(fdTodo);
    Control lastControl = wlTodo;

    // The reference locale
    //
    Label wlReferenceLocale = new Label(composite, SWT.RIGHT);
    wlReferenceLocale.setText(BaseMessages.getString(PKG, "i18nDialog.TranslationReferenceLocale"));
    props.setLook(wlReferenceLocale);
    FormData fdlReferenceLocale = new FormData();
    fdlReferenceLocale.left = new FormAttachment(left, props.getMargin());
    fdlReferenceLocale.right = new FormAttachment(middle, 0);
    fdlReferenceLocale.top = new FormAttachment(lastControl, props.getMargin());
    wlReferenceLocale.setLayoutData(fdlReferenceLocale);

    wReferenceLocale = new Text(composite, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wReferenceLocale);
    FormData fdReferenceLocale = new FormData();
    fdReferenceLocale.left = new FormAttachment(middle, props.getMargin());
    fdReferenceLocale.right = new FormAttachment(100, 0);
    fdReferenceLocale.top = new FormAttachment(wlReferenceLocale, 0, SWT.CENTER);
    wReferenceLocale.setLayoutData(fdReferenceLocale);
    wReferenceLocale.setEditable(false);
    wReferenceLocale.setText(Const.NVL(referenceLocale, ""));
    lastControl = wReferenceLocale;

    // The selected source folder
    //
    Label wlSelectedSourceFolder = new Label(composite, SWT.RIGHT);
    wlSelectedSourceFolder.setText(
        BaseMessages.getString(PKG, "i18nDialog.TranslationSelectedSourceFolder"));
    props.setLook(wlSelectedSourceFolder);
    FormData fdlSelectedSourceFolder = new FormData();
    fdlSelectedSourceFolder.left = new FormAttachment(left, props.getMargin());
    fdlSelectedSourceFolder.right = new FormAttachment(middle, 0);
    fdlSelectedSourceFolder.top = new FormAttachment(lastControl, props.getMargin());
    wlSelectedSourceFolder.setLayoutData(fdlSelectedSourceFolder);

    wSelectedSourceFolder = new Text(composite, SWT.SINGLE | SWT.LEFT);
    props.setLook(wSelectedSourceFolder);
    FormData fdSelectedSourceFolder = new FormData();
    fdSelectedSourceFolder.left = new FormAttachment(middle, props.getMargin());
    fdSelectedSourceFolder.right = new FormAttachment(100, 0);
    fdSelectedSourceFolder.top = new FormAttachment(wlSelectedSourceFolder, 0, SWT.CENTER);
    wSelectedSourceFolder.setLayoutData(fdSelectedSourceFolder);
    wSelectedSourceFolder.setEditable(false);
    lastControl = wSelectedSourceFolder;

    // The selected package
    //
    Label wlSelectedPackage = new Label(composite, SWT.RIGHT);
    wlSelectedPackage.setText(BaseMessages.getString(PKG, "i18nDialog.TranslationSelectedPackage"));
    props.setLook(wlSelectedPackage);
    FormData fdlSelectedPackage = new FormData();
    fdlSelectedPackage.left = new FormAttachment(left, props.getMargin());
    fdlSelectedPackage.right = new FormAttachment(middle, 0);
    fdlSelectedPackage.top = new FormAttachment(lastControl, props.getMargin());
    wlSelectedPackage.setLayoutData(fdlSelectedPackage);

    wSelectedPackage = new Text(composite, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wSelectedPackage);
    FormData fdSelectedPackage = new FormData();
    fdSelectedPackage.left = new FormAttachment(middle, props.getMargin());
    fdSelectedPackage.right = new FormAttachment(100, 0);
    fdSelectedPackage.top = new FormAttachment(wlSelectedPackage, 0, SWT.CENTER);
    wSelectedPackage.setLayoutData(fdSelectedPackage);
    wSelectedPackage.setEditable(false);
    lastControl = wSelectedPackage;

    // The key
    //
    Label wlKey = new Label(composite, SWT.RIGHT);
    wlKey.setText(BaseMessages.getString(PKG, "i18nDialog.TranslationKey"));
    props.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(left, props.getMargin());
    fdlKey.right = new FormAttachment(middle, 0);
    fdlKey.top = new FormAttachment(lastControl, props.getMargin());
    wlKey.setLayoutData(fdlKey);

    wKey = new Text(composite, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wKey);
    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(middle, props.getMargin());
    fdKey.right = new FormAttachment(100, 0);
    fdKey.top = new FormAttachment(wlKey, 0, SWT.CENTER);
    wKey.setLayoutData(fdKey);
    wKey.setEditable(false);
    lastControl = wKey;

    // The Main translation
    //
    Label wlMain = new Label(composite, SWT.RIGHT);
    wlMain.setText(BaseMessages.getString(PKG, "i18nDialog.MainTranslation"));
    props.setLook(wlMain);
    FormData fdlMain = new FormData();
    fdlMain.left = new FormAttachment(left, props.getMargin());
    fdlMain.right = new FormAttachment(middle, 0);
    fdlMain.top = new FormAttachment(wKey, props.getMargin());
    wlMain.setLayoutData(fdlMain);

    wMain = new Text(composite, SWT.MULTI | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wMain);
    FormData fdMain = new FormData();
    fdMain.left = new FormAttachment(middle, props.getMargin());
    fdMain.right = new FormAttachment(100, 0);
    fdMain.top = new FormAttachment(wKey, props.getMargin());
    fdMain.bottom = new FormAttachment(wKey, 250 + props.getMargin());
    wMain.setLayoutData(fdMain);
    wMain.setEditable(false);

    wSearch = new Button(composite, SWT.PUSH);
    wSearch.setText(BaseMessages.getString(PKG, "i18nDialog.Search"));
    FormData fdSearch = new FormData();
    fdSearch.left = new FormAttachment(left, 3 * props.getMargin());
    fdSearch.right = new FormAttachment(middle, -props.getMargin() * 2);
    fdSearch.top = new FormAttachment(wMain, 0, SWT.CENTER);
    wSearch.setLayoutData(fdSearch);

    wNext = new Button(composite, SWT.PUSH);
    wNext.setText(BaseMessages.getString(PKG, "i18nDialog.Next"));
    FormData fdNext = new FormData();
    fdNext.left = new FormAttachment(left, 3 * props.getMargin());
    fdNext.right = new FormAttachment(middle, -props.getMargin() * 2);
    fdNext.top = new FormAttachment(wSearch, props.getMargin() * 2);
    wNext.setLayoutData(fdNext);

    // A few lines of source code at the bottom...
    //
    Label wlSource = new Label(composite, SWT.RIGHT);
    wlSource.setText(BaseMessages.getString(PKG, "i18nDialog.LineOfSourceCode"));
    props.setLook(wlSource);
    FormData fdlSource = new FormData();
    fdlSource.left = new FormAttachment(left, props.getMargin());
    fdlSource.right = new FormAttachment(middle, 0);
    fdlSource.top = new FormAttachment(wClose, -300 - props.getMargin());
    wlSource.setLayoutData(fdlSource);

    wSource = new Text(composite, SWT.MULTI | SWT.BORDER | SWT.WRAP | SWT.V_SCROLL);
    props.setLook(wSource);
    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment(middle, props.getMargin());
    fdSource.right = new FormAttachment(100, 0);
    fdSource.top = new FormAttachment(wClose, -300 - props.getMargin());
    fdSource.bottom = new FormAttachment(wClose, -props.getMargin());
    wSource.setLayoutData(fdSource);
    wSource.setEditable(false);

    // The source file (java)
    //
    Label wlSourceFile = new Label(composite, SWT.RIGHT);
    wlSourceFile.setText(BaseMessages.getString(PKG, "i18nDialog.SourceFile"));
    props.setLook(wlSourceFile);
    FormData fdlSourceFile = new FormData();
    fdlSourceFile.left = new FormAttachment(left, props.getMargin());
    fdlSourceFile.right = new FormAttachment(middle, 0);
    fdlSourceFile.bottom = new FormAttachment(wSource, -2 * props.getMargin(), SWT.TOP);
    wlSourceFile.setLayoutData(fdlSourceFile);

    wSourceFile = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(wSourceFile);
    FormData fdSourceFile = new FormData();
    fdSourceFile.left = new FormAttachment(middle, props.getMargin());
    fdSourceFile.right = new FormAttachment(100, 0);
    fdSourceFile.top = new FormAttachment(wlSourceFile, 0, SWT.CENTER);
    wSourceFile.setLayoutData(fdSourceFile);
    wSourceFile.setEditable(false);

    // The bundle file (messages*.properties)
    //
    Label wlBundleFile = new Label(composite, SWT.RIGHT);
    wlBundleFile.setText(BaseMessages.getString(PKG, "i18nDialog.BundleFile"));
    props.setLook(wlBundleFile);
    FormData fdlBundleFile = new FormData();
    fdlBundleFile.left = new FormAttachment(left, props.getMargin());
    fdlBundleFile.right = new FormAttachment(middle, 0);
    fdlBundleFile.bottom = new FormAttachment(wSourceFile, -props.getMargin());
    wlBundleFile.setLayoutData(fdlBundleFile);

    wBundleFile = new Text(composite, SWT.SINGLE | SWT.BORDER);
    props.setLook(wBundleFile);
    FormData fdBundleFile = new FormData();
    fdBundleFile.left = new FormAttachment(middle, props.getMargin());
    fdBundleFile.right = new FormAttachment(100, 0);
    fdBundleFile.top = new FormAttachment(wlBundleFile, 0, SWT.CENTER);
    wBundleFile.setLayoutData(fdBundleFile);
    wBundleFile.setEditable(false);

    // The translation
    //
    Label wlValue = new Label(composite, SWT.RIGHT);
    wlValue.setText(BaseMessages.getString(PKG, "i18nDialog.Translation"));
    wlValue.setFont(GuiResource.getInstance().getFontBold());
    props.setLook(wlValue);
    FormData fdlValue = new FormData();
    fdlValue.left = new FormAttachment(left, props.getMargin());
    fdlValue.right = new FormAttachment(middle, 0);
    fdlValue.top = new FormAttachment(wMain, props.getMargin());
    wlValue.setLayoutData(fdlValue);

    wValue = new Text(composite, SWT.MULTI | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wValue);
    wValue.setFont(GuiResource.getInstance().getFontBold());
    FormData fdValue = new FormData();
    fdValue.left = new FormAttachment(middle, props.getMargin());
    fdValue.right = new FormAttachment(100, 0);
    fdValue.top = new FormAttachment(wMain, props.getMargin());
    fdValue.bottom = new FormAttachment(wBundleFile, -props.getMargin());
    wValue.setLayoutData(fdValue);
    wValue.setEditable(true);

    wApply = new Button(composite, SWT.PUSH);
    wApply.setText(BaseMessages.getString(PKG, "i18nDialog.Apply"));
    FormData fdApply = new FormData();
    fdApply.left = new FormAttachment(left, 3 * props.getMargin());
    fdApply.right = new FormAttachment(middle, -props.getMargin() * 2);
    fdApply.top = new FormAttachment(wValue, 0, SWT.CENTER);
    wApply.setLayoutData(fdApply);
    wApply.setEnabled(false);

    wRevert = new Button(composite, SWT.PUSH);
    wRevert.setText(BaseMessages.getString(PKG, "i18nDialog.Revert"));
    FormData fdRevert = new FormData();
    fdRevert.left = new FormAttachment(left, 3 * props.getMargin());
    fdRevert.right = new FormAttachment(middle, -props.getMargin() * 2);
    fdRevert.top = new FormAttachment(wApply, props.getMargin() * 2);
    wRevert.setLayoutData(fdRevert);
    wRevert.setEnabled(false);

    wSearchV = new Button(composite, SWT.PUSH);
    wSearchV.setText(BaseMessages.getString(PKG, "i18nDialog.Search"));
    FormData fdSearchV = new FormData();
    fdSearchV.left = new FormAttachment(left, 3 * props.getMargin());
    fdSearchV.right = new FormAttachment(middle, -props.getMargin() * 2);
    fdSearchV.top = new FormAttachment(wRevert, props.getMargin() * 4);
    wSearchV.setLayoutData(fdSearchV);

    wNextV = new Button(composite, SWT.PUSH);
    wNextV.setText(BaseMessages.getString(PKG, "i18nDialog.Next"));
    FormData fdNextV = new FormData();
    fdNextV.left = new FormAttachment(left, 3 * props.getMargin());
    fdNextV.right = new FormAttachment(middle, -props.getMargin() * 2);
    fdNextV.top = new FormAttachment(wSearchV, props.getMargin() * 2);
    wNextV.setLayoutData(fdNextV);

    wAll.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            refreshGrid();
          }
        });

    wTodo.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            // If someone clicks on the todo list, we set the appropriate values
            //
            if (wTodo.getSelectionCount() == 1) {

              String key = wTodo.getSelection()[0];

              showKeySelection(key);
            }
          }
        });

    wValue.addModifyListener(
        e -> {
          // The main value got changed...
          // Capture this automatically
          //
          lastValueChanged = true;
          lastValue = wValue.getText();

          wApply.setEnabled(true);
          wRevert.setEnabled(true);
        });

    wApply.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            applyChangedValue();
          }
        });

    wRevert.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            revertChangedValue();
          }
        });

    wSave.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent event) {
            saveFiles();
          }
        });

    wSearch.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            search(referenceLocale);
          }
        });

    wNext.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            searchAgain(referenceLocale);
          }
        });

    wSearchV.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            search(selectedLocale);
          }
        });

    wNextV.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            searchAgain(selectedLocale);
          }
        });
  }

  protected boolean saveFiles() {
    java.util.List<BundleFile> changedBundleFiles = store.getChangedBundleFiles();
    if (changedBundleFiles.size() > 0) {

      StringBuilder msg = new StringBuilder();
      for (BundleFile bundleFile : changedBundleFiles) {
        String filename = bundleFile.getFilename();
        msg.append(bundleFile.getFilename());
        if (!new File(filename).exists()) {
          msg.append(" (NEW!)");
        }
        msg.append(Const.CR);
      }

      EnterTextDialog dialog =
          new EnterTextDialog(
              shell,
              BaseMessages.getString(PKG, "i18nDialog.ChangedFiles"),
              BaseMessages.getString(PKG, "i18nDialog.ChangedMessagesFiles"),
              msg.toString());
      if (dialog.open() != null) {
        try {
          for (BundleFile bundleFile : changedBundleFiles) {
            bundleFile.write();
            log.logBasic(
                BaseMessages.getString(
                    PKG, "i18n.Log.SavedMessagesFile", bundleFile.getFilename()));
          }
        } catch (HopException e) {
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "i18n.UnexpectedError"),
              "There was an error saving the changed messages files:",
              e);
          return false;
        }
        return true;
      } else {
        return false;
      }

    } else {
      // Nothing was saved.
      // TODO: disable the button if nothing changed.
      return true;
    }
  }

  protected void saveFilesToZip() {
    if (saveFiles()) {
      java.util.List<BundleFile> bundleFiles = store.findBundleFiles(selectedLocale, null);
      if (bundleFiles.size() > 0) {

        StringBuilder msg = new StringBuilder();
        for (BundleFile bundleFile : bundleFiles) {
          // Find the main locale variation for this messages store...
          //
          BundleFile mainLocaleMessagesStore =
              store.findMainBundleFile(bundleFile.getPackageName());
          String filename = bundleFile.getFilename();
          msg.append(filename).append(Const.CR);
        }

        // Ask for the target filename if we're still here...
        // Keep this FileDialog as is
        //
        String zipFilename =
            BaseDialog.presentFileDialog(
                shell,
                new String[] {"*.zip", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "System.FileType.ZIPFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true);
        if (zipFilename != null) {
          try {
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFilename));
            byte[] buf = new byte[1024];
            for (BundleFile bundleFile : bundleFiles) {
              FileInputStream in = new FileInputStream(bundleFile.getFilename());
              out.putNextEntry(new ZipEntry(bundleFile.getFilename()));
              int len;
              while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
              }
              out.closeEntry();
              in.close();
            }
            out.close();
          } catch (Exception e) {
            new ErrorDialog(
                shell,
                BaseMessages.getString(PKG, "i18n.UnexpectedError"),
                "There was an error saving the changed messages files:",
                e);
          }
        }
      }
    }
  }

  protected void search(String searchLocale) {
    // Ask for the search string...
    //
    EnterStringDialog dialog =
        new EnterStringDialog(
            shell,
            Const.NVL(searchString, ""),
            BaseMessages.getString(PKG, "i18nDialog.SearchKey"),
            BaseMessages.getString(PKG, "i18nDialog.SearchLocale1")
                + " '"
                + Const.NVL(searchLocale, "")
                + "' "
                + BaseMessages.getString(PKG, "i18nDialog.SearchLocale2"));
    searchString = dialog.open();

    lastFoundKey = null;

    searchAgain(searchLocale);
  }

  protected void searchAgain(String searchLocale) {
    if (searchString != null) {

      // We want to search for key in the list here...
      // That means we'll enter a String to search for in the values
      //

      String upperSearchString = searchString.toUpperCase();

      boolean lastKeyFound = lastFoundKey == null;

      // Search through all the main locale messages stores for the selected
      // package
      //
      java.util.List<BundleFile> mainLocaleBundleFiles =
          store.findBundleFiles(searchLocale, selectedMessagesPackage);
      for (BundleFile bundleFile : mainLocaleBundleFiles) {
        for (String key : bundleFile.getContents().keySet()) {
          String value = bundleFile.getContents().get(key);
          String upperValue = value.toUpperCase();
          if (upperValue.indexOf(upperSearchString) >= 0) {
            // OK, we found a key worthy of our attention...
            //
            if (lastKeyFound) {
              int index = wTodo.indexOf(key);
              if (index >= 0) {
                lastFoundKey = key;
                wTodo.setSelection(index);
                showKeySelection(wTodo.getSelection()[0]);
                return;
              }
            }
            if (key.equals(lastFoundKey)) {
              lastKeyFound = true;
            }
          }
        }
      }
    }
  }

  protected void showKeySelection(String key) {

    wSelectedSourceFolder.setText(Const.NVL(selectedSourceFolder, ""));
    wSelectedPackage.setText(Const.NVL(selectedMessagesPackage, ""));
    wReferenceLocale.setText(Const.NVL(referenceLocale, ""));

    if (!key.equals(selectedKey)) {

      applyChangedValue();
    }

    if (selectedLocale != null && key != null && selectedMessagesPackage != null) {
      String mainValue = store.lookupKeyValue(referenceLocale, selectedMessagesPackage, key);
      String value = store.lookupKeyValue(selectedLocale, selectedMessagesPackage, key);
      KeyOccurrence keyOccurrence = crawler.getKeyOccurrence(key, selectedMessagesPackage);

      wKey.setText(key);
      wMain.setText(Const.NVL(mainValue, ""));
      wValue.setText(Const.NVL(value, ""));
      wSource.setText(Const.NVL(keyOccurrence.getSourceLine(), ""));

      String javaFilename = stripRootFolder(HopVfs.getFilename(keyOccurrence.getFileObject()));
      wSourceFile.setText(Const.NVL(javaFilename, ""));

      String bundleFilename = "";
      BundleFile file =
          store.getBundleStore().findBundleFile(selectedMessagesPackage, selectedLocale);
      if (file != null) {
        bundleFilename = stripRootFolder(file.getFilename());
      }
      wBundleFile.setText(Const.NVL(bundleFilename, ""));

      // Focus on the entry field
      // Put the cursor all the way at the back
      //
      wValue.setFocus();
      wValue.setSelection(wValue.getText().length());
      wValue.showSelection();
      wValue.clearSelection();

      selectedKey = key;
      lastValueChanged = false;
      wApply.setEnabled(false);
      wRevert.setEnabled(false);
    }
  }

  private String stripRootFolder(String filename) {
    if (filename != null && filename.startsWith(rootFolder)) {
      return filename.substring(rootFolder.length());
    }
    return filename;
  }

  public void refreshGrid() {

    applyChangedValue();

    wTodo.removeAll();
    wKey.setText("");
    wMain.setText("");
    wValue.setText("");
    wSource.setText("");

    selectedLocale = wLocale.getSelectionCount() == 0 ? null : wLocale.getSelection()[0];
    selectedSourceFolder =
        wPackages.table.getSelectionCount() == 0
            ? null
            : wPackages.table.getSelection()[0].getText(1);
    selectedMessagesPackage =
        wPackages.table.getSelectionCount() == 0
            ? null
            : wPackages.table.getSelection()[0].getText(2);
    refreshPackages();

    // Only continue with a locale & a messages package, otherwise we won't
    // budge ;-)
    //
    if (selectedLocale != null && selectedSourceFolder != null && selectedMessagesPackage != null) {
      // Get the list of keys that need a translation...
      //
      java.util.List<KeyOccurrence> todo =
          getTodoList(selectedLocale, selectedMessagesPackage, selectedSourceFolder, false);
      String[] todoItems = new String[todo.size()];
      for (int i = 0; i < todoItems.length; i++) {
        todoItems[i] = todo.get(i).getKey();
      }
      wTodo.setItems(todoItems);
    }
  }

  private java.util.List<KeyOccurrence> getTodoList(
      String locale, String messagesPackage, String sourceFolder, boolean strict) {
    // Get the list of keys that need a translation...
    //
    java.util.List<KeyOccurrence> keys = crawler.getOccurrencesForPackage(messagesPackage);
    java.util.List<KeyOccurrence> todo = new ArrayList<>();
    for (KeyOccurrence keyOccurrence : keys) {
      // Avoid the System keys. Those are taken care off in a different package
      //
      if (showKey(keyOccurrence.getKey(), keyOccurrence.getMessagesPackage())) {
        String value = store.lookupKeyValue(locale, messagesPackage, keyOccurrence.getKey());
        if (Utils.isEmpty(value) || (wAll.getSelection() && !strict)) {
          todo.add(keyOccurrence);
        }
      }
    }

    return todo;
  }

  private void applyChangedValue() {
    // Hang on, before we clear it all, did we have a previous value?
    //
    int todoIndex = wTodo.getSelectionIndex();

    if (selectedKey != null
        && selectedLocale != null
        && selectedMessagesPackage != null
        && lastValueChanged
        && selectedSourceFolder != null) {
      // Store the last modified value
      //
      if (!Utils.isEmpty(lastValue)) {
        store.storeValue(
            selectedLocale, selectedSourceFolder, selectedMessagesPackage, selectedKey, lastValue);
        lastValueChanged = false;

        if (!wAll.getSelection()) {
          wTodo.remove(selectedKey);
          if (wTodo.getSelectionIndex() < 0) {
            // Select the next one in the list...
            if (todoIndex > wTodo.getItemCount()) {
              todoIndex = wTodo.getItemCount() - 1;
            }

            if (todoIndex >= 0 && todoIndex < wTodo.getItemCount()) {
              wTodo.setSelection(todoIndex);
              showKeySelection(wTodo.getSelection()[0]);
            } else {
              refreshGrid();
            }
          } else {
            showKeySelection(wTodo.getSelection()[0]);
          }
        }
      }
      lastValue = null;
      wApply.setEnabled(false);
      wRevert.setEnabled(false);
    }
  }

  private void revertChangedValue() {
    lastValueChanged = false;
    refreshGrid();
  }

  public void refresh() {
    refreshLocale();
    refreshPackages();
    refreshGrid();
  }

  public void refreshPackages() {
    int index = wPackages.getSelectionIndex();

    // OK, we have a distinct list of packages to work with...
    wPackages.table.removeAll();

    Map<String, Map<String, java.util.List<KeyOccurrence>>> sourceMessagesPackages =
        crawler.getSourcePackageOccurrences();

    // Sort the source folders...
    //
    java.util.List<String> sourceFolders = new ArrayList<>(sourceMessagesPackages.keySet());
    Collections.sort(sourceFolders);
    for (String sourceFolder : sourceFolders) {
      Map<String, java.util.List<KeyOccurrence>> messagesPackages =
          sourceMessagesPackages.get(sourceFolder);
      java.util.List<String> packageNames = new ArrayList<>(messagesPackages.keySet());
      Collections.sort(packageNames);

      for (String packageName : packageNames) {

        TableItem item = new TableItem(wPackages.table, SWT.NONE);
        item.setText(1, sourceFolder);
        item.setText(2, packageName);

        // count the number of keys for the package that are NOT yet translated...
        //
        if (selectedLocale != null) {

          // Check if there is a bundle file for this package.
          // If not we'll paint it in light red
          //
          BundleFile bundleFile =
              store.getBundleStore().findBundleFile(packageName, selectedLocale);
          if (bundleFile == null) {
            item.setBackground(new Color(shell.getDisplay(), 230, 150, 150));
          } else {
            java.util.List<KeyOccurrence> todo =
                getTodoList(selectedLocale, packageName, sourceFolder, true);
            if (todo.size() > 50) {
              item.setBackground(
                  new Color(shell.getDisplay(), props.contrastColor(150, 150, 150))); // dark gray
            } else if (todo.size() > 25) {
              item.setBackground(new Color(shell.getDisplay(), props.contrastColor(170, 170, 170)));
            } else if (todo.size() > 10) {
              item.setBackground(new Color(shell.getDisplay(), props.contrastColor(190, 190, 190)));
            } else if (todo.size() > 5) {
              item.setBackground(new Color(shell.getDisplay(), props.contrastColor(210, 210, 210)));
            } else if (todo.size() > 0) {
              item.setBackground(
                  new Color(shell.getDisplay(), props.contrastColor(230, 230, 230))); // light gray
            }
          }
        }
      }
      if (messagesPackages.size() == 0) {
        new TableItem(wPackages.table, SWT.NONE);
      } else {
        wPackages.setRowNums();
      }
    }

    if (index >= 0) {
      wPackages.table.setSelection(index);
      wPackages.table.showSelection();
    }
  }

  public void refreshLocale() {
    // OK, we have a distinct list of locale to work with...
    wLocale.removeAll();
    wLocale.setItems(localeList.toArray(new String[localeList.size()]));
  }

  public String toString() {
    return APP_NAME;
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("Usage: Translator <translator.xml> <path-to-source>");
      System.err.println("Example:");
      System.err.println("sh hop-translator.sh translator.xml /home/john/git/hop/");
      System.exit(1);
    }

    HopClientEnvironment.init();

    String configFile = args[0];
    String sourceFolder = args[1];

    Display display = new Display();
    ILogChannel log = new LogChannel(APP_NAME);

    HopNamespace.setNamespace(TRANSLATOR_NAMESPACE);

    Translator translator = new Translator(display);
    translator.loadConfiguration(configFile, sourceFolder);
    translator.open();

    try {
      while (!display.isDisposed()) {
        if (!display.readAndDispatch()) {
          display.sleep();
        }
      }
    } catch (Throwable e) {
      log.logError(BaseMessages.getString(PKG, "i18n.UnexpectedError", e.getMessage()));
      log.logError(Const.getStackTracker(e));
    }
  }
}
