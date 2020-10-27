/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.metainject;

import com.sun.corba.se.spi.ior.ObjectId;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class MetaInjectDialog extends BaseTransformDialog implements ITransformDialog {

  public static final String CONST_VALUE = "<const>";
  private static final Class<?> PKG = MetaInjectMeta.class; // for i18n purposes, needed by Translator2!!

  private final MetaInjectMeta metaInjectMeta;

  private TextVar wPath;

  private CTabFolder wTabFolder;

  private PipelineMeta injectPipelineMeta = null;

  protected boolean transModified;

  private ModifyListener lsMod;

  private ObjectId referenceObjectId;
  private ObjectLocationSpecificationMethod specificationMethod;

  // the source transform
  //
  private CCombo wSourceTransform;

  // The source transform output fields...
  //
  private TableView wSourceFields;

  // the target file
  //
  private TextVar wTargetFile;

  // don't execute the transformation
  //
  private Button wNoExecution;

  private CCombo wStreamingSourceTransform;

  // the streaming target transform
  //
  private Label wlStreamingTargetTransform;
  private CCombo wStreamingTargetTransform;

  // The tree object to show the options...
  //
  private Tree wTree;

  private Map<TreeItem, TargetTransformAttribute> treeItemTargetMap;

  private final Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping;

  public MetaInjectDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    metaInjectMeta = (MetaInjectMeta) in;
    transModified = false;

    targetSourceMapping = new HashMap<>();
    targetSourceMapping.putAll( metaInjectMeta.getTargetSourceMapping() );
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, metaInjectMeta );

    lsMod = e -> metaInjectMeta.setChanged();
    changed = metaInjectMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MetaInjectDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    positionBottomButtons( shell, new Button[] { wOk, wCancel, }, props.getMargin(), null );

    // Transform Name line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MetaInjectDialog.TransformName.Label") );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, 0 );
    wlTransformName.setLayoutData( fdlTransformName );

    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.right = new FormAttachment( 90, 0);
    fdTransformName.left = new FormAttachment( 0, 0 );
    fdTransformName.top = new FormAttachment( wlTransformName, 5 );
    wTransformName.setLayoutData( fdTransformName );

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wTransformName, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    spacer.setLayoutData( fdSpacer );

    Label wlPath = new Label(shell, SWT.LEFT);
    props.setLook(wlPath);
    wlPath.setText( BaseMessages.getString( PKG, "MetaInjectDialog.Transformation.Label" ) );
    FormData fdlTransformation = new FormData();
    fdlTransformation.left = new FormAttachment( 0, 0 );
    fdlTransformation.top = new FormAttachment( spacer, 20 );
    fdlTransformation.right = new FormAttachment( 100, 0 );
    wlPath.setLayoutData( fdlTransformation );

    Button wbBrowse = new Button(shell, SWT.PUSH);
    props.setLook(wbBrowse);
    wbBrowse.setText( BaseMessages.getString( PKG, "MetaInjectDialog.Browse.Label" ) );
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment( 100, -props.getMargin() );
    fdBrowse.top = new FormAttachment(wlPath, Const.isOSX() ? 0 : 5 );
    wbBrowse.setLayoutData( fdBrowse );

    wPath = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPath );
    FormData fdTransformation = new FormData();
    fdTransformation.left = new FormAttachment( 0, 0 );
    fdTransformation.top = new FormAttachment(wlPath, 5 );
    fdTransformation.right = new FormAttachment( wbBrowse, -props.getMargin());
    wPath.setLayoutData( fdTransformation );
    wPath.addFocusListener( new FocusAdapter() {
      @Override public void focusLost( FocusEvent focusEvent ) {
        refreshTree();
      }
    } );


    wbBrowse.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        selectFileTrans( true );
        refreshTree();
      }
    } );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );




    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment( 0, 0 );
    fdhSpacer.bottom = new FormAttachment( wCancel, -15 );
    fdhSpacer.right = new FormAttachment( 100, 0 );
    hSpacer.setLayoutData( fdhSpacer );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wPath, 20 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( hSpacer, -15 );
    wTabFolder.setLayoutData( fdTabFolder );

    addInjectTab();
    addOptionsTab();

    // Add listeners
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wPath.addSelectionListener( lsDef );
    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    metaInjectMeta.setChanged( changed );

    shell.open();

    checkInvalidMapping();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private Image getImage() {
    return SwtSvgImageUtil
      .getImage( shell.getDisplay(), getClass().getClassLoader(), "GenericTransform.svg", ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE );
  }

  private void checkInvalidMapping() {
    if ( injectPipelineMeta == null ) {
      try {
        if ( !loadPipeline() ) {
          return;
        }
      } catch ( HopException e ) {
        showErrorOnLoadTransformationDialog( e );
        return;
      }
    }
    Set<SourceTransformField> unavailableSourceTransforms =
        MetaInject.getUnavailableSourceTransforms( targetSourceMapping, pipelineMeta, transformMeta );
    Set<TargetTransformAttribute> unavailableTargetTransforms =
        MetaInject.getUnavailableTargetTransforms( targetSourceMapping, injectPipelineMeta);
    Set<TargetTransformAttribute> missingTargetKeys =
        MetaInject.getUnavailableTargetKeys( targetSourceMapping, injectPipelineMeta, unavailableTargetTransforms );
    if ( unavailableSourceTransforms.isEmpty() && unavailableTargetTransforms.isEmpty() && missingTargetKeys.isEmpty() ) {
      return;
    }
    showInvalidMappingDialog( unavailableSourceTransforms, unavailableTargetTransforms, missingTargetKeys );
  }

  private void showInvalidMappingDialog(Set<SourceTransformField> unavailableSourceTransforms,
                                        Set<TargetTransformAttribute> unavailableTargetTransforms, Set<TargetTransformAttribute> missingTargetKeys ) {
    MessageBox mb = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION );
    mb.setMessage( BaseMessages.getString( PKG, "MetaInjectDialog.InvalidMapping.Question" ) );
    mb.setText( BaseMessages.getString( PKG, "MetaInjectDialog.InvalidMapping.Title" ) );
    int id = mb.open();
    if ( id == SWT.YES ) {
      MetaInject.removeUnavailableTransformsFromMapping( targetSourceMapping, unavailableSourceTransforms,
          unavailableTargetTransforms );
      for ( TargetTransformAttribute target : missingTargetKeys ) {
        targetSourceMapping.remove( target );
      }
    }
  }

  private void showErrorOnLoadTransformationDialog( HopException e ) {
    new ErrorDialog( shell, BaseMessages.getString( PKG, "MetaInjectDialog.ErrorLoadingSpecifiedTransformation.Title" ),
        BaseMessages.getString( PKG, "MetaInjectDialog.ErrorLoadingSpecifiedTransformation.Message" ), e );
  }

  private void addOptionsTab() {
    // ////////////////////////
    // START OF OPTIONS TAB ///
    // ////////////////////////

    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setText( BaseMessages.getString( PKG, "MetaInjectDialog.OptionsTab.TabTitle" ) );

    ScrolledComposite wOptionsSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wOptionsSComp.setLayout( new FillLayout() );

    Composite wOptionsComp = new Composite(wOptionsSComp, SWT.NONE);
    props.setLook(wOptionsComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 15;
    fileLayout.marginHeight = 15;
    wOptionsComp.setLayout( fileLayout );

    Label wlSourceTransform = new Label(wOptionsComp, SWT.RIGHT );
    wlSourceTransform.setText( BaseMessages.getString( PKG, "MetaInjectDialog.SourceTransform.Label") );
    props.setLook( wlSourceTransform );
    FormData fdlSourceTransform = new FormData();
    fdlSourceTransform.left = new FormAttachment( 0, 0 );
    fdlSourceTransform.top = new FormAttachment( 0, 0 );
    wlSourceTransform.setLayoutData( fdlSourceTransform );

    wSourceTransform = new CCombo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(wSourceTransform);
    wSourceTransform.addModifyListener( lsMod );
    FormData fdSourceTransform = new FormData();
    fdSourceTransform.right = new FormAttachment(100, 0);
    fdSourceTransform.left = new FormAttachment( 0, 0 );
    fdSourceTransform.top = new FormAttachment( wlSourceTransform, 5 );
    wSourceTransform.setLayoutData( fdSourceTransform );
    wSourceTransform.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        setActive();
      }
    } );

    final int fieldRows = metaInjectMeta.getSourceOutputFields().size();

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.ColumnInfo.Fieldname" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.ColumnInfo.Type" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMetaFactory.getAllValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.ColumnInfo.Length" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.ColumnInfo.Precision" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ), };

    wSourceFields = new TableView( pipelineMeta, wOptionsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldRows, false, lsMod, props, false );
    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment(wSourceTransform, 10 );
    fdFields.bottom = new FormAttachment( 50, 0);
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    wSourceFields.setLayoutData( fdFields );
    wSourceFields.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 25, 25, 25, 25 ) );

    Label wlTargetFile = new Label(wOptionsComp, SWT.RIGHT );
    wlTargetFile.setText( BaseMessages.getString( PKG, "MetaInjectDialog.TargetFile.Label" ) );
    props.setLook( wlTargetFile );
    FormData fdlTargetFile = new FormData();
    fdlTargetFile.left = new FormAttachment( 0, 0 );
    fdlTargetFile.top = new FormAttachment( wSourceFields, 10 );
    wlTargetFile.setLayoutData( fdlTargetFile );

    wTargetFile = new TextVar( pipelineMeta, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetFile );
    wTargetFile.addModifyListener( lsMod );
    FormData fdTargetFile = new FormData();
    fdTargetFile.right = new FormAttachment(100, 0);
    fdTargetFile.left = new FormAttachment( 0, 0 );
    fdTargetFile.top = new FormAttachment( wlTargetFile, 5 );
    wTargetFile.setLayoutData( fdTargetFile );

    // the streaming source transform
    //
    Label wlStreamingSourceTransform = new Label(wOptionsComp, SWT.RIGHT);
    wlStreamingSourceTransform.setText( BaseMessages.getString( PKG, "MetaInjectDialog.StreamingSourceTransform.Label") );
    props.setLook(wlStreamingSourceTransform);
    FormData fdlStreamingSourceTransform = new FormData();
    fdlStreamingSourceTransform.left = new FormAttachment( 0, 0 );
    fdlStreamingSourceTransform.top = new FormAttachment( wTargetFile, 10 );
    wlStreamingSourceTransform.setLayoutData( fdlStreamingSourceTransform );

    wStreamingSourceTransform = new CCombo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(wStreamingSourceTransform);
    FormData fdStreamingSourceTransform = new FormData();
    fdStreamingSourceTransform.right = new FormAttachment(100, 0);
    fdStreamingSourceTransform.left = new FormAttachment( 0, 0 );
    fdStreamingSourceTransform.top = new FormAttachment(wlStreamingSourceTransform, 5 );
    wStreamingSourceTransform.setLayoutData( fdStreamingSourceTransform );
    wStreamingSourceTransform.setItems( pipelineMeta.getTransformNames() );
    wStreamingSourceTransform.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        setActive();
      }
    } );

    wlStreamingTargetTransform = new Label(wOptionsComp, SWT.RIGHT );
    wlStreamingTargetTransform.setText( BaseMessages.getString( PKG, "MetaInjectDialog.StreamingTargetTransform.Label") );
    props.setLook(wlStreamingTargetTransform);
    FormData fdlStreamingTargetTransform = new FormData();
    fdlStreamingTargetTransform.left = new FormAttachment( 0, 0 );
    fdlStreamingTargetTransform.top = new FormAttachment(wStreamingSourceTransform, 10 );
    wlStreamingTargetTransform.setLayoutData( fdlStreamingTargetTransform );

    wStreamingTargetTransform = new CCombo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(wStreamingTargetTransform);
    FormData fdStreamingTargetTransform = new FormData();
    fdStreamingTargetTransform.right = new FormAttachment(100, 0);
    fdStreamingTargetTransform.left = new FormAttachment( 0, 0 );
    fdStreamingTargetTransform.top = new FormAttachment(wlStreamingTargetTransform, 5 );
    wStreamingTargetTransform.setLayoutData( fdStreamingTargetTransform );

    wNoExecution = new Button(wOptionsComp, SWT.CHECK );
    wNoExecution.setText( BaseMessages.getString( PKG, "MetaInjectDialog.NoExecution.Label" ) );
    props.setLook( wNoExecution );
    FormData fdNoExecution = new FormData();
    fdNoExecution.width = 350;
    fdNoExecution.left = new FormAttachment( 0, 0 );
    fdNoExecution.top = new FormAttachment(wStreamingTargetTransform, 10 );
    wNoExecution.setLayoutData( fdNoExecution );

    FormData fdOptionsComp = new FormData();
    fdOptionsComp.left = new FormAttachment( 0, 0 );
    fdOptionsComp.top = new FormAttachment( 0, 0 );
    fdOptionsComp.right = new FormAttachment( 100, 0 );
    fdOptionsComp.bottom = new FormAttachment( 100, 0 );
    wOptionsComp.setLayoutData( fdOptionsComp );

    wOptionsComp.pack();
    Rectangle bounds = wOptionsComp.getBounds();

    wOptionsSComp.setContent(wOptionsComp);
    wOptionsSComp.setExpandHorizontal( true );
    wOptionsSComp.setExpandVertical( true );
    wOptionsSComp.setMinWidth( bounds.width );
    wOptionsSComp.setMinHeight( bounds.height );

    wOptionsTab.setControl(wOptionsSComp);

    // ///////////////////////////////////////////////////////////
    // / END OF OPTIONS TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addInjectTab() {
    // ////////////////////////
    // START OF INJECT TAB ///
    // ////////////////////////

    CTabItem wInjectTab = new CTabItem(wTabFolder, SWT.NONE);
    wInjectTab.setText( BaseMessages.getString( PKG, "MetaInjectDialog.InjectTab.TabTitle" ) );

    ScrolledComposite wInjectSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wInjectSComp.setLayout( new FillLayout() );

    Composite wInjectComp = new Composite(wInjectSComp, SWT.NONE);
    props.setLook(wInjectComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 15;
    fileLayout.marginHeight = 15;
    wInjectComp.setLayout( fileLayout );

    wTree = new Tree(wInjectComp, SWT.SINGLE | SWT.FULL_SELECTION | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER );
    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment( 0, 0 );
    fdTree.top = new FormAttachment( 0, 0 );
    fdTree.right = new FormAttachment( 100, 0 );
    fdTree.bottom = new FormAttachment( 100, 0 );
    wTree.setLayoutData( fdTree );

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.Column.TargetTransform"), ColumnInfo.COLUMN_TYPE_TEXT,
          false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.Column.TargetDescription" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.Column.SourceTransform"),
          ColumnInfo.COLUMN_TYPE_CCOMBO, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MetaInjectDialog.Column.SourceField" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, false, true ), };

    wTree.setHeaderVisible( true );
    for (ColumnInfo columnInfo : colinf) {
      TreeColumn treeColumn = new TreeColumn(wTree, columnInfo.getAlignment());
      treeColumn.setText(columnInfo.getName());
      treeColumn.setWidth(200);
    }

    wTree.addListener( SWT.MouseDown, event -> {
      try {
        Point point = new Point( event.x, event.y );
        TreeItem item = wTree.getItem( point );
        if ( item != null ) {
          TargetTransformAttribute target = treeItemTargetMap.get( item );
          if ( target != null ) {
            SourceTransformField source = targetSourceMapping.get( target );

            String[] prevTransformNames = pipelineMeta.getPrevTransformNames( transformMeta );
            Arrays.sort( prevTransformNames );

            Map<String, SourceTransformField> fieldMap = new HashMap<>();
            for ( String prevTransformName : prevTransformNames ) {
              IRowMeta fields = pipelineMeta.getTransformFields( prevTransformName );
              for ( IValueMeta field : fields.getValueMetaList() ) {
                String key = buildTransformFieldKey( prevTransformName, field.getName() );
                fieldMap.put( key, new SourceTransformField( prevTransformName, field.getName() ) );
              }
            }
            String[] sourceFields = fieldMap.keySet().toArray( new String[fieldMap.size()] );
            Arrays.sort( sourceFields );

            String constant = source != null && source.getTransformName() == null ? source.getField() : "";
            EnterSelectionDialog selectSourceField = new EnterSelectionDialog( shell, sourceFields,
              BaseMessages.getString( PKG, "MetaInjectDialog.SourceFieldDialog.Title" ),
              BaseMessages.getString( PKG, "MetaInjectDialog.SourceFieldDialog.Label" ), constant, pipelineMeta );
            if ( source != null ) {
              if ( source.getTransformName() != null && !Utils.isEmpty( source.getTransformName() ) ) {
                String key = buildTransformFieldKey( source.getTransformName(), source.getField() );
                selectSourceField.setCurrentValue( key );
                int index = Const.indexOfString( key, sourceFields );
                if ( index >= 0 ) {
                  selectSourceField.setSelectedNrs( new int[] { index, } );
                }
              } else {
                selectSourceField.setCurrentValue( source.getField() );
              }
            }
            String selectedTransformField = selectSourceField.open();
            if ( selectedTransformField != null ) {
              SourceTransformField newSource = fieldMap.get( selectedTransformField );
              if ( newSource == null ) {
                newSource = new SourceTransformField( null, selectedTransformField );
                item.setText( 2, CONST_VALUE );
                item.setText( 3, selectedTransformField );
              } else {
                item.setText( 2, newSource.getTransformName() );
                item.setText( 3, newSource.getField() );
              }
              targetSourceMapping.put( target, newSource );
            } else {
              item.setText( 2, "" );
              item.setText( 3, "" );
              targetSourceMapping.remove( target );
            }

          }

        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, "Oops", "Unexpected Error", e );
      }
    });

    FormData fdInjectComp = new FormData();
    fdInjectComp.left = new FormAttachment( 0, 0 );
    fdInjectComp.top = new FormAttachment( 0, 0 );
    fdInjectComp.right = new FormAttachment( 100, 0 );
    fdInjectComp.bottom = new FormAttachment( 100, 0 );
    wInjectComp.setLayoutData( fdInjectComp );

    wInjectComp.pack();
    Rectangle bounds = wInjectComp.getBounds();

    wInjectSComp.setContent(wInjectComp);
    wInjectSComp.setExpandHorizontal( true );
    wInjectSComp.setExpandVertical( true );
    wInjectSComp.setMinWidth( bounds.width );
    wInjectSComp.setMinHeight( bounds.height );

    wInjectTab.setControl(wInjectSComp);

    // ///////////////////////////////////////////////////////////
    // / END OF INJECT TAB
    // ///////////////////////////////////////////////////////////
  }

  private void selectFileTrans( boolean useVfs ) {
    String curFile = pipelineMeta.environmentSubstitute( wPath.getText() );

    if ( useVfs ) {
      FileObject root = null;

      String parentFolder = null;
      try {
        parentFolder = HopVfs.getFileObject( pipelineMeta.environmentSubstitute( pipelineMeta.getFilename() ) ).getParent().toString();
      } catch ( Exception e ) {
        // Take no action
      }

      try {
        HopPipelineFileType<PipelineMeta> fileType = HopGui.getDataOrchestrationPerspective().getPipelineFileType();
        String filename = BaseDialog.presentFileDialog( shell, wPath, pipelineMeta, fileType.getFilterExtensions(), fileType.getFilterNames(), true );
        if ( filename != null ) {
          loadPipelineFile( filename );
//          if ( parentFolder != null && filename.startsWith( parentFolder ) ) {
//            filename = filename.replace( parentFolder, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}" );
//          }
          wPath.setText( filename );
        }
      } catch ( HopException e ) {
        new ErrorDialog( shell,
                BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingPipeline.DialogTitle" ),
                BaseMessages.getString( PKG, "PipelineExecutorDialog.ErrorLoadingPipeline.DialogMessage" ), e );
      }
/*
      try {
       root = HopVfs.getFileObject( curFile != null ? curFile : Const.getUserHomeDirectory() );

        VfsFileChooserDialog vfsFileChooser = Spoon.getInstance().getVfsFileChooserDialog( root.getParent(), root );
        FileObject file =
          vfsFileChooser.open(
            shell, null, Const.STRING_TRANS_FILTER_EXT, Const.getPipelineFilterNames(),
            VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE );
        if ( file == null ) {
          return;
        }
        String fileName = file.getName().toString();
        if ( fileName != null ) {
          loadFileTrans( fileName );
          if ( parentFolder != null && fileName.startsWith( parentFolder ) ) {
            fileName = fileName.replace( parentFolder, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}" );
          }
          wPath.setText( fileName );
          specificationMethod = ObjectLocationSpecificationMethod.FILENAME;
        }
      } catch ( IOException | HopException e ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingTransformation.DialogTitle" ),
          BaseMessages.getString( PKG, "SingleThreaderDialog.ErrorLoadingTransformation.DialogMessage" ), e );
      }
*/
    }
  }

  private void loadPipelineFile(String fname ) throws HopException {
    String filename = pipelineMeta.environmentSubstitute( fname );
    injectPipelineMeta = new PipelineMeta( filename, metadataProvider, true, pipelineMeta );
    injectPipelineMeta.clearChanged();
  }

  private boolean loadPipeline() throws HopException {
    String filename = wPath.getText();
    specificationMethod = ObjectLocationSpecificationMethod.FILENAME;
    switch ( specificationMethod ) {
      case FILENAME:
        if ( Utils.isEmpty( filename ) ) {
          return false;
        }
        if ( !filename.endsWith( ".hpl" ) ) {
          filename = filename + ".hpl";
          wPath.setText( filename );
        }
        loadPipelineFile( filename );
        break;
      default:
        break;
    }
    return true;
  }

  public void setActive() {
    boolean outputCapture = !Utils.isEmpty( wSourceTransform.getText() );
    wSourceFields.setEnabled( outputCapture );

    boolean streaming = !Utils.isEmpty( wStreamingSourceTransform.getText() );
    wStreamingTargetTransform.setEnabled( streaming );
    wlStreamingTargetTransform.setEnabled( streaming );
  }

/*
  private void getByReferenceData( RepositoryElementMetaInterface transInf  ) {
    String path =
      DialogUtils.getPath( pipelineMeta.getRepositoryDirectory().getPath(), transInf.getRepositoryDirectory().getPath() );
    String fullPath =
      Const.NVL( path, "" ) + "/" + Const.NVL( transInf.getName(), "" );
    wPath.setText( fullPath );
  }
*/

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    specificationMethod = metaInjectMeta.getSpecificationMethod();
    switch ( specificationMethod ) {
      case FILENAME:
        wPath.setText( Const.NVL( metaInjectMeta.getFileName(), "" ) );
        break;
      default:
        break;
    }

    wSourceTransform.setText( Const.NVL( metaInjectMeta.getSourceTransformName(), "" ) );
    int rownr = 0;
    for ( MetaInjectOutputField field : metaInjectMeta.getSourceOutputFields() ) {
      int colnr = 1;
      wSourceFields.setText( field.getName(), colnr++, rownr );
      wSourceFields.setText( field.getTypeDescription(), colnr++, rownr );
      wSourceFields.setText( field.getLength() < 0 ? "" : Integer.toString( field.getLength() ), colnr++, rownr );
      wSourceFields.setText( field.getPrecision() < 0 ? "" : Integer.toString( field.getPrecision() ), colnr++, rownr );
      rownr++;
    }

    wTargetFile.setText( Const.NVL( metaInjectMeta.getTargetFile(), "" ) );
    wNoExecution.setSelection( !metaInjectMeta.isNoExecution() );

    wStreamingSourceTransform.setText( Const.NVL( metaInjectMeta.getStreamSourceTransform() == null ? null : metaInjectMeta.getStreamSourceTransform().getName(), "" ) );
    wStreamingTargetTransform.setText( Const.NVL( metaInjectMeta.getStreamTargetTransformName(), "" ) );

    setActive();
    refreshTree();

    wTabFolder.setSelection( 0 );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  protected String buildTransformFieldKey(String transformName, String field ) {
    return transformName + " : " + field;
  }

  private void refreshTree() {
    try {
      loadPipeline();

      treeItemTargetMap = new HashMap<>();

      wTree.removeAll();

      TreeItem transItem = new TreeItem( wTree, SWT.NONE );
      transItem.setExpanded( true );
      transItem.setText( injectPipelineMeta.getName() );
      List<TransformMeta> injectTransforms = new ArrayList<>();
      for ( TransformMeta transformMeta : injectPipelineMeta.getUsedTransforms() ) {
        ITransformMeta meta = transformMeta.getTransform();
        if ( BeanInjectionInfo.isInjectionSupported( meta.getClass() ) ) {
          injectTransforms.add( transformMeta );
        }
      }
      Collections.sort( injectTransforms );

      for ( TransformMeta transformMeta : injectTransforms ) {
        TreeItem transformItem = new TreeItem( transItem, SWT.NONE );
        transformItem.setText( transformMeta.getName() );
        transformItem.setExpanded( true );

        // For each transform, add the keys
        //
        ITransformMeta metaInterface = transformMeta.getTransform();
        if ( BeanInjectionInfo.isInjectionSupported( metaInterface.getClass() ) ) {
          processNewMDIDescription( transformMeta, transformItem, metaInterface );
//        } else {
//          processOldMDIDescription( transformMeta, transformItem, metaInterface.getTransformMetaInjectionInterface() );
        }
      }

    } catch ( Throwable t ) {
      // Ignore errors
    }

    for ( TreeItem item : wTree.getItems() ) {
      expandItemAndChildren( item );
    }

    // Also set the source transform combo values
    //
    if ( injectPipelineMeta != null ) {
      String[] sourceTransforms = injectPipelineMeta.getTransformNames();
      Arrays.sort( sourceTransforms );
      wSourceTransform.setItems( sourceTransforms );
      wStreamingTargetTransform.setItems( sourceTransforms );
    }
  }


  private void processNewMDIDescription( TransformMeta transformMeta, TreeItem transformItem, ITransformMeta metaInterface ) {
    BeanInjectionInfo transformInjectionInfo = new BeanInjectionInfo( metaInterface.getClass() );

    List<BeanInjectionInfo.Group> groupsList = transformInjectionInfo.getGroups();

    for ( BeanInjectionInfo.Group gr : groupsList ) {
      boolean rootGroup = StringUtils.isEmpty( gr.getName() );
      TreeItem groupItem;
      if ( !rootGroup ) {
        groupItem = new TreeItem( transformItem, SWT.NONE );
        groupItem.setText( gr.getName() );
        groupItem.setText( 1, gr.getDescription() );
      } else {
        groupItem = null;
      }

      List<BeanInjectionInfo.Property> propertyList = gr.getGroupProperties();
      for ( BeanInjectionInfo.Property property : propertyList ) {
        TreeItem treeItem = new TreeItem( rootGroup ? transformItem : groupItem, SWT.NONE );
        treeItem.setText( property.getName() );
        treeItem.setText( 1, property.getDescription() );

        TargetTransformAttribute target = new TargetTransformAttribute( transformMeta.getName(), property.getName(), !rootGroup );
        treeItemTargetMap.put( treeItem, target );

        SourceTransformField source = targetSourceMapping.get( target );
        if ( source != null ) {
          treeItem.setText( 2, Const.NVL( source.getTransformName() == null ? CONST_VALUE : source.getTransformName(), "" ) );
          treeItem.setText( 3, Const.NVL( source.getField(), "" ) );
        }
      }
    }
  }

  private void expandItemAndChildren( TreeItem item ) {
    item.setExpanded( true );
    for ( TreeItem item2 : item.getItems() ) {
      expandItemAndChildren( item2 );
    }

  }

  private void cancel() {
    transformName = null;
    metaInjectMeta.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    try {
      loadPipeline();
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "MetaInjectDialog.ErrorLoadingSpecifiedTransformation.Title" ),
        BaseMessages.getString( PKG, "MetaInjectDialog.ErrorLoadingSpecifiedTransformation.Message" ), e );
    }

//    if ( repository != null ) {
//      specificationMethod = ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME;
//    } else {
      specificationMethod = ObjectLocationSpecificationMethod.FILENAME;
//    }
    metaInjectMeta.setSpecificationMethod( specificationMethod );
    switch ( specificationMethod ) {
      case FILENAME:
        metaInjectMeta.setFileName( wPath.getText() );
        metaInjectMeta.setDirectoryPath( null );
        metaInjectMeta.setPipelineName( null );
//        metaInjectMeta.setTransObjectId( null );
        break;
//      case REPOSITORY_BY_NAME:
//        String transPath = wPath.getText();
//        String transName = transPath;
//        String directory = "";
//        int index = transPath.lastIndexOf( "/" );
//        if ( index != -1 ) {
//          transName = transPath.substring( index + 1 );
//          directory = transPath.substring( 0, index );
//        }
//        metaInjectMeta.setDirectoryPath( directory );
//        metaInjectMeta.setTransName( transName );
//        metaInjectMeta.setFileName( null );
//        metaInjectMeta.setTransObjectId( null );
//        break;
      default:
        break;
    }

    metaInjectMeta.setSourceTransformName( wSourceTransform.getText() );
    metaInjectMeta.setSourceOutputFields(new ArrayList<>() );
    for ( int i = 0; i < wSourceFields.nrNonEmpty(); i++ ) {
      TableItem item = wSourceFields.getNonEmpty( i );
      int colIndex = 1;
      String name = item.getText( colIndex++ );
      int type = ValueMetaFactory.getIdForValueMeta( item.getText( colIndex++ ) );
      int length = Const.toInt( item.getText( colIndex++ ), -1 );
      int precision = Const.toInt( item.getText( colIndex++ ), -1 );
      metaInjectMeta.getSourceOutputFields().add( new MetaInjectOutputField( name, type, length, precision ) );
    }

    metaInjectMeta.setTargetFile( wTargetFile.getText() );
    metaInjectMeta.setNoExecution( !wNoExecution.getSelection() );

    final TransformMeta streamSourceTransform = pipelineMeta.findTransform( wStreamingSourceTransform.getText() );
    metaInjectMeta.setStreamSourceTransform( streamSourceTransform );
    // PDI-15989 Save streamSourceTransformName to find streamSourceTransform when loading
    metaInjectMeta.setStreamSourceTransformName( streamSourceTransform != null ? streamSourceTransform.getName() : "" );
    metaInjectMeta.setStreamTargetTransformName( wStreamingTargetTransform.getText() );

    metaInjectMeta.setTargetSourceMapping( targetSourceMapping );
    metaInjectMeta.setChanged( true );

    dispose();
  }

/*
  private void getByReferenceData( ObjectId transObjectId ) {
    try {
      if ( repository == null ) {
        throw new HopException( BaseMessages.getString(
          PKG, "MappingDialog.Exception.NotConnectedToRepository.Message" ) );
      }
      RepositoryObject transInf = repository.getObjectInformation( transObjectId, RepositoryObjectType.JOB );
      if ( transInf != null ) {
        getByReferenceData( transInf );
      }
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "MappingDialog.Exception.UnableToReferenceObjectId.Title" ),
        BaseMessages.getString( PKG, "MappingDialog.Exception.UnableToReferenceObjectId.Message" ), e );
    }
  }
*/
}
