/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.database.dialog.DatabaseDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;


/**
 * Shows a user 2 lists of strings and allows the linkage of values between values in the 2 lists
 *
 * @author Matt
 * @since 23-03-2006
 */
public class EnterMappingDialog extends Dialog {
  private static Class<?> PKG = DatabaseDialog.class; // for i18n purposes, needed by Translator2!!

  public class GuessPair {
    private int _srcIndex = -1;
    private int _targetIndex = -1;
    private boolean _found = false;

    public GuessPair() {
      _found = false;
    }

    public GuessPair( int src ) {
      _srcIndex = src;
      _found = false;
    }

    public GuessPair( int src, int target, boolean found ) {
      _srcIndex = src;
      _targetIndex = target;
      _found = found;
    }

    public GuessPair( int src, int target ) {
      _srcIndex = src;
      _targetIndex = target;
      _found = true;
    }

    public int getTargetIndex() {
      return _targetIndex;
    }

    public void setTargetIndex( int targetIndex ) {
      _found = true;
      _targetIndex = targetIndex;
    }

    public int getSrcIndex() {
      return _srcIndex;
    }

    public void setSrcIndex( int srcIndex ) {
      _srcIndex = srcIndex;
    }

    public boolean getFound() {
      return _found;
    }
  }

  public static final String STRING_ORIGIN_SEPARATOR = "            (";

  private Label wlSource;

  public static final String STRING_SFORCE_EXTERNALID_SEPARATOR = "/";

  private List wSource;

  private FormData fdlSource, fdSource;

  private Label wlSourceAuto;

  private Button wSourceAuto;

  private FormData fdlSourceAuto, fdSourceAuto;

  private Label wlSourceHide;

  private Button wSourceHide;

  private FormData fdlSourceHide, fdSourceHide;

  private Label wlTarget;

  private List wTarget;

  private FormData fdlTarget, fdTarget;

  private Label wlTargetAuto;

  private Button wTargetAuto;

  private FormData fdlTargetAuto, fdTargetAuto;

  private Label wlTargetHide;

  private Button wTargetHide;

  private FormData fdlTargetHide, fdTargetHide;

  private Label wlResult;

  private List wResult;

  private FormData fdlResult, fdResult;

  private Button wAdd;

  private FormData fdAdd;

  private Button wDelete;

  private FormData fdDelete;

  private Button wOK, wGuess, wCancel;

  private Listener lsOK, lsGuess, lsCancel;

  private Shell shell;

  private String[] sourceList;

  private String[] targetList;

  private PropsUI props;

  private java.util.List<SourceToTargetMapping> mappings;

  /**
   * Create a new dialog allowing the user to enter a mapping
   *
   * @param parent the parent shell
   * @param source the source values
   * @param target the target values
   */
  public EnterMappingDialog( Shell parent, String[] source, String[] target ) {
    this( parent, source, target, new ArrayList<SourceToTargetMapping>() );
  }

  /**
   * Create a new dialog allowing the user to enter a mapping
   *
   * @param parent   the parent shell
   * @param source   the source values
   * @param target   the target values
   * @param mappings the already selected mappings (ArrayList containing <code>SourceToTargetMapping</code>s)
   */
  public EnterMappingDialog( Shell parent, String[] source, String[] target,
                             java.util.List<SourceToTargetMapping> mappings ) {
    super( parent, SWT.NONE );
    props = PropsUI.getInstance();
    this.sourceList = source;
    this.targetList = target;

    this.mappings = mappings;
  }

  public java.util.List<SourceToTargetMapping> open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell =
      new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX | SWT.APPLICATION_MODAL | SWT.SHEET );
    props.setLook( shell );

    shell.setImage( GUIResource.getInstance().getImageHopUi() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "EnterMappingDialog.Title" ) );
    shell.setImage( GUIResource.getInstance().getImageTransGraph() );

    int margin = props.getMargin();
    int buttonSpace = 90;

    // Source table
    wlSource = new Label( shell, SWT.NONE );
    wlSource.setText( BaseMessages.getString( PKG, "EnterMappingDialog.SourceFields.Label" ) );
    props.setLook( wlSource );
    fdlSource = new FormData();
    fdlSource.left = new FormAttachment( 0, 0 );
    fdlSource.top = new FormAttachment( 0, margin );
    wlSource.setLayoutData( fdlSource );
    wSource = new List( shell, SWT.SINGLE | SWT.RIGHT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL );
    for ( int i = 0; i < sourceList.length; i++ ) {
      wSource.add( sourceList[ i ] );
    }
    props.setLook( wSource );
    fdSource = new FormData();
    fdSource.left = new FormAttachment( 0, 0 );
    fdSource.right = new FormAttachment( 25, 0 );
    fdSource.top = new FormAttachment( wlSource, margin );
    fdSource.bottom = new FormAttachment( 100, -buttonSpace );
    wSource.setLayoutData( fdSource );

    // Automatic target selection
    wlSourceAuto = new Label( shell, SWT.NONE );
    wlSourceAuto.setText( BaseMessages.getString( PKG, "EnterMappingDialog.AutoTargetSelection.Label" ) );
    props.setLook( wlSourceAuto );
    fdlSourceAuto = new FormData();
    fdlSourceAuto.left = new FormAttachment( 0, 0 );
    fdlSourceAuto.top = new FormAttachment( wSource, margin );
    wlSourceAuto.setLayoutData( fdlSourceAuto );
    wSourceAuto = new Button( shell, SWT.CHECK );
    wSourceAuto.setSelection( true );
    props.setLook( wSourceAuto );
    fdSourceAuto = new FormData();
    fdSourceAuto.left = new FormAttachment( wlSourceAuto, margin * 2 );
    fdSourceAuto.right = new FormAttachment( 25, 0 );
    fdSourceAuto.top = new FormAttachment( wSource, margin );
    wSourceAuto.setLayoutData( fdSourceAuto );

    // Hide used source fields?
    wlSourceHide = new Label( shell, SWT.NONE );
    wlSourceHide.setText( BaseMessages.getString( PKG, "EnterMappingDialog.HideUsedSources" ) );
    props.setLook( wlSourceHide );
    fdlSourceHide = new FormData();
    fdlSourceHide.left = new FormAttachment( 0, 0 );
    fdlSourceHide.top = new FormAttachment( wSourceAuto, margin );
    wlSourceHide.setLayoutData( fdlSourceHide );
    wSourceHide = new Button( shell, SWT.CHECK );
    wSourceHide.setSelection( true );
    props.setLook( wSourceHide );
    fdSourceHide = new FormData();
    fdSourceHide.left = new FormAttachment( wlSourceHide, margin * 2 );
    fdSourceHide.right = new FormAttachment( 25, 0 );
    fdSourceHide.top = new FormAttachment( wSourceAuto, margin );
    wSourceHide.setLayoutData( fdSourceHide );
    wSourceHide.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        refreshMappings();
      }
    } );

    // Target table
    wlTarget = new Label( shell, SWT.NONE );
    wlTarget.setText( BaseMessages.getString( PKG, "EnterMappingDialog.TargetFields.Label" ) );
    props.setLook( wlTarget );
    fdlTarget = new FormData();
    fdlTarget.left = new FormAttachment( wSource, margin * 2 );
    fdlTarget.top = new FormAttachment( 0, margin );
    wlTarget.setLayoutData( fdlTarget );
    wTarget = new List( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL );
    for ( int i = 0; i < targetList.length; i++ ) {
      wTarget.add( targetList[ i ] );
    }
    props.setLook( wTarget );
    fdTarget = new FormData();
    fdTarget.left = new FormAttachment( wSource, margin * 2 );
    fdTarget.right = new FormAttachment( 50, 0 );
    fdTarget.top = new FormAttachment( wlTarget, margin );
    fdTarget.bottom = new FormAttachment( 100, -buttonSpace );
    wTarget.setLayoutData( fdTarget );

    // Automatic target selection
    wlTargetAuto = new Label( shell, SWT.NONE );
    wlTargetAuto.setText( BaseMessages.getString( PKG, "EnterMappingDialog.AutoSourceSelection.Label" ) );
    props.setLook( wlTargetAuto );
    fdlTargetAuto = new FormData();
    fdlTargetAuto.left = new FormAttachment( wSource, margin * 2 );
    fdlTargetAuto.top = new FormAttachment( wTarget, margin );
    wlTargetAuto.setLayoutData( fdlTargetAuto );
    wTargetAuto = new Button( shell, SWT.CHECK );
    wTargetAuto.setSelection( false );
    props.setLook( wTargetAuto );
    fdTargetAuto = new FormData();
    fdTargetAuto.left = new FormAttachment( wlTargetAuto, margin * 2 );
    fdTargetAuto.right = new FormAttachment( 50, 0 );
    fdTargetAuto.top = new FormAttachment( wTarget, margin );
    wTargetAuto.setLayoutData( fdTargetAuto );

    // Automatic target selection
    wlTargetHide = new Label( shell, SWT.NONE );
    wlTargetHide.setText( BaseMessages.getString( PKG, "EnterMappingDialog.HideUsedTargets" ) );
    props.setLook( wlTargetHide );
    fdlTargetHide = new FormData();
    fdlTargetHide.left = new FormAttachment( wSource, margin * 2 );
    fdlTargetHide.top = new FormAttachment( wTargetAuto, margin );
    wlTargetHide.setLayoutData( fdlTargetHide );
    wTargetHide = new Button( shell, SWT.CHECK );
    wTargetHide.setSelection( true );
    props.setLook( wTargetHide );
    fdTargetHide = new FormData();
    fdTargetHide.left = new FormAttachment( wlTargetHide, margin * 2 );
    fdTargetHide.right = new FormAttachment( 50, 0 );
    fdTargetHide.top = new FormAttachment( wTargetAuto, margin );
    wTargetHide.setLayoutData( fdTargetHide );

    wTargetHide.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        refreshMappings();
      }
    } );

    // Add a couple of buttons:
    wAdd = new Button( shell, SWT.PUSH );
    fdAdd = new FormData();
    wAdd.setText( BaseMessages.getString( PKG, "EnterMappingDialog.Button.Add" ) );
    fdAdd.left = new FormAttachment( wTarget, margin * 2 );
    fdAdd.top = new FormAttachment( wTarget, 0, SWT.CENTER );
    wAdd.setLayoutData( fdAdd );
    Listener lsAdd = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        add();
      }
    };
    wAdd.addListener( SWT.Selection, lsAdd );

    // Delete a couple of buttons:
    wDelete = new Button( shell, SWT.PUSH );
    fdDelete = new FormData();
    wDelete.setText( BaseMessages.getString( PKG, "EnterMappingDialog.Button.Delete" ) );
    fdDelete.left = new FormAttachment( wTarget, margin * 2 );
    fdDelete.top = new FormAttachment( wAdd, margin * 2 );
    wDelete.setLayoutData( fdDelete );
    Listener lsDelete = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        delete();
      }
    };
    wDelete.addListener( SWT.Selection, lsDelete );

    // Result table
    wlResult = new Label( shell, SWT.NONE );
    wlResult.setText( BaseMessages.getString( PKG, "EnterMappingDialog.ResultMappings.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( wDelete, margin * 2 );
    fdlResult.top = new FormAttachment( 0, margin );
    wlResult.setLayoutData( fdlResult );
    wResult = new List( shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL );
    for ( int i = 0; i < targetList.length; i++ ) {
      wResult.add( targetList[ i ] );
    }
    props.setLook( wResult );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( wDelete, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    fdResult.top = new FormAttachment( wlResult, margin );
    fdResult.bottom = new FormAttachment( 100, -30 );
    wResult.setLayoutData( fdResult );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    lsOK = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        ok();
      }
    };
    wOK.addListener( SWT.Selection, lsOK );

    // Some buttons
    wGuess = new Button( shell, SWT.PUSH );
    wGuess.setText( BaseMessages.getString( PKG, "EnterMappingDialog.Button.Guess" ) );
    lsGuess = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        guess();
      }
    };
    wGuess.addListener( SWT.Selection, lsGuess );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    lsCancel = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    wCancel.addListener( SWT.Selection, lsCancel );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wGuess, wCancel }, margin, null );

    wSource.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        if ( wSourceAuto.getSelection() ) {
          findTarget();
        }
      }

      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        add();
      }
    } );

    wTarget.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        if ( wTargetAuto.getSelection() ) {
          findSource();
        }
      }

      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        add();
      }
    } );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return mappings;
  }

  private void guess() {
    // Guess the target for all the sources...
    String[] sortedSourceList = Arrays.copyOf( sourceList, sourceList.length );

    // Sort Longest to Shortest string - makes matching better
    Arrays.sort( sortedSourceList, new Comparator<String>() {
      @Override
      public int compare( String s1, String s2 ) {
        return s2.length() - s1.length();
      }
    } );
    // Look for matches using longest field name to shortest
    ArrayList<GuessPair> pList = new ArrayList<GuessPair>();
    for ( int i = 0; i < sourceList.length; i++ ) {
      int idx = Const.indexOfString( sortedSourceList[ i ], wSource.getItems() );
      if ( idx >= 0 ) {
        pList.add( findTargetPair( idx ) );
      }
    }
    // Now add them in order or source field list
    Collections.sort( pList, new Comparator<GuessPair>() {
      @Override
      public int compare( GuessPair s1, GuessPair s2 ) {
        return s1.getSrcIndex() - s2.getSrcIndex();
      }
    } );
    for ( GuessPair p : pList ) {
      if ( p.getFound() ) {
        SourceToTargetMapping mapping = new SourceToTargetMapping( p.getSrcIndex(), p.getTargetIndex() );
        mappings.add( mapping );
      }
    }
    refreshMappings();
  }

  private boolean findTarget() {
    int sourceIndex = wSource.getSelectionIndex();
    GuessPair p = findTargetPair( sourceIndex );
    if ( p.getFound() ) {
      wTarget.setSelection( p.getTargetIndex() );
    }
    return p.getFound();
  }

  private GuessPair findTargetPair( int sourceIndex ) {
    // Guess, user selects an entry in the list on the left.
    // Find a comparable entry in the target list...
    GuessPair result = new GuessPair( sourceIndex );

    if ( sourceIndex < 0 ) {
      return result;  // Not Found
    }

    // Skip everything after the bracket...
    String sourceStr = wSource.getItem( sourceIndex ).toUpperCase();

    int indexOfBracket = sourceStr.indexOf( EnterMappingDialog.STRING_ORIGIN_SEPARATOR );
    String sourceString = sourceStr;
    if ( indexOfBracket >= 0 ) {
      sourceString = sourceStr.substring( 0, indexOfBracket );
    }
    int length = sourceString.length();
    boolean first = true;

    boolean found = false;
    while ( !found && ( length >= ( (int) ( sourceString.length() * 0.85 ) ) || first ) ) {
      first = false;

      for ( int i = 0; i < wTarget.getItemCount() && !found; i++ ) {
        String test = wTarget.getItem( i ).toUpperCase();
        // Clean up field names in the form of OBJECT:LOOKUPFIELD/OBJECTNAME
        if ( test.contains( EnterMappingDialog.STRING_SFORCE_EXTERNALID_SEPARATOR ) ) {
          String[] tmp = test.split( EnterMappingDialog.STRING_SFORCE_EXTERNALID_SEPARATOR );
          test = tmp[ tmp.length - 1 ];
          if ( test.endsWith( "__R" ) ) {
            test = test.substring( 0, test.length() - 3 ) + "__C";
          }
        }
        if ( test.indexOf( sourceString.substring( 0, length ) ) >= 0 ) {
          result.setSrcIndex( sourceIndex );
          result.setTargetIndex( i );
          found = true;
        }
      }
      length--;
    }
    return result;
  }

  private boolean findSource() {
    // Guess, user selects an entry in the list on the right.
    // Find a comparable entry in the source list...
    boolean found = false;

    int targetIndex = wTarget.getSelectionIndex();
    // Skip everything after the bracket...
    String targetString = wTarget.getItem( targetIndex ).toUpperCase();

    int length = targetString.length();
    boolean first = true;

    while ( !found && ( length >= 2 || first ) ) {
      first = false;

      for ( int i = 0; i < wSource.getItemCount() && !found; i++ ) {
        if ( wSource.getItem( i ).toUpperCase().indexOf( targetString.substring( 0, length ) ) >= 0 ) {
          wSource.setSelection( i );
          found = true;
        }
      }
      length--;
    }
    return found;
  }

  private void add() {
    if ( wSource.getSelectionCount() == 1 && wTarget.getSelectionCount() == 1 ) {
      String sourceString = wSource.getSelection()[ 0 ];
      String targetString = wTarget.getSelection()[ 0 ];

      int srcIndex = Const.indexOfString( sourceString, sourceList );
      int tgtIndex = Const.indexOfString( targetString, targetList );

      if ( srcIndex >= 0 && tgtIndex >= 0 ) {
        // New mapping: add it to the list...
        SourceToTargetMapping mapping = new SourceToTargetMapping( srcIndex, tgtIndex );
        mappings.add( mapping );

        refreshMappings();
      }
    }
  }

  private void refreshMappings() {
    // Refresh the results...
    wResult.removeAll();
    for ( int i = 0; i < mappings.size(); i++ ) {
      SourceToTargetMapping mapping = mappings.get( i );
      String mappingString =
        sourceList[ mapping.getSourcePosition() ] + " --> " + targetList[ mapping.getTargetPosition() ];
      wResult.add( mappingString );
    }

    wSource.removeAll();
    // Refresh the sources
    for ( int a = 0; a < sourceList.length; a++ ) {
      boolean found = false;
      if ( wSourceHide.getSelection() ) {
        for ( int b = 0; b < mappings.size() && !found; b++ ) {
          SourceToTargetMapping mapping = mappings.get( b );
          if ( mapping.getSourcePosition() == Const.indexOfString( sourceList[ a ], sourceList ) ) {
            found = true;
          }
        }
      }

      if ( !found ) {
        wSource.add( sourceList[ a ] );
      }
    }

    wTarget.removeAll();
    // Refresh the targets
    for ( int a = 0; a < targetList.length; a++ ) {
      boolean found = false;
      if ( wTargetHide.getSelection() ) {
        for ( int b = 0; b < mappings.size() && !found; b++ ) {
          SourceToTargetMapping mapping = mappings.get( b );
          if ( mapping.getTargetPosition() == Const.indexOfString( targetList[ a ], targetList ) ) {
            found = true;
          }
        }
      }

      if ( !found ) {
        wTarget.add( targetList[ a ] );
      }
    }

  }

  private void delete() {
    String[] result = wResult.getSelection();
    for ( int i = result.length - 1; i >= 0; i-- ) {
      int idx = wResult.indexOf( result[ i ] );
      if ( idx >= 0 && idx < mappings.size() ) {
        mappings.remove( idx );
      }
    }
    refreshMappings();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    refreshMappings();
  }

  private void cancel() {
    mappings = null;
    dispose();
  }

  private void ok() {
    dispose();
  }
}
