/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.core.widget;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.jface.fieldassist.ControlDecoration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ExtendedModifyEvent;
import org.eclipse.swt.custom.ExtendedModifyListener;
import org.eclipse.swt.custom.LineStyleListener;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetAdapter;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.MenuDetectEvent;
import org.eclipse.swt.events.MenuDetectListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

import java.util.LinkedList;
import java.util.List;

public class StyledTextComp extends Composite {
  private static final Class<?> PKG = StyledTextComp.class; // Needed by Translator

  // Modification for Undo/Redo on Styled Text
  private static final int MAX_STACK_SIZE = 25;
  private List<UndoRedoStack> undoStack;
  private List<UndoRedoStack> redoStack;
  private boolean bFullSelection = false;
  private StyledText styledText;
  private Menu styledTextPopupmenu;
  private String strTabName;
  private Composite xParent;
  private Image image;

  private KeyListener kls;
  private IVariables variables;
  private boolean varsSensitive;

  public StyledTextComp( IVariables variables, Composite parent, int args, String strTabName ) {
    this( variables, parent, args, strTabName, true, false );
  }


  public StyledTextComp( IVariables variables, Composite parent, int args, String strTabName, boolean varsSensitive ) {
    this( variables, parent, args, strTabName, varsSensitive, false );
  }

  public StyledTextComp( IVariables variables, Composite parent, int args, String strTabName, boolean varsSensitive,
                         boolean variableIconOnTop ) {
    super( parent, SWT.NONE );
    this.varsSensitive = varsSensitive;
    this.variables = variables;
    undoStack = new LinkedList<UndoRedoStack>();
    redoStack = new LinkedList<UndoRedoStack>();
    styledText = new StyledText( this, args );
    styledTextPopupmenu = new Menu( parent.getShell(), SWT.POP_UP );
    xParent = parent;
    this.strTabName = strTabName;
    // clipboard = new Clipboard(parent.getDisplay());
    this.setLayout( variableIconOnTop ? new FormLayout() : new FillLayout() );
    buildingStyledTextMenu();
    addUndoRedoSupport();

    kls = new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        if ( e.keyCode == 'h' && ( e.stateMask & SWT.MOD1 & SWT.SHIFT ) != 0 ) {
          new StyledTextCompReplace( styledTextPopupmenu.getShell(), styledText ).open();
        } else if ( e.keyCode == 'z' && ( e.stateMask & SWT.MOD1 ) != 0 ) {
          undo();
        } else if ( e.keyCode == 'y' && ( e.stateMask & SWT.MOD1 ) != 0 ) {
          redo();
        } else if ( e.keyCode == 'a' && ( e.stateMask & SWT.MOD1 ) != 0 ) {
          bFullSelection = true;
          styledText.selectAll();
        } else if ( e.keyCode == 'f' && ( e.stateMask & SWT.MOD1 ) != 0 ) {
          new StyledTextCompFind( styledTextPopupmenu.getShell(), styledText, BaseMessages.getString(
            PKG, "WidgetDialog.Styled.Find" ) ).open();
        }
      }
    };

    styledText.addKeyListener( kls );

    if ( this.varsSensitive ) {
      styledText.addKeyListener( new ControlSpaceKeyAdapter( this.variables, styledText ) );
      image = GuiResource.getInstance().getImageVariable();
      if ( variableIconOnTop ) {
        final Label wicon = new Label( this, SWT.RIGHT );
        PropsUi.getInstance().setLook( wicon );
        wicon.setToolTipText( BaseMessages.getString( PKG, "StyledTextComp.tooltip.InsertVariable" ) );
        wicon.setImage( image );
        wicon.setLayoutData( new FormDataBuilder().top().right( 100, 0 ).result() );
        styledText.setLayoutData( new FormDataBuilder().top( new FormAttachment( wicon, 0, 0 ) ).left().right( 100,
          0 ).bottom( 100, 0 ).result() );
      } else {
        ControlDecoration controlDecoration = new ControlDecoration( styledText, SWT.TOP | SWT.RIGHT );
        controlDecoration.setImage( image );
        controlDecoration
          .setDescriptionText( BaseMessages.getString( PKG, "StyledTextComp.tooltip.InsertVariable" ) );
        PropsUi.getInstance().setLook( controlDecoration.getControl() );
      }
    }

    // Create the drop target on the StyledText
    DropTarget dt = new DropTarget( styledText, DND.DROP_MOVE );
    dt.setTransfer( new Transfer[] { TextTransfer.getInstance() } );
    dt.addDropListener( new DropTargetAdapter() {
      public void dragOver( DropTargetEvent e ) {
        styledText.setFocus();
        Point location = xParent.getDisplay().map( null, styledText, e.x, e.y );
        location.x = Math.max( 0, location.x );
        location.y = Math.max( 0, location.y );
        try {
          int offset = styledText.getOffsetAtLocation( new Point( location.x, location.y ) );
          styledText.setCaretOffset( offset );
        } catch ( IllegalArgumentException ex ) {
          int maxOffset = styledText.getCharCount();
          Point maxLocation = styledText.getLocationAtOffset( maxOffset );
          if ( location.y >= maxLocation.y ) {
            if ( location.x >= maxLocation.x ) {
              styledText.setCaretOffset( maxOffset );
            } else {
              int offset = styledText.getOffsetAtLocation( new Point( location.x, maxLocation.y ) );
              styledText.setCaretOffset( offset );
            }
          } else {
            styledText.setCaretOffset( maxOffset );
          }
        }
      }

      public void drop( DropTargetEvent event ) {
        // Set the buttons text to be the text being dropped
        styledText.insert( (String) event.data );
      }
    } );

  }

  public String getSelectionText() {
    return styledText.getSelectionText();
  }

  public String getText() {
    return styledText.getText();
  }

  public void setText( String text ) {
    styledText.setText( text );
  }

  public int getCaretOffset() {
    return styledText.getCaretOffset();
  }

  public int getLineAtOffset( int iOffset ) {
    return styledText.getLineAtOffset( iOffset );
  }

  public void insert( String strInsert ) {
    styledText.insert( strInsert );
  }

  public void addModifyListener( ModifyListener lsMod ) {
    styledText.addModifyListener( lsMod );
  }

  public void addLineStyleListener( LineStyleListener lineStyler ) {
    styledText.addLineStyleListener( lineStyler );
  }

  public void addKeyListener( KeyAdapter keyAdapter ) {
    styledText.addKeyListener( keyAdapter );
  }

  public void addFocusListener( FocusAdapter focusAdapter ) {
    styledText.addFocusListener( focusAdapter );
  }

  public void addMouseListener( MouseAdapter mouseAdapter ) {
    styledText.addMouseListener( mouseAdapter );
  }

  public int getSelectionCount() {
    return styledText.getSelectionCount();
  }

  public void setSelection( int arg0 ) {
    styledText.setSelection( arg0 );
  }

  public void setSelection( int arg0, int arg1 ) {
    styledText.setSelection( arg0, arg1 );

  }

  public void setFont( Font fnt ) {
    styledText.setFont( fnt );
  }

  private void buildingStyledTextMenu() {
    // styledTextPopupmenu = new Menu(, SWT.POP_UP);
    MenuItem undoItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    undoItem.setText( OsHelper.customizeMenuitemText( BaseMessages.getString( PKG, "WidgetDialog.Styled.Undo" ) ) );
    undoItem.addListener( SWT.Selection, e -> undo() );

    MenuItem redoItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    redoItem.setText( OsHelper.customizeMenuitemText( BaseMessages.getString( PKG, "WidgetDialog.Styled.Redo" ) ) );
    redoItem.addListener( SWT.Selection, e -> redo() );

    new MenuItem( styledTextPopupmenu, SWT.SEPARATOR );
    MenuItem cutItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    cutItem.setText( OsHelper.customizeMenuitemText( BaseMessages.getString( PKG, "WidgetDialog.Styled.Cut" ) ) );
    cutItem.addListener( SWT.Selection, e -> styledText.cut() );

    MenuItem copyItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    copyItem.setText( OsHelper.customizeMenuitemText( BaseMessages.getString( PKG, "WidgetDialog.Styled.Copy" ) ) );
    copyItem.addListener( SWT.Selection, e -> styledText.copy() );

    MenuItem pasteItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    pasteItem
      .setText( OsHelper.customizeMenuitemText( BaseMessages.getString( PKG, "WidgetDialog.Styled.Paste" ) ) );
    pasteItem.addListener( SWT.Selection, e -> styledText.paste() );

    MenuItem selectAllItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    selectAllItem.setText( OsHelper.customizeMenuitemText( BaseMessages.getString(
      PKG, "WidgetDialog.Styled.SelectAll" ) ) );
    selectAllItem.addListener( SWT.Selection, e -> styledText.selectAll() );

    new MenuItem( styledTextPopupmenu, SWT.SEPARATOR );
    MenuItem findItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    findItem.setText( OsHelper.customizeMenuitemText( BaseMessages.getString( PKG, "WidgetDialog.Styled.Find" ) ) );
    findItem.addListener( SWT.Selection, e -> {
      StyledTextCompFind stFind =
        new StyledTextCompFind( styledText.getShell(), styledText, BaseMessages.getString(
          PKG, "WidgetDialog.Styled.FindString", strTabName ) );
      stFind.open();
    } );
    MenuItem replaceItem = new MenuItem( styledTextPopupmenu, SWT.PUSH );
    replaceItem.setText( OsHelper.customizeMenuitemText( BaseMessages.getString(
      PKG, "WidgetDialog.Styled.Replace" ) ) );
    replaceItem.setAccelerator( SWT.MOD1 | 'H' );
    // (helpMenu, SWT.PUSH, "&About\tCtrl+A",
    // null, SWT.CTRL + 'A', true, "doAbout");

    replaceItem.addListener( SWT.Selection, e -> {
      StyledTextCompReplace stReplace = new StyledTextCompReplace( styledText.getShell(), styledText );
      stReplace.open();
    } );

    styledText.addMenuDetectListener( e -> {
      // Enable menus, if the Selection is ok
      if ( undoStack.size() > 0 ) {
        styledTextPopupmenu.getItem( 0 ).setEnabled( true );
      } else {
        styledTextPopupmenu.getItem( 0 ).setEnabled( false );
      }

      if ( redoStack.size() > 0 ) {
        styledTextPopupmenu.getItem( 1 ).setEnabled( true );
      } else {
        styledTextPopupmenu.getItem( 1 ).setEnabled( false );
      }

      styledTextPopupmenu.getItem( 5 ).setEnabled( checkPaste() );
      if ( styledText.getSelectionCount() > 0 ) {
        styledTextPopupmenu.getItem( 3 ).setEnabled( true );
        styledTextPopupmenu.getItem( 4 ).setEnabled( true );
      } else {
        styledTextPopupmenu.getItem( 3 ).setEnabled( false );
        styledTextPopupmenu.getItem( 4 ).setEnabled( false );
      }
    } );
    styledText.setMenu( styledTextPopupmenu );
  }

  // Check if something is stored inside the Clipboard
  private boolean checkPaste() {
    try {
      Clipboard clipboard = new Clipboard( xParent.getDisplay() );
      TextTransfer transfer = TextTransfer.getInstance();
      String text = (String) clipboard.getContents( transfer );
      if ( text != null && text.length() > 0 ) {
        return true;
      } else {
        return false;
      }
    } catch ( Exception e ) {
      return false;
    }
  }

  // Start Functions for Undo / Redo on wSrcipt
  private void addUndoRedoSupport() {

    styledText.addSelectionListener( new SelectionListener() {
      public void widgetSelected( SelectionEvent event ) {
        if ( styledText.getSelectionCount() == styledText.getCharCount() ) {
          bFullSelection = true;
          try {
            event.wait( 2 );
          } catch ( Exception e ) {
            // Ignore errors
          }
        }
      }

      public void widgetDefaultSelected( SelectionEvent event ) {
      }
    } );

    styledText.addExtendedModifyListener( event -> {
      int iEventLength = event.length;
      int iEventStartPostition = event.start;

      // Unterscheidung um welche Art es sich handelt Delete or Insert
      String newText = styledText.getText();
      String repText = event.replacedText;
      String oldText = "";
      int iEventType = -1;

      // if((event.length!=newText.length()) || newText.length()==1){
      if ( ( event.length != newText.length() ) || ( bFullSelection ) ) {
        if ( repText != null && repText.length() > 0 ) {
          oldText =
            newText.substring( 0, event.start ) + repText + newText.substring( event.start + event.length );
          iEventType = UndoRedoStack.DELETE;
          iEventLength = repText.length();
        } else {
          oldText = newText.substring( 0, event.start ) + newText.substring( event.start + event.length );
          iEventType = UndoRedoStack.INSERT;
        }

        if ( ( oldText != null && oldText.length() > 0 ) || ( iEventStartPostition == event.length ) ) {
          UndoRedoStack urs =
            new UndoRedoStack( iEventStartPostition, newText, oldText, iEventLength, iEventType );
          if ( undoStack.size() == MAX_STACK_SIZE ) {
            undoStack.remove( undoStack.size() - 1 );
          }
          undoStack.add( 0, urs );
        }
      }
      bFullSelection = false;
    } );

  }

  private void undo() {
    if ( undoStack.size() > 0 ) {
      UndoRedoStack urs = undoStack.remove( 0 );
      if ( redoStack.size() == MAX_STACK_SIZE ) {
        redoStack.remove( redoStack.size() - 1 );
      }
      UndoRedoStack rro =
        new UndoRedoStack( urs.getCursorPosition(), urs.getReplacedText(), styledText.getText(), urs
          .getEventLength(), urs.getType() );
      bFullSelection = false;
      styledText.setText( urs.getReplacedText() );
      if ( urs.getType() == UndoRedoStack.INSERT ) {
        styledText.setCaretOffset( urs.getCursorPosition() );
      } else if ( urs.getType() == UndoRedoStack.DELETE ) {
        styledText.setCaretOffset( urs.getCursorPosition() + urs.getEventLength() );
        styledText.setSelection( urs.getCursorPosition(), urs.getCursorPosition() + urs.getEventLength() );
        if ( styledText.getSelectionCount() == styledText.getCharCount() ) {
          bFullSelection = true;
        }
      }
      redoStack.add( 0, rro );
    }

  }

  private void redo() {
    if ( redoStack.size() > 0 ) {
      UndoRedoStack urs = redoStack.remove( 0 );
      if ( undoStack.size() == MAX_STACK_SIZE ) {
        undoStack.remove( undoStack.size() - 1 );
      }
      UndoRedoStack rro =
        new UndoRedoStack( urs.getCursorPosition(), urs.getReplacedText(), styledText.getText(), urs
          .getEventLength(), urs.getType() );
      bFullSelection = false;
      styledText.setText( urs.getReplacedText() );
      if ( urs.getType() == UndoRedoStack.INSERT ) {
        styledText.setCaretOffset( urs.getCursorPosition() );
      } else if ( urs.getType() == UndoRedoStack.DELETE ) {
        styledText.setCaretOffset( urs.getCursorPosition() + urs.getEventLength() );
        styledText.setSelection( urs.getCursorPosition(), urs.getCursorPosition() + urs.getEventLength() );
        if ( styledText.getSelectionCount() == styledText.getCharCount() ) {
          bFullSelection = true;
        }
      }
      undoStack.add( 0, rro );
    }
  }

  /**
   * Returns the {@link Image} that the {@link StyledText} is decorated with.
   *
   * @return the {@link Image} that the {@link StyledText} is decorated with.
   */
  public Image getImage() {
    return image;
  }

  public StyledText getStyledText() {
    return styledText;
  }

  public boolean isEditable() {
    return styledText.getEditable();
  }

  public void setEditable( boolean canEdit ) {
    styledText.setEditable( canEdit );
  }


  @Override
  public void setEnabled( boolean enabled ) {
    styledText.setEnabled( enabled );
    // StyledText component does not get the "disabled" look, so it needs to be applied explicitly
    // See https://bugs.eclipse.org/bugs/show_bug.cgi?id=4745
    if ( Display.getDefault() != null ) {
      Color foreground = Display.getDefault().getSystemColor( enabled ? SWT.COLOR_BLACK : SWT.COLOR_DARK_GRAY );
      Color background = Display.getDefault().getSystemColor( enabled ? SWT.COLOR_WHITE : SWT.COLOR_WIDGET_BACKGROUND );
      styledText.setForeground( foreground );
      styledText.setBackground( background );
    }
  }
}
