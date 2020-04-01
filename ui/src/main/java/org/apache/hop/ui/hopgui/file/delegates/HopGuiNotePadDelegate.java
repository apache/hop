package org.apache.hop.ui.hopgui.file.delegates;

import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;

import java.util.List;

public class HopGuiNotePadDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private HopGui hopUi;
  private HopFileTypeHandlerInterface handler;
  private PropsUI props;

  public HopGuiNotePadDelegate( HopGui hopGui, HopFileTypeHandlerInterface handler ) {
    this.hopUi = hopGui;
    this.handler = handler;
    this.props = PropsUI.getInstance();
  }

  public void deleteNotes( AbstractMeta meta, List<NotePadMeta> notes ) {
    if ( notes == null || notes.isEmpty() ) {
      return; // Nothing to do
    }
    int[] idxs = new int[ notes.size() ];
    NotePadMeta[] noteCopies = new NotePadMeta[ notes.size() ];
    for ( int i = 0; i < idxs.length; i++ ) {
      idxs[ i ] = meta.indexOfNote( notes.get( i ) );
      noteCopies[ i ] = new NotePadMeta( notes.get( i ) );
    }
    for ( int idx : idxs ) {
      meta.removeNote( idx );
    }
    hopUi.undoDelegate.addUndoDelete( meta, noteCopies, idxs );
    handler.updateGui();
  }

  public void deleteNote( AbstractMeta meta, NotePadMeta notePadMeta ) {
    int idx = meta.indexOfNote( notePadMeta );
    if ( idx >= 0 ) {
      meta.removeNote( idx );
      hopUi.undoDelegate.addUndoDelete( meta, new NotePadMeta[] { (NotePadMeta) notePadMeta.clone() }, new int[] { idx } );
    }
    handler.updateGui();
  }

  public void newNote( AbstractMeta meta, int x, int y ) {
    String title = BaseMessages.getString( PKG, "PipelineGraph.Dialog.NoteEditor.Title" );
    NotePadDialog dd = new NotePadDialog( meta, hopUi.getShell(), title );
    NotePadMeta n = dd.open();
    if ( n != null ) {
      NotePadMeta npi =
        new NotePadMeta( n.getNote(), x, y, ConstUI.NOTE_MIN_SIZE, ConstUI.NOTE_MIN_SIZE, n
          .getFontName(), n.getFontSize(), n.isFontBold(), n.isFontItalic(), n.getFontColorRed(), n
          .getFontColorGreen(), n.getFontColorBlue(), n.getBackGroundColorRed(), n.getBackGroundColorGreen(), n
          .getBackGroundColorBlue(), n.getBorderColorRed(), n.getBorderColorGreen(), n.getBorderColorBlue(), n
          .isDrawShadow() );
      meta.addNote( npi );
      hopUi.undoDelegate.addUndoNew( meta, new NotePadMeta[] { npi }, new int[] { meta.indexOfNote( npi ) } );
      handler.updateGui();
    }
  }
}
