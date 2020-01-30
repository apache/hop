package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.dialog.NotePadDialog;

import java.util.List;

public class HopGuiNotePadDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  private HopGui hopUi;
  private HopFileTypeHandlerInterface handler;
  private PropsUI props;

  public HopGuiNotePadDelegate( HopGui hopGui, HopFileTypeHandlerInterface handler ) {
    this.hopUi = hopGui;
    this.handler = handler;
    this.props = PropsUI.getInstance();
  }

  public void deleteNotes( AbstractMeta transMeta, List<NotePadMeta> notes) {
    if (notes==null || notes.isEmpty()) {
      return; // Nothing to do
    }
    int[] idxs = new int[notes.size()];
    NotePadMeta[] noteCopies = new NotePadMeta[notes.size()];
    for (int i=0;i<idxs.length;i++) {
      idxs[i]=transMeta.indexOfNote( notes.get(i) );
      noteCopies[i] = new NotePadMeta(notes.get( i ));
    }
    for (int idx : idxs) {
      transMeta.removeNote( idx );
    }
    hopUi.undoDelegate.addUndoDelete( transMeta, noteCopies, idxs );
    handler.updateGui();
  }

  public void deleteNote( AbstractMeta transMeta, NotePadMeta notePadMeta) {
    int idx = transMeta.indexOfNote( notePadMeta );
    if ( idx >= 0 ) {
      transMeta.removeNote( idx );
      hopUi.undoDelegate.addUndoDelete( transMeta, new NotePadMeta[] { (NotePadMeta) notePadMeta.clone() }, new int[] { idx } );
    }
    handler.updateGui();
  }

  public void newNote(AbstractMeta transMeta, int x, int y) {
    String title = BaseMessages.getString( PKG, "TransGraph.Dialog.NoteEditor.Title" );
    NotePadDialog dd = new NotePadDialog( transMeta, hopUi.getShell(), title );
    NotePadMeta n = dd.open();
    if ( n != null ) {
      NotePadMeta npi =
        new NotePadMeta( n.getNote(), x, y, ConstUI.NOTE_MIN_SIZE, ConstUI.NOTE_MIN_SIZE, n
          .getFontName(), n.getFontSize(), n.isFontBold(), n.isFontItalic(), n.getFontColorRed(), n
          .getFontColorGreen(), n.getFontColorBlue(), n.getBackGroundColorRed(), n.getBackGroundColorGreen(), n
          .getBackGroundColorBlue(), n.getBorderColorRed(), n.getBorderColorGreen(), n.getBorderColorBlue(), n
          .isDrawShadow() );
      transMeta.addNote( npi );
      hopUi.undoDelegate.addUndoNew( transMeta, new NotePadMeta[] { npi }, new int[] { transMeta.indexOfNote( npi ) } );
      handler.updateGui();
    }
  }
}
