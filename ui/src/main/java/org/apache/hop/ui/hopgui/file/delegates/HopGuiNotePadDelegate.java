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

package org.apache.hop.ui.hopgui.file.delegates;

import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;

import java.util.List;

public class HopGuiNotePadDelegate {
  private static final Class<?> PKG = HopGui.class; // For Translator

  private HopGui hopGui;
  private IHopFileTypeHandler handler;
  private PropsUi props;

  public HopGuiNotePadDelegate( HopGui hopGui, IHopFileTypeHandler handler ) {
    this.hopGui = hopGui;
    this.handler = handler;
    this.props = PropsUi.getInstance();
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
    hopGui.undoDelegate.addUndoDelete( meta, noteCopies, idxs );
    handler.updateGui();
  }

  public void deleteNote( AbstractMeta meta, NotePadMeta notePadMeta ) {
    int idx = meta.indexOfNote( notePadMeta );
    if ( idx >= 0 ) {
      meta.removeNote( idx );
      hopGui.undoDelegate.addUndoDelete( meta, new NotePadMeta[] { (NotePadMeta) notePadMeta.clone() }, new int[] { idx } );
    }
    handler.updateGui();
  }

  public void newNote( IVariables variables, AbstractMeta meta, int x, int y ) {
    String title = BaseMessages.getString( PKG, "PipelineGraph.Dialog.NoteEditor.Title" );
    NotePadDialog dd = new NotePadDialog( variables, hopGui.getShell(), title );
    NotePadMeta n = dd.open();
    if ( n != null ) {
      NotePadMeta npi =
        new NotePadMeta( n.getNote(), x, y, ConstUi.NOTE_MIN_SIZE, ConstUi.NOTE_MIN_SIZE, n
          .getFontName(), n.getFontSize(), n.isFontBold(), n.isFontItalic(), n.getFontColorRed(), n
          .getFontColorGreen(), n.getFontColorBlue(), n.getBackGroundColorRed(), n.getBackGroundColorGreen(), n
          .getBackGroundColorBlue(), n.getBorderColorRed(), n.getBorderColorGreen(), n.getBorderColorBlue() );
      meta.addNote( npi );
      hopGui.undoDelegate.addUndoNew( meta, new NotePadMeta[] { npi }, new int[] { meta.indexOfNote( npi ) } );
      handler.updateGui();
    }
  }
}
