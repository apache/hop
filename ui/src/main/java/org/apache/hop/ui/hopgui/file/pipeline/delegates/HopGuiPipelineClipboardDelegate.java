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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiPipelineClipboardDelegate {
  private static final Class<?> PKG = HopGui.class; // For Translator

  public static final String XML_TAG_PIPELINE_TRANSFORMS = "pipeline-transforms";
  private static final String XML_TAG_TRANSFORMS = "transforms";

  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;
  private ILogChannel log;

  public HopGuiPipelineClipboardDelegate( HopGui hopGui, HopGuiPipelineGraph pipelineGraph ) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
    this.log = hopGui.getLog();
  }

  public void toClipboard( String clipText ) {
    try {
      GuiResource.getInstance().toClipboard( clipText );
    } catch ( Throwable e ) {
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Message" ), e );
    }
  }

  public String fromClipboard() {
    try {
      return GuiResource.getInstance().fromClipboard();
    } catch ( Throwable e ) {
      new ErrorDialog(
        hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Message" ), e );
      return null;
    }
  }

  public void pasteXml( PipelineMeta pipelineMeta, String clipcontent, Point loc ) {

    try {
      Document doc = XmlHandler.loadXmlString( clipcontent );
      Node pipelineNode = XmlHandler.getSubNode( doc, XML_TAG_PIPELINE_TRANSFORMS );
      // De-select all, re-select pasted transforms...
      pipelineMeta.unselectAll();

      Node transformsNode = XmlHandler.getSubNode( pipelineNode, "transforms" );
      int nr = XmlHandler.countNodes( transformsNode, "transform" );
      if ( log.isDebug() ) {
        // "I found "+nr+" transforms to paste on location: "
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundTransforms", "" + nr ) + loc );
      }
      TransformMeta[] transforms = new TransformMeta[ nr ];
      ArrayList<String> transformOldNames = new ArrayList<>( nr );

      // Point min = new Point(loc.x, loc.y);
      Point min = new Point( 99999999, 99999999 );

      // Load the transforms...
      for ( int i = 0; i < nr; i++ ) {
        Node transformNode = XmlHandler.getSubNodeByNr( transformsNode, "transform", i );
        transforms[ i ] = new TransformMeta( transformNode, hopGui.getMetadataProvider() );

        if ( loc != null ) {
          Point p = transforms[ i ].getLocation();

          if ( min.x > p.x ) {
            min.x = p.x;
          }
          if ( min.y > p.y ) {
            min.y = p.y;
          }
        }
      }

      // Load the hops...
      Node hopsNode = XmlHandler.getSubNode( pipelineNode, "order" );
      nr = XmlHandler.countNodes( hopsNode, "hop" );
      if ( log.isDebug() ) {
        // "I found "+nr+" hops to paste."
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundHops", "" + nr ) );
      }
      PipelineHopMeta[] hops = new PipelineHopMeta[ nr ];

      for ( int i = 0; i < nr; i++ ) {
        Node hopNode = XmlHandler.getSubNodeByNr( hopsNode, "hop", i );
        hops[ i ] = new PipelineHopMeta( hopNode, Arrays.asList( transforms ) );
      }

      // This is the offset:
      Point offset = new Point( loc.x - min.x, loc.y - min.y );

      // Undo/redo object positions...
      int[] position = new int[ transforms.length ];

      for ( int i = 0; i < transforms.length; i++ ) {
        Point p = transforms[ i ].getLocation();
        String name = transforms[ i ].getName();

        PropsUi.setLocation( transforms[ i ], p.x + offset.x, p.y + offset.y );

        // Check the name, find alternative...
        transformOldNames.add( name );
        transforms[ i ].setName( pipelineMeta.getAlternativeTransformName( name ) );
        pipelineMeta.addTransform( transforms[ i ] );
        position[ i ] = pipelineMeta.indexOfTransform( transforms[ i ] );
        transforms[ i ].setSelected( true );
      }

      // Add the hops too...
      for ( PipelineHopMeta hop : hops ) {
        pipelineMeta.addPipelineHop( hop );
      }

      // Load the notes...
      Node notesNode = XmlHandler.getSubNode( pipelineNode, "notepads" );
      nr = XmlHandler.countNodes( notesNode, "notepad" );
      if ( log.isDebug() ) {
        // "I found "+nr+" notepads to paste."
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundNotepads", "" + nr ) );
      }
      NotePadMeta[] notes = new NotePadMeta[ nr ];

      for ( int i = 0; i < notes.length; i++ ) {
        Node noteNode = XmlHandler.getSubNodeByNr( notesNode, "notepad", i );
        notes[ i ] = new NotePadMeta( noteNode );
        Point p = notes[ i ].getLocation();
        notes[ i ].setLocation( p.x + offset.x, p.y + offset.y );
        pipelineMeta.addNote( notes[ i ] );
        notes[ i ].setSelected( true );
      }

      // Set the source and target transforms ...
      for ( TransformMeta transform : transforms ) {
        ITransformMeta smi = transform.getTransform();
        smi.searchInfoAndTargetTransforms( pipelineMeta.getTransforms() );
      }

      // Set the error handling hops
      Node errorHandlingNode = XmlHandler.getSubNode( pipelineNode, PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING );
      int nrErrorHandlers = XmlHandler.countNodes( errorHandlingNode, TransformErrorMeta.XML_ERROR_TAG );
      for ( int i = 0; i < nrErrorHandlers; i++ ) {
        Node transformErrorMetaNode = XmlHandler.getSubNodeByNr( errorHandlingNode, TransformErrorMeta.XML_ERROR_TAG, i );
        TransformErrorMeta transformErrorMeta =
          new TransformErrorMeta( transformErrorMetaNode, pipelineMeta.getTransforms() );

        // Handle pasting multiple times, need to update source and target transform names
        int srcTransformPos = transformOldNames.indexOf( transformErrorMeta.getSourceTransform().getName() );
        int tgtTransformPos = transformOldNames.indexOf( transformErrorMeta.getTargetTransform().getName() );
        TransformMeta sourceTransform = pipelineMeta.findTransform( transforms[ srcTransformPos ].getName() );
        if ( sourceTransform != null ) {
          sourceTransform.setTransformErrorMeta( transformErrorMeta );
        }
        sourceTransform.setTransformErrorMeta( null );
        if ( tgtTransformPos >= 0 ) {
          sourceTransform.setTransformErrorMeta( transformErrorMeta );
          TransformMeta targetTransform = pipelineMeta.findTransform( transforms[ tgtTransformPos ].getName() );
          transformErrorMeta.setSourceTransform( sourceTransform );
          transformErrorMeta.setTargetTransform( targetTransform );
        }
      }

      // Save undo information too...
      hopGui.undoDelegate.addUndoNew( pipelineMeta, transforms, position, false );

      int[] hopPos = new int[ hops.length ];
      for ( int i = 0; i < hops.length; i++ ) {
        hopPos[ i ] = pipelineMeta.indexOfPipelineHop( hops[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( pipelineMeta, hops, hopPos, true );

      int[] notePos = new int[ notes.length ];
      for ( int i = 0; i < notes.length; i++ ) {
        notePos[ i ] = pipelineMeta.indexOfNote( notes[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( pipelineMeta, notes, notePos, true );

    } catch ( HopException e ) {
      // "Error pasting transforms...",
      // "I was unable to paste transforms to this pipeline"
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteTransforms.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.UnablePasteTransforms.Message" ), e );
    }
    pipelineGraph.redraw();
  }

  public void copySelected( PipelineMeta pipelineMeta, List<TransformMeta> transforms, List<NotePadMeta> notes ) {
    if ( transforms == null || transforms.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( 5000 ).append( XmlHandler.getXmlHeader() );
    try {
      xml.append( XmlHandler.openTag( XML_TAG_PIPELINE_TRANSFORMS ) ).append( Const.CR );

      xml.append( XmlHandler.openTag( XML_TAG_TRANSFORMS ) ).append( Const.CR );
      for ( TransformMeta transform : transforms ) {
        xml.append( transform.getXml() );
      }
      xml.append( XmlHandler.closeTag( XML_TAG_TRANSFORMS ) ).append( Const.CR );

      // Also check for the hops in between the selected transforms...
      xml.append( XmlHandler.openTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );
      for ( TransformMeta transform1 : transforms ) {
        for ( TransformMeta transform2 : transforms ) {
          if ( transform1 != transform2 ) {
            PipelineHopMeta hop = pipelineMeta.findPipelineHop( transform1, transform2, true );
            if ( hop != null ) {
              // Ok, we found one...
              xml.append( hop.getXml() ).append( Const.CR );
            }
          }
        }
      }
      xml.append( XmlHandler.closeTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );

      xml.append( XmlHandler.openTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );
      if ( notes != null ) {
        for ( NotePadMeta note : notes ) {
          xml.append( note.getXml() );
        }
      }
      xml.append( XmlHandler.closeTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );

      xml.append( XmlHandler.openTag( PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING ) ).append( Const.CR );
      for ( TransformMeta transform : transforms ) {
        if ( transform.getTransformErrorMeta() != null ) {
          xml.append( transform.getTransformErrorMeta().getXml() ).append( Const.CR );
        }
      }
      xml.append( XmlHandler.closeTag( PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING ) ).append( Const.CR );

      xml.append( XmlHandler.closeTag( XML_TAG_PIPELINE_TRANSFORMS ) ).append( Const.CR );

      toClipboard( xml.toString() );
    } catch ( Exception ex ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error encoding to XML", ex );
    }
  }


  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  /**
   * @param pipelineGraph The pipelineGraph to set
   */
  public void setPipelineGraph( HopGuiPipelineGraph pipelineGraph ) {
    this.pipelineGraph = pipelineGraph;
  }
}
