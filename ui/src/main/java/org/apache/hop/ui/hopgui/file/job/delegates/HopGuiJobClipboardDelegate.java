package org.apache.hop.ui.hopgui.file.job.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiJobClipboardDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  public static final String XML_TAG_JOB_JOB_ENTRIES = "job-jobentries";

  public static final String XML_TAG_JOB_ENTRIES = "job-entries";
  private static final String XML_TAG_ENTRIES = "entries";

  private HopGui hopGui;
  private HopGuiJobGraph jobGraph;
  private LogChannelInterface log;

  public HopGuiJobClipboardDelegate( HopGui hopGui, HopGuiJobGraph jobGraph ) {
    this.hopGui = hopGui;
    this.jobGraph = jobGraph;
    this.log = hopGui.getLog();
  }

  public void toClipboard( String clipText ) {
    try {
      GUIResource.getInstance().toClipboard( clipText );
    } catch ( Throwable e ) {
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Message" ), e );
    }
  }

  public String fromClipboard() {
    try {
      return GUIResource.getInstance().fromClipboard();
    } catch ( Throwable e ) {
      new ErrorDialog(
        hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Message" ), e );
      return null;
    }
  }

  public void pasteXML( JobMeta jobMeta, String clipcontent, Point loc ) {

    try {
      Document doc = XMLHandler.loadXMLString( clipcontent );
      Node jobNode = XMLHandler.getSubNode( doc, XML_TAG_JOB_JOB_ENTRIES );
      // De-select all, re-select pasted steps...
      jobMeta.unselectAll();

      Node entriesNode = XMLHandler.getSubNode( jobNode, XML_TAG_ENTRIES );
      int nr = XMLHandler.countNodes( entriesNode, JobEntryCopy.XML_TAG );
      JobEntryCopy[] entries = new JobEntryCopy[ nr ];
      ArrayList<String> entriesOldNames = new ArrayList<>( nr );

      // Point min = new Point(loc.x, loc.y);
      Point min = new Point( 99999999, 99999999 );

      // Load the entries...
      for ( int i = 0; i < nr; i++ ) {
        Node entryNode = XMLHandler.getSubNodeByNr( entriesNode, JobEntryCopy.XML_TAG, i );
        entries[ i ] = new JobEntryCopy( entryNode, hopGui.getMetaStore() );

        if ( loc != null ) {
          Point p = entries[ i ].getLocation();

          if ( min.x > p.x ) {
            min.x = p.x;
          }
          if ( min.y > p.y ) {
            min.y = p.y;
          }
        }
      }

      // Load the hops...
      Node hopsNode = XMLHandler.getSubNode( jobNode, "order" );
      nr = XMLHandler.countNodes( hopsNode, "hop" );
      JobHopMeta[] hops = new JobHopMeta[ nr ];

      for ( int i = 0; i < nr; i++ ) {
        Node hopNode = XMLHandler.getSubNodeByNr( hopsNode, "hop", i );
        hops[ i ] = new JobHopMeta( hopNode, Arrays.asList( entries ) );
      }

      // This is the offset:
      Point offset = new Point( loc.x - min.x, loc.y - min.y );

      // Undo/redo object positions...
      int[] position = new int[ entries.length ];

      for ( int i = 0; i < entries.length; i++ ) {
        Point p = entries[ i ].getLocation();
        String name = entries[ i ].getName();
        entries[ i ].setLocation( p.x + offset.x, p.y + offset.y );

        // Check the name, find alternative...
        entriesOldNames.add( name );
        entries[ i ].setName( jobMeta.getAlternativeJobentryName( name ) );
        jobMeta.addJobEntry( entries[ i ] );
        position[ i ] = jobMeta.indexOfJobEntry( entries[ i ] );
        entries[ i ].setSelected( true );
      }

      // Add the hops too...
      //
      for ( JobHopMeta hop : hops ) {
        jobMeta.addJobHop( hop );
      }

      // Load the notes...
      Node notesNode = XMLHandler.getSubNode( jobNode, "notepads" );
      nr = XMLHandler.countNodes( notesNode, "notepad" );
      if ( log.isDebug() ) {
        // "I found "+nr+" notepads to paste."
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundNotepads", "" + nr ) );
      }
      NotePadMeta[] notes = new NotePadMeta[ nr ];

      for ( int i = 0; i < notes.length; i++ ) {
        Node noteNode = XMLHandler.getSubNodeByNr( notesNode, "notepad", i );
        notes[ i ] = new NotePadMeta( noteNode );
        Point p = notes[ i ].getLocation();
        notes[ i ].setLocation( p.x + offset.x, p.y + offset.y );
        jobMeta.addNote( notes[ i ] );
        notes[ i ].setSelected( true );
      }

      // Save undo information too...
      hopGui.undoDelegate.addUndoNew( jobMeta, entries, position, false );

      int[] hopPos = new int[ hops.length ];
      for ( int i = 0; i < hops.length; i++ ) {
        hopPos[ i ] = jobMeta.indexOfJobHop( hops[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( jobMeta, hops, hopPos, true );

      int[] notePos = new int[ notes.length ];
      for ( int i = 0; i < notes.length; i++ ) {
        notePos[ i ] = jobMeta.indexOfNote( notes[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( jobMeta, notes, notePos, true );

    } catch ( HopException e ) {
      // "Error pasting steps...",
      // "I was unable to paste steps to this pipeline"
      new ErrorDialog( hopGui.getShell(),
        BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteEntries.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteEntries.Message" ), e );
    }
    jobGraph.redraw();
  }

  public void copySelected( JobMeta jobMeta, List<JobEntryCopy> entries, List<NotePadMeta> notes ) {
    if ( entries == null || entries.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( 5000 ).append( XMLHandler.getXMLHeader() );
    try {
      xml.append( XMLHandler.openTag( XML_TAG_JOB_ENTRIES ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( XML_TAG_ENTRIES ) ).append( Const.CR );
      for ( JobEntryCopy entry : entries ) {
        xml.append( entry.getXML() );
      }
      xml.append( XMLHandler.closeTag( XML_TAG_ENTRIES ) ).append( Const.CR );

      // Also check for the hops in between the selected steps...
      xml.append( XMLHandler.openTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );
      for ( JobEntryCopy step1 : entries ) {
        for ( JobEntryCopy step2 : entries ) {
          if ( !step1.equals( step2 ) ) {
            JobHopMeta hop = jobMeta.findJobHop( step1, step2, true );
            if ( hop != null ) {
              // Ok, we found one...
              xml.append( hop.getXML() ).append( Const.CR );
            }
          }
        }
      }
      xml.append( XMLHandler.closeTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );
      if ( notes != null ) {
        for ( NotePadMeta note : notes ) {
          xml.append( note.getXML() );
        }
      }
      xml.append( XMLHandler.closeTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );

      xml.append( XMLHandler.closeTag( XML_TAG_JOB_JOB_ENTRIES ) ).append( Const.CR );

      toClipboard( xml.toString() );
    } catch ( Exception ex ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error encoding to XML", ex );
    }
  }

  public void copyJobEntries( List<JobEntryCopy> jec ) {
    if ( jec == null || jec.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( XMLHandler.getXMLHeader() );
    xml.append( XMLHandler.openTag( XML_TAG_JOB_JOB_ENTRIES ) ).append( Const.CR );

    for ( JobEntryCopy aJec : jec ) {
      xml.append( aJec.getXML() );
    }

    xml.append( "    " ).append( XMLHandler.closeTag( XML_TAG_JOB_JOB_ENTRIES ) ).append( Const.CR );

    jobGraph.jobClipboardDelegate.toClipboard( xml.toString() );
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
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiJobGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiJobGraph jobGraph ) {
    this.jobGraph = jobGraph;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public LogChannelInterface getLog() {
    return log;
  }

  /**
   * @param log The log to set
   */
  public void setLog( LogChannelInterface log ) {
    this.log = log;
  }
}
