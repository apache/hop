/*! ******************************************************************************
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

package org.apache.hop.ui.hopui.trans;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.HasLogChannelInterface;
import org.apache.hop.core.logging.HopLogLayout;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogParentProvidedInterface;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.text.Format;
import org.apache.hop.ui.core.widget.text.TextFormatter;
import org.apache.hop.ui.hopui.HopUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

import java.awt.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LogBrowser {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  private StyledText text;
  private LogParentProvidedInterface logProvider;
  private List<String> childIds = new ArrayList<>();
  private Date lastLogRegistryChange;
  private AtomicBoolean paused;

  public LogBrowser( final StyledText text, final LogParentProvidedInterface logProvider ) {
    this.text = text;
    this.logProvider = logProvider;
    this.paused = new AtomicBoolean( false );
  }

  public void installLogSniffer() {

    // Create a new buffer appender to the log and capture that directly...
    //
    final AtomicInteger lastLogId = new AtomicInteger( -1 );
    final AtomicBoolean busy = new AtomicBoolean( false );
    final HopLogLayout logLayout = new HopLogLayout( true );

    final StyleRange normalLogLineStyle = new StyleRange();
    normalLogLineStyle.foreground = GUIResource.getInstance().getColorBlue();
    final StyleRange errorLogLineStyle = new StyleRange();
    errorLogLineStyle.foreground = GUIResource.getInstance().getColorRed();

    // Refresh the log every second or so
    //
    final Timer logRefreshTimer = new Timer( "log sniffer Timer" );
    TimerTask timerTask = new TimerTask() {
      public void run() {
        if ( text.isDisposed() ) {
          return;
        }

        text.getDisplay().asyncExec( new Runnable() {
          public void run() {
            HasLogChannelInterface provider = logProvider.getLogChannelProvider();

            if ( provider != null && !text.isDisposed() && !busy.get() && !paused.get() && text.isVisible() ) {
              busy.set( true );

              LogChannelInterface logChannel = provider.getLogChannel();
              String parentLogChannelId = logChannel.getLogChannelId();
              LoggingRegistry registry = LoggingRegistry.getInstance();
              Date registryModDate = registry.getLastModificationTime();

              if ( childIds == null
                || lastLogRegistryChange == null || registryModDate.compareTo( lastLogRegistryChange ) > 0 ) {
                lastLogRegistryChange = registry.getLastModificationTime();
                childIds = LoggingRegistry.getInstance().getLogChannelChildren( parentLogChannelId );
              }

              // See if we need to log any lines...
              //
              int lastNr = HopLogStore.getLastBufferLineNr();
              if ( lastNr > lastLogId.get() ) {
                List<HopLoggingEvent> logLines =
                  HopLogStore.getLogBufferFromTo( childIds, true, lastLogId.get(), lastNr );

                // The maximum size of the log buffer
                //
                int maxSize = Props.getInstance().getMaxNrLinesInLog() * 150;

                // int position = text.getSelection().x;
                // StringBuilder buffer = new StringBuilder(text.getText());

                synchronized ( text ) {

                  for ( int i = 0; i < logLines.size(); i++ ) {
                    HopLoggingEvent event = logLines.get( i );
                    String line = logLayout.format( event ).trim();
                    int start = text.getText().length();
                    int length = line.length();

                    if ( length > 0 ) {
                      Format format = TextFormatter.getInstance().execute( line );
                      text.append( format.getText() );
                      text.append( Const.CR );

                      for ( StyleRange styleRange : format.getStyleRanges() ) {
                        styleRange.start += start;
                        text.setStyleRange( styleRange );
                      }

                      if ( event.getLevel() == LogLevel.ERROR ) {
                        StyleRange styleRange = new StyleRange();
                        styleRange.foreground = GUIResource.getInstance().getColorRed();
                        styleRange.start = start;
                        styleRange.length = length;
                        text.setStyleRange( styleRange );
                      } else {
                        StyleRange styleRange = new StyleRange();
                        styleRange.foreground = GUIResource.getInstance().getColorBlue();
                        styleRange.start = start;
                        styleRange.length = Math.min( 20, length );
                        text.setStyleRange( styleRange );
                      }
                    }
                  }
                }

                // Erase it all in one go
                // This makes it a bit more efficient
                //
                int size = text.getText().length();
                if ( maxSize > 0 && size > maxSize ) {

                  int dropIndex = ( text.getText().indexOf( Const.CR, size - maxSize ) ) + Const.CR.length();
                  text.replaceTextRange( 0, dropIndex, "" );
                }

                text.setSelection( text.getText().length() );

                lastLogId.set( lastNr );
              }

              busy.set( false );
            }
          }
        } );
      }
    };

    // Refresh every often enough
    //
    logRefreshTimer
      .schedule( timerTask, Const.toInt( EnvUtil.getSystemProperty( Const.HOP_LOG_TAB_REFRESH_DELAY ), 1000 ),
        Const.toInt( EnvUtil.getSystemProperty( Const.HOP_LOG_TAB_REFRESH_PERIOD ), 1000 ) );


    text.addListener( SWT.MouseDown, e -> {
      try {
        int offset = text.getOffsetAtLocation( new Point( e.x, e.y ) );
        StyleRange style = text.getStyleRangeAtOffset( offset );
        if ( style != null && style.underline && style.underlineStyle == SWT.UNDERLINE_LINK ) {
          if ( Desktop.isDesktopSupported() ) {
            Desktop.getDesktop().browse( new URI( (String) style.data ) );
          }
        }
      } catch ( Exception ex ) {
        // no character under event.x, event.y
      }
    } );

    // Make sure the timer goes down when the widget is disposed
    //
    text.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent event ) {
        logRefreshTimer.cancel();
      }
    } );

    final Menu menu = new Menu( text );
    MenuItem item = new MenuItem( menu, SWT.NONE );
    item.setText( BaseMessages.getString( PKG, "LogBrowser.CopySelectionToClipboard.MenuItem" ) );
    item.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent event ) {
        String selection = text.getSelectionText();
        if ( !Utils.isEmpty( selection ) ) {
          GUIResource.getInstance().toClipboard( selection );
        }
      }
    } );
    text.setMenu( menu );

    text.addMouseListener( new MouseAdapter() {
      public void mouseDown( MouseEvent event ) {
        if ( event.button == 3 ) {
          ConstUI.displayMenu( menu, text );
        }
      }
    } );
  }

  /**
   * @return the text
   */
  public StyledText getText() {
    return text;
  }

  public LogParentProvidedInterface getLogProvider() {
    return logProvider;
  }

  public boolean isPaused() {
    return paused.get();
  }

  public void setPaused( boolean paused ) {
    this.paused.set( paused );
  }
}
