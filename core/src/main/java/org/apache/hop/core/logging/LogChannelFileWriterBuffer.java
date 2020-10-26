/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.core.logging;

import org.apache.hop.core.Const;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class LogChannelFileWriterBuffer {

  private HopLogLayout layout;

  private String logChannelId;

  private final List<HopLoggingEvent> buffer = Collections.synchronizedList( new LinkedList<HopLoggingEvent>() );

  public LogChannelFileWriterBuffer( String logChannelId ) {
    this.logChannelId = logChannelId;
    layout = new HopLogLayout( true );
  }

  public void addEvent( HopLoggingEvent event ) {
    synchronized ( buffer ) {
      buffer.add( event );
    }
  }

  public StringBuffer getBuffer() {
    StringBuffer stringBuffer = new StringBuffer( 1000 );

    synchronized ( buffer ) {
      for ( HopLoggingEvent event : buffer ) {
        stringBuffer.append( layout.format( event ) ).append( Const.CR );
      }

      buffer.clear();
    }

    return stringBuffer;
  }

  public String getLogChannelId() {
    return logChannelId;
  }
}
