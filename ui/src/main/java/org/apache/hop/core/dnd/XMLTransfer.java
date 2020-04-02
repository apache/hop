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

package org.apache.hop.core.dnd;

import org.apache.commons.codec.binary.Base64;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.eclipse.swt.dnd.ByteArrayTransfer;
import org.eclipse.swt.dnd.TransferData;

public class XMLTransfer extends ByteArrayTransfer {
  private static final String MYTYPENAME = "HOP_XML_TRANSFER";

  private static final int MYTYPEID = registerType( MYTYPENAME );

  private static XMLTransfer _instance = new XMLTransfer();

  private ILogChannel log;

  private XMLTransfer() {
    this.log = new LogChannel( "XML DND Transfer" );
  }

  public static XMLTransfer getInstance() {
    return _instance;
  }

  public void javaToNative( Object object, TransferData transferData ) {
    if ( !checkMyType( object ) ) {
      return; // DND.error(DND.ERROR_INVALID_DATA);
    }

    try {
      byte[] buffer =
        Base64.encodeBase64( ( (DragAndDropContainer) object ).getXml().getBytes( Const.XML_ENCODING ) );

      super.javaToNative( buffer, transferData );
    } catch ( Exception e ) {
      log.logError( "Unexpected error trying to put a string onto the XML Transfer type: " + e.toString() );
      log.logError( Const.getStackTracker( e ) );
      return;
    }
  }

  boolean checkMyType( Object object ) {
    if ( object == null || !( object instanceof DragAndDropContainer ) ) {
      return false;
    }

    return true;
  }

  public Object nativeToJava( TransferData transferData ) {
    if ( isSupportedType( transferData ) ) {
      try {
        byte[] buffer = (byte[]) super.nativeToJava( transferData );
        String xml = new String( Base64.decodeBase64( new String( buffer ).getBytes() ) );
        return new DragAndDropContainer( xml );
      } catch ( Exception e ) {
        log.logError( "Unexpected error trying to read a drag and drop container from the XML Transfer type: "
          + e.toString() );
        log.logError( Const.getStackTracker( e ) );
        return null;
      }
    }
    return null;
  }

  protected String[] getTypeNames() {
    return new String[] { MYTYPENAME };
  }

  protected int[] getTypeIds() {
    return new int[] { MYTYPEID };
  }
}
