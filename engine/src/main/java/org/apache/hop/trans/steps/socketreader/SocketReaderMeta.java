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

package org.apache.hop.trans.steps.socketreader;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/*
 * Created on 02-jun-2003
 *
 */

public class SocketReaderMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = SocketReaderMeta.class; // for i18n purposes, needed by Translator2!!

  private String hostname;
  private String port;
  private String bufferSize;
  private boolean compressed;

  public SocketReaderMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public String getXML() {
    StringBuilder xml = new StringBuilder();

    xml.append( "     " + XMLHandler.addTagValue( "hostname", hostname ) );
    xml.append( "     " + XMLHandler.addTagValue( "port", port ) );
    xml.append( "     " + XMLHandler.addTagValue( "buffer_size", bufferSize ) );
    xml.append( "     " + XMLHandler.addTagValue( "compressed", compressed ) );

    return xml.toString();
  }

  private void readData( Node stepnode ) {
    hostname = XMLHandler.getTagValue( stepnode, "hostname" );
    port = XMLHandler.getTagValue( stepnode, "port" );
    bufferSize = XMLHandler.getTagValue( stepnode, "buffer_size" );
    compressed = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "compressed" ) );
  }

  public void setDefault() {
    bufferSize = "3000";
    compressed = true;
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // Default: nothing changes to rowMeta
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SocketReaderMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SocketReaderMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SocketReaderMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SocketReaderMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new SocketReader( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new SocketReaderData();
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname
   *          the hostname to set
   */
  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port
   *          the port to set
   */
  public void setPort( String port ) {
    this.port = port;
  }

  public String getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize( String bufferSize ) {
    this.bufferSize = bufferSize;
  }

  /**
   * @return the compressed
   */
  public boolean isCompressed() {
    return compressed;
  }

  /**
   * @param compressed
   *          the compressed to set
   */
  public void setCompressed( boolean compressed ) {
    this.compressed = compressed;
  }
}
