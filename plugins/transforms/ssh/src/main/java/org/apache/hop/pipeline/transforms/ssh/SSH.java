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

package org.apache.hop.pipeline.transforms.ssh;

import com.trilead.ssh2.Session;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Write commands to SSH *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class SSH extends BaseTransform<SSHMeta, SSHData> implements ITransform<SSHMeta, SSHData> {
  private static final Class<?> PKG = SSHMeta.class; // For Translator

  public SSH( TransformMeta transformMeta, SSHMeta meta, SSHData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row;
    if ( meta.isDynamicCommand() ) {
      row = getRow();
      if ( row == null ) {
        setOutputDone();
        return false;
      }
      if ( first ) {
        first = false;
        data.outputRowMeta = getInputRowMeta().clone();
        data.nrInputFields = data.outputRowMeta.size();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
        data.nrOutputFields = data.outputRowMeta.size();

        // Check if commands field is provided
        if ( meta.isDynamicCommand() ) {
          if ( Utils.isEmpty( meta.getcommandfieldname() ) ) {
            throw new HopException( BaseMessages.getString( PKG, "SSH.Error.CommandFieldMissing" ) );
          }
          // cache the position of the source filename field
          data.indexOfCommand = data.outputRowMeta.indexOfValue( meta.getcommandfieldname() );
          if ( data.indexOfCommand < 0 ) {
            // The field is unreachable !
            throw new HopException( BaseMessages.getString( PKG, "SSH.Exception.CouldnotFindField", meta
              .getcommandfieldname() ) );
          }
        }
      }
    } else {
      if ( !data.wroteOneRow ) {
        row = new Object[] {}; // empty row
        incrementLinesRead();
        data.wroteOneRow = true;
        if ( first ) {
          first = false;
          data.outputRowMeta = new RowMeta();
          data.nrInputFields = 0;
          meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
          data.nrOutputFields = data.outputRowMeta.size();
          data.commands = resolve( meta.getCommand() );
        }
      } else {
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
    }

    IRowMeta imeta = getInputRowMeta();
    if ( imeta == null ) {
      imeta = new RowMeta();
      this.setInputRowMeta( imeta );
    }
    // Reserve room
    Object[] rowData = new Object[ data.nrOutputFields ];
    for ( int i = 0; i < data.nrInputFields; i++ ) {
      rowData[ i ] = row[ i ]; // no data is changed, clone is not needed here.
    }
    int index = data.nrInputFields;

    Session session = null;
    try {
      if ( meta.isDynamicCommand() ) {
        // get commands
        data.commands = data.outputRowMeta.getString( row, data.indexOfCommand );
        if ( Utils.isEmpty( data.commands ) ) {
          throw new HopException( BaseMessages.getString( PKG, "SSH.Error.MessageEmpty" ) );
        }
      }

      // Open a session
      session = data.conn.openSession();
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "SSH.Log.SessionOpened" ) );
      }

      // execute commands
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "SSH.Log.RunningCommand", data.commands ) );
      }
      session.execCommand( data.commands );

      // Read Stdout, Sterr and exitStatus
      SessionResult sessionresult = new SessionResult( session );
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "SSH.Log.CommandRunnedCommand", data.commands, sessionresult
          .getStdOut(), sessionresult.getStdErr() ) );
      }

      // Add stdout to output
      rowData[ index++ ] = sessionresult.getStd();

      if ( !Utils.isEmpty( data.stdTypeField ) ) {
        // Add stdtype to output
        rowData[ index++ ] = sessionresult.isStdTypeErr();
      }

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "SSH.Log.OutputLine", data.outputRowMeta.getString( rowData ) ) );
      }

      putRow( data.outputRowMeta, rowData );

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "SSH.LineNumber", "" + getLinesRead() ) );
        }
      }
    } catch ( Exception e ) {

      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "SSH.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), row, 1, errorMessage, null, "SSH001" );
      }
    } finally {
      if ( session != null ) {
        session.close();
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "SSH.Log.SessionClosed" ) );
        }
      }
    }

    return true;
  }

  @Override
  public boolean init() {

    if ( super.init() ) {
      String servername = resolve( meta.getServerName() );
      int nrPort = Const.toInt( resolve( meta.getPort() ), 22 );
      String username = resolve( meta.getuserName() );
      String password = Utils.resolvePassword( variables, meta.getpassword() );
      String keyFilename = resolve( meta.getKeyFileName() );
      String passphrase = resolve( meta.getPassphrase() );
      int timeOut = Const.toInt( resolve( meta.getTimeOut() ), 0 );
      String proxyhost = resolve( meta.getProxyHost() );
      int proxyport = Const.toInt( resolve( meta.getProxyPort() ), 0 );
      String proxyusername = resolve( meta.getProxyUsername() );
      String proxypassword = resolve( meta.getProxyPassword() );

      // Check target server
      if ( Utils.isEmpty( servername ) ) {
        logError( BaseMessages.getString( PKG, "SSH.MissingServerName" ) );
      }

      // Check if username field is provided
      if ( Utils.isEmpty( meta.getuserName() ) ) {
        logError( BaseMessages.getString( PKG, "SSH.Error.UserNamedMissing" ) );
        return false;
      }

      // Get output fields
      data.stdOutField = resolve( meta.getStdOutFieldName() );
      if ( Utils.isEmpty( data.stdOutField ) ) {
        logError( BaseMessages.getString( PKG, "SSH.Error.StdOutFieldNameMissing" ) );
        return false;
      }
      data.stdTypeField = resolve( meta.getStdErrFieldName() );

      try {
        // Open connection
        data.conn =
          SSHData.OpenConnection(
            servername, nrPort, username, password, meta.isusePrivateKey(), keyFilename, passphrase, timeOut,
            this, proxyhost, proxyport, proxyusername, proxypassword );

        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "SSH.Log.ConnectionOpened" ) );
        }

      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "SSH.Error.OpeningConnection", e.getMessage() ) );
        return false;
      }

      return true;
    }
    return false;
  }

}
