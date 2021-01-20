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

package org.apache.hop.workflow.actions.http;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This defines an HTTP action.
 *
 * @author Matt
 * @since 05-11-2003
 */

@Action(
  id = "HTTP",
  name = "i18n::ActionHTTP.Name",
  description = "i18n::ActionHTTP.Description",
  image = "HTTP.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/http.html"
)
public class ActionHttp extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionHttp.class; // For Translator

  private static final String URL_FIELDNAME = "URL";
  private static final String UPLOADFILE_FIELDNAME = "UPLOAD";
  private static final String TARGETFILE_FIELDNAME = "DESTINATION";


  // Base info
  private String url;

  private String targetFilename;

  private boolean fileAppended;

  private boolean dateTimeAdded;

  private String targetFilenameExtension;

  // Send file content to server?
  private String uploadFilename;

  // The fieldname that contains the URL
  // Get it from a previous pipeline with Result.
  private String urlFieldname;
  private String uploadFieldname;
  private String destinationFieldname;

  private boolean runForEveryRow;

  // Proxy settings
  private String proxyHostname;

  private String proxyPort;

  private String nonProxyHosts;

  private String username;

  private String password;

  private boolean addfilenameresult;

  private String[] headerName;

  private String[] headerValue;

  public ActionHttp( String n ) {
    super( n, "" );
    url = null;
    addfilenameresult = true;
  }

  public ActionHttp() {
    this( "" );
  }

  private void allocate( int nrHeaders ) {
    headerName = new String[ nrHeaders ];
    headerValue = new String[ nrHeaders ];
  }

  @Override
  public Object clone() {
    ActionHttp je = (ActionHttp) super.clone();
    if ( headerName != null ) {
      int nrHeaders = headerName.length;
      je.allocate( nrHeaders );
      System.arraycopy( headerName, 0, je.headerName, 0, nrHeaders );
      System.arraycopy( headerValue, 0, je.headerValue, 0, nrHeaders );
    }
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "url", url ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "targetfilename", targetFilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "file_appended", fileAppended ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "date_time_added", dateTimeAdded ) );
    retval
      .append( "      " ).append( XmlHandler.addTagValue( "targetfilename_extension", targetFilenameExtension ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "uploadfilename", uploadFilename ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "run_every_row", runForEveryRow ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "url_fieldname", urlFieldname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "upload_fieldname", uploadFieldname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "dest_fieldname", destinationFieldname ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "username", username ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_host", proxyHostname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxy_port", proxyPort ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "non_proxy_hosts", nonProxyHosts ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addfilenameresult", addfilenameresult ) );
    retval.append( "      <headers>" ).append( Const.CR );
    if ( headerName != null ) {
      for ( int i = 0; i < headerName.length; i++ ) {
        retval.append( "        <header>" ).append( Const.CR );
        retval.append( "          " ).append( XmlHandler.addTagValue( "header_name", headerName[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "header_value", headerValue[ i ] ) );
        retval.append( "        </header>" ).append( Const.CR );
      }
    }
    retval.append( "      </headers>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      url = XmlHandler.getTagValue( entrynode, "url" );
      targetFilename = XmlHandler.getTagValue( entrynode, "targetfilename" );
      fileAppended = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "file_appended" ) );
      dateTimeAdded = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "date_time_added" ) );
      targetFilenameExtension = Const.NVL( XmlHandler.getTagValue( entrynode, "targetfilename_extension" ),
        XmlHandler.getTagValue( entrynode, "targetfilename_extention" ) );

      uploadFilename = XmlHandler.getTagValue( entrynode, "uploadfilename" );

      urlFieldname = XmlHandler.getTagValue( entrynode, "url_fieldname" );
      uploadFieldname = XmlHandler.getTagValue( entrynode, "upload_fieldname" );
      destinationFieldname = XmlHandler.getTagValue( entrynode, "dest_fieldname" );
      runForEveryRow = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "run_every_row" ) );

      username = XmlHandler.getTagValue( entrynode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( entrynode, "password" ) );

      proxyHostname = XmlHandler.getTagValue( entrynode, "proxy_host" );
      proxyPort = XmlHandler.getTagValue( entrynode, "proxy_port" );
      nonProxyHosts = XmlHandler.getTagValue( entrynode, "non_proxy_hosts" );
      addfilenameresult =
        "Y".equalsIgnoreCase( Const.NVL( XmlHandler.getTagValue( entrynode, "addfilenameresult" ), "Y" ) );
      Node headers = XmlHandler.getSubNode( entrynode, "headers" );

      // How many field headerName?
      int nrHeaders = XmlHandler.countNodes( headers, "header" );
      allocate( nrHeaders );
      for ( int i = 0; i < nrHeaders; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( headers, "header", i );
        headerName[ i ] = XmlHandler.getTagValue( fnode, "header_name" );
        headerValue[ i ] = XmlHandler.getTagValue( fnode, "header_value" );
      }
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load action of type 'HTTP' from XML node", xe );
    }
  }

  /**
   * @return Returns the URL.
   */
  public String getUrl() {
    return url;
  }

  /**
   * @param url The URL to set.
   */
  public void setUrl( String url ) {
    this.url = url;
  }

  /**
   * @return Returns the target filename.
   */
  public String getTargetFilename() {
    return targetFilename;
  }

  /**
   * @param targetFilename The target filename to set.
   */
  public void setTargetFilename( String targetFilename ) {
    this.targetFilename = targetFilename;
  }

  public String getNonProxyHosts() {
    return nonProxyHosts;
  }

  public void setNonProxyHosts( String nonProxyHosts ) {
    this.nonProxyHosts = nonProxyHosts;
  }

  public boolean isAddFilenameToResult() {
    return addfilenameresult;
  }

  public void setAddFilenameToResult( boolean addfilenameresult ) {
    this.addfilenameresult = addfilenameresult;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword( String password ) {
    this.password = password;
  }

  public String getProxyHostname() {
    return proxyHostname;
  }

  public void setProxyHostname( String proxyHostname ) {
    this.proxyHostname = proxyHostname;
  }

  public String getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort( String proxyPort ) {
    this.proxyPort = proxyPort;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername( String username ) {
    this.username = username;
  }

  public String[] getHeaderName() {
    return headerName;
  }

  public void setHeaderName( String[] headerName ) {
    this.headerName = headerName;
  }

  public String[] getHeaderValue() {
    return headerValue;
  }

  public void setHeaderValue( String[] headerValue ) {
    this.headerValue = headerValue;
  }

  /**
   * We made this one synchronized in the JVM because otherwise, this is not thread safe. In that case if (on an
   * application server for example) several HTTP's are running at the same time, you get into problems because the
   * System.setProperty() calls are system wide!
   */
  @Override
  public synchronized Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );

    logBasic( BaseMessages.getString( PKG, "JobHTTP.StartAction" ) );

    // Get previous result rows...
    List<RowMetaAndData> resultRows;
    String urlFieldnameToUse, uploadFieldnameToUse, destinationFieldnameToUse;

    if ( Utils.isEmpty( urlFieldname ) ) {
      urlFieldnameToUse = URL_FIELDNAME;
    } else {
      urlFieldnameToUse = urlFieldname;
    }

    if ( Utils.isEmpty( uploadFieldname ) ) {
      uploadFieldnameToUse = UPLOADFILE_FIELDNAME;
    } else {
      uploadFieldnameToUse = uploadFieldname;
    }

    if ( Utils.isEmpty( destinationFieldname ) ) {
      destinationFieldnameToUse = TARGETFILE_FIELDNAME;
    } else {
      destinationFieldnameToUse = destinationFieldname;
    }

    if ( runForEveryRow ) {
      resultRows = previousResult.getRows();
      if ( resultRows == null ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobHTTP.Error.UnableGetResultPrevious" ) );
        return result;
      }
    } else {
      resultRows = new ArrayList<>();
      RowMetaAndData row = new RowMetaAndData();
      row.addValue(
        new ValueMetaString( urlFieldnameToUse ), resolve( url ) );
      row.addValue(
        new ValueMetaString( uploadFieldnameToUse ), resolve( uploadFilename ) );
      row.addValue(
        new ValueMetaString( destinationFieldnameToUse ), resolve( targetFilename ) );
      resultRows.add( row );
    }

    URL server = null;

    String beforeProxyHost = getVariable( "http.proxyHost" );
    String beforeProxyPort = getVariable( "http.proxyPort" );
    String beforeNonProxyHosts = getVariable( "http.nonProxyHosts" );

    for ( int i = 0; i < resultRows.size() && result.getNrErrors() == 0; i++ ) {
      RowMetaAndData row = resultRows.get( i );

      OutputStream outputFile = null;
      OutputStream uploadStream = null;
      BufferedInputStream fileStream = null;
      InputStream input = null;

      try {
        String urlToUse = resolve( row.getString( urlFieldnameToUse, "" ) );
        String realUploadFile = resolve( row.getString( uploadFieldnameToUse, "" ) );
        String realTargetFile = resolve( row.getString( destinationFieldnameToUse, "" ) );

        logBasic( BaseMessages.getString( PKG, "JobHTTP.Log.ConnectingURL", urlToUse ) );

        if ( !Utils.isEmpty( proxyHostname ) ) {
          System.setProperty( "http.proxyHost", resolve( proxyHostname ) );
          System.setProperty( "http.proxyPort", resolve( proxyPort ) );
          if ( nonProxyHosts != null ) {
            System.setProperty( "http.nonProxyHosts", resolve( nonProxyHosts ) );
          }
        }

        if ( !Utils.isEmpty( username ) ) {
          Authenticator.setDefault( new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
              String realPassword = Encr.decryptPasswordOptionallyEncrypted( resolve( password ) );
              return new PasswordAuthentication( resolve( username ), realPassword != null
                ? realPassword.toCharArray() : new char[] {} );
            }
          } );
        }

        if ( dateTimeAdded ) {
          SimpleDateFormat daf = new SimpleDateFormat();
          Date now = new Date();

          daf.applyPattern( "yyyMMdd" );
          realTargetFile += "_" + daf.format( now );
          daf.applyPattern( "HHmmss" );
          realTargetFile += "_" + daf.format( now );

          if ( !Utils.isEmpty( targetFilenameExtension ) ) {
            realTargetFile += "." + resolve( targetFilenameExtension );
          }
        }

        // Create the output File...
        outputFile = HopVfs.getOutputStream( realTargetFile, fileAppended );

        // Get a stream for the specified URL
        server = new URL( urlToUse );
        URLConnection connection = server.openConnection();

        // if we have HTTP headers, add them
        if ( !Utils.isEmpty( headerName ) ) {
          if ( log.isDebug() ) {
            log.logDebug( BaseMessages.getString( PKG, "JobHTTP.Log.HeadersProvided" ) );
          }
          for ( int j = 0; j < headerName.length; j++ ) {
            if ( !Utils.isEmpty( headerValue[ j ] ) ) {
              connection.setRequestProperty(
                resolve( headerName[ j ] ), resolve( headerValue[ j ] ) );
              if ( log.isDebug() ) {
                log.logDebug( BaseMessages.getString(
                  PKG, "JobHTTP.Log.HeaderSet", resolve( headerName[ j ] ),
                  resolve( headerValue[ j ] ) ) );
              }
            }
          }
        }

        connection.setDoOutput( true );

        // See if we need to send a file over?
        if ( !Utils.isEmpty( realUploadFile ) ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobHTTP.Log.SendingFile", realUploadFile ) );
          }

          // Grab an output stream to upload data to web server
          uploadStream = connection.getOutputStream();
          fileStream = new BufferedInputStream( new FileInputStream( new File( realUploadFile ) ) );
          try {
            int c;
            while ( ( c = fileStream.read() ) >= 0 ) {
              uploadStream.write( c );
            }
          } finally {
            // Close upload and file
            if ( uploadStream != null ) {
              uploadStream.close();
              uploadStream = null;
            }
            if ( fileStream != null ) {
              fileStream.close();
              fileStream = null;
            }
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobHTTP.Log.FinishedSendingFile" ) );
          }
        }

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobHTTP.Log.StartReadingReply" ) );
        }

        // Read the result from the server...
        input = connection.getInputStream();
        Date date = new Date( connection.getLastModified() );
        logBasic( BaseMessages.getString( PKG, "JobHTTP.Log.ReplayInfo", connection.getContentType(), date ) );

        int oneChar;
        long bytesRead = 0L;
        while ( ( oneChar = input.read() ) != -1 ) {
          outputFile.write( oneChar );
          bytesRead++;
        }

        logBasic( BaseMessages.getString( PKG, "JobHTTP.Log.FinisedWritingReply", bytesRead, realTargetFile ) );

        if ( addfilenameresult ) {
          // Add to the result files...
          ResultFile resultFile =
            new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( realTargetFile ), parentWorkflow
              .getWorkflowName(), toString() );
          result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
        }

        result.setResult( true );
      } catch ( MalformedURLException e ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobHTTP.Error.NotValidURL", url, e.getMessage() ) );
        logError( Const.getStackTracker( e ) );
      } catch ( IOException e ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobHTTP.Error.CanNotSaveHTTPResult", e.getMessage() ) );
        logError( Const.getStackTracker( e ) );
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobHTTP.Error.ErrorGettingFromHTTP", e.getMessage() ) );
        logError( Const.getStackTracker( e ) );
      } finally {
        // Close it all
        try {
          if ( uploadStream != null ) {
            uploadStream.close(); // just to make sure
          }
          if ( fileStream != null ) {
            fileStream.close(); // just to make sure
          }

          if ( input != null ) {
            input.close();
          }
          if ( outputFile != null ) {
            outputFile.close();
          }
        } catch ( Exception e ) {
          logError( BaseMessages.getString( PKG, "JobHTTP.Error.CanNotCloseStream", e.getMessage() ) );
          result.setNrErrors( 1 );
        }

        // Set the proxy settings back as they were on the system!
        System.setProperty( "http.proxyHost", Const.NVL( beforeProxyHost, "" ) );
        System.setProperty( "http.proxyPort", Const.NVL( beforeProxyPort, "" ) );
        System.setProperty( "http.nonProxyHosts", Const.NVL( beforeNonProxyHosts, "" ) );
      }

    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  public String getUploadFilename() {
    return uploadFilename;
  }

  public void setUploadFilename( String uploadFilename ) {
    this.uploadFilename = uploadFilename;
  }

  /**
   * @return Returns the Result URL Fieldname.
   */
  public String getUrlFieldname() {
    return urlFieldname;
  }

  /**
   * @param getFieldname The Result URL Fieldname to set.
   */
  public void setUrlFieldname( String getFieldname ) {
    this.urlFieldname = getFieldname;
  }

  /*
   * @return Returns the Upload File Fieldname
   */
  public String getUploadFieldname() {
    return uploadFieldname;
  }

  /*
   * @param uploadFieldName
   *         The Result Upload Fieldname to use
   */
  public void setUploadFieldname( String uploadFieldname ) {
    this.uploadFieldname = uploadFieldname;
  }

  /*
   * @return Returns the Result Destination Path Fieldname
   */
  public String getDestinationFieldname() {
    return destinationFieldname;
  }

  /*
   * @param destinationFieldname
   *           The Result Destination Fieldname to set.
   */
  public void setDestinationFieldname( String destinationFieldname ) {
    this.destinationFieldname = destinationFieldname;
  }

  /**
   * @return Returns the runForEveryRow.
   */
  public boolean isRunForEveryRow() {
    return runForEveryRow;
  }

  /**
   * @param runForEveryRow The runForEveryRow to set.
   */
  public void setRunForEveryRow( boolean runForEveryRow ) {
    this.runForEveryRow = runForEveryRow;
  }

  /**
   * @return Returns the fileAppended.
   */
  public boolean isFileAppended() {
    return fileAppended;
  }

  /**
   * @param fileAppended The fileAppended to set.
   */
  public void setFileAppended( boolean fileAppended ) {
    this.fileAppended = fileAppended;
  }

  /**
   * @return Returns the dateTimeAdded.
   */
  public boolean isDateTimeAdded() {
    return dateTimeAdded;
  }

  /**
   * @param dateTimeAdded The dateTimeAdded to set.
   */
  public void setDateTimeAdded( boolean dateTimeAdded ) {
    this.dateTimeAdded = dateTimeAdded;
  }

  /**
   * @return Returns the uploadFilenameExtension.
   */
  public String getTargetFilenameExtension() {
    return targetFilenameExtension;
  }

  /**
   * @param uploadFilenameExtension The uploadFilenameExtension to set.
   */
  public void setTargetFilenameExtension( String uploadFilenameExtension ) {
    this.targetFilenameExtension = uploadFilenameExtension;
  }

  /**
   * @return Returns the uploadFilenameExtension.
   * @deprecated Use {@link ActionHttp#getTargetFilenameExtension()} instead
   */
  @Deprecated
  public String getTargetFilenameExtention() {
    return targetFilenameExtension;
  }

  /**
   * @param uploadFilenameExtension The uploadFilenameExtension to set.
   * @deprecated Use {@link ActionHttp#setTargetFilenameExtension(String uploadFilenameExtension)} instead
   */
  @Deprecated
  public void setTargetFilenameExtention( String uploadFilenameExtension ) {
    this.targetFilenameExtension = uploadFilenameExtension;
  }

  @Override
  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    String realUrl = resolve( url );
    ResourceReference reference = new ResourceReference( this );
    reference.getEntries().add( new ResourceEntry( realUrl, ResourceType.URL ) );
    references.add( reference );
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "targetFilename", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "targetFilenameExtention", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "uploadFilename", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "proxyPort", remarks,
      AndValidator.putValidators( ActionValidatorUtils.integerValidator() ) );
  }

}
