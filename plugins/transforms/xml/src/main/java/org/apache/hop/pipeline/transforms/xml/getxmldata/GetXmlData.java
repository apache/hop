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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.dom4j.Element;
import org.dom4j.ElementHandler;
import org.dom4j.ElementPath;
import org.dom4j.Namespace;
import org.dom4j.Node;
import org.dom4j.XPath;
import org.dom4j.io.SAXReader;
import org.dom4j.tree.AbstractNode;

import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;


/**
 * Read XML files, parse them and convert them to rows and writes these to one or more output streams.
 * 
 * @author Samatar,Brahim
 * @since 20-06-2007
 */
public class GetXmlData extends BaseTransform<GetXmlDataMeta, GetXmlDataData> implements ITransform<GetXmlDataMeta, GetXmlDataData> {
  private static final Class<?> PKG = GetXmlDataMeta.class; // For Translator

  private Object[] prevRow = null; // A pre-allocated spot for the previous row

  public GetXmlData(TransformMeta transformMeta, GetXmlDataMeta meta, GetXmlDataData data, int copyNr, PipelineMeta pipelineMeta,
                    Pipeline trans ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, trans );
  }

  protected boolean setDocument( String StringXML, FileObject file, boolean IsInXMLField, boolean readurl )
    throws HopException {

    this.prevRow = buildEmptyRow(); // pre-allocate previous row

    try {
      SAXReader reader = XmlParserFactoryProducer.getSAXReader( null );
      data.stopPruning = false;
      // Validate XML against specified schema?
      if ( meta.isValidating() ) {
        reader.setValidation( true );
        reader.setFeature( "http://apache.org/xml/features/validation/schema", true );
      } else {
        // Ignore DTD declarations
        reader.setEntityResolver( new IgnoreDtdEntityResolver() );
      }

      // Ignore comments?
      if ( meta.isIgnoreComments() ) {
        reader.setIgnoreComments( true );
      }

      if ( data.prunePath != null ) {
        // when pruning is on: reader.read() below will wait until all is processed in the handler
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.Activated" ) );
        }
        if ( data.PathValue.equals( data.prunePath ) ) {
          // Edge case, but if true, there will only ever be one item in the list
          data.an = new ArrayList<>( 1 ); // pre-allocate array and sizes
          data.an.add( null );
        }
        reader.addHandler( data.prunePath, new ElementHandler() {
          public void onStart( ElementPath path ) {
            // do nothing here...
          }

          public void onEnd( ElementPath path ) {
            long rowLimit = meta.getRowLimit();
            if ( isStopped() || ( rowLimit > 0 && data.rownr > rowLimit ) ) {
              // when a large file is processed and it should be stopped it is still reading the hole thing
              // the only solution I see is to prune / detach the document and this will lead into a
              // NPE or other errors depending on the parsing location - this will be treated in the catch part below
              // any better idea is welcome
              if ( log.isBasic() ) {
                logBasic( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.Stopped" ) );
              }
              data.stopPruning = true;
              path.getCurrent().getDocument().detach(); // trick to stop reader
              return;
            }

            // process a ROW element
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.StartProcessing" ) );
            }
            Element row = path.getCurrent();
            try {
              // Pass over the row instead of just the document. If
              // if there's only one row, there's no need to
              // go back to the whole document.
              processStreaming( row );
            } catch ( Exception e ) {
              // catch the HopException or others and forward to caller, e.g. when applyXPath() has a problem
              throw new RuntimeException( e );
            }
            // prune the tree
            row.detach();
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.EndProcessing" ) );
            }
          }
        } );
      }

      if ( IsInXMLField ) {
        // read string to parse
        data.document = reader.read( new StringReader( StringXML ) );
      } else if ( readurl && HopVfs.startsWithScheme( StringXML ) ) {
        data.document = reader.read( HopVfs.getInputStream( StringXML ) );
      } else if ( readurl ) {
        // read url as source
        HttpClient client = HttpClientManager.getInstance().createDefaultClient();
        HttpGet method = new HttpGet( StringXML );
        method.addHeader( "Accept-Encoding", "gzip" );
        HttpResponse response = client.execute( method );
        Header contentEncoding = response.getFirstHeader( "Content-Encoding" );
        HttpEntity responseEntity = response.getEntity();
        if ( responseEntity != null ) {
          if ( contentEncoding != null ) {
            String acceptEncodingValue = contentEncoding.getValue();
            if ( acceptEncodingValue.contains( "gzip" ) ) {
              GZIPInputStream in = new GZIPInputStream( responseEntity.getContent() );

              data.document = reader.read( in );
            }
          } else {
            data.document = reader.read( responseEntity.getContent() );
          }
        }
      } else {
        // get encoding. By default UTF-8
        String encoding = "UTF-8";
        if ( !Utils.isEmpty( meta.getEncoding() ) ) {
          encoding = meta.getEncoding();
        }
        InputStream is = HopVfs.getInputStream( file );
        try {
          data.document = reader.read( is, encoding );
        } finally {
          BaseTransform.closeQuietly( is );
        }
      }

      if ( meta.isNamespaceAware() ) {
        prepareNSMap( data.document.getRootElement() );
      }
    } catch ( Exception e ) {
      if ( data.stopPruning ) {
        // ignore error when pruning
        return false;
      } else {
        throw new HopException( e );
      }
    }
    return true;
  }

  /**
   * Process chunk of data in streaming mode. Called only by the handler when pruning is true. Not allowed in
   * combination with meta.getIsInFields(), but could be redesigned later on.
   * 
   */
  private void processStreaming( Element row ) throws HopException {
    data.document = row.getDocument();

    if ( meta.isNamespaceAware() ) {
      prepareNSMap( data.document.getRootElement() );
    }
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.ApplyXPath" ) );
    }
    // If the prune path and the path are the same, then
    // we're processing one row at a time through here.
    if ( data.PathValue.equals( data.prunePath ) ) {
      data.an.set( 0, (AbstractNode) row );
      data.nodesize = 1; // it's always just one row.
      data.nodenr = 0;
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.ProcessingRows" ) );
      }
      Object[] r = getXmlRowPutRowWithErrorhandling();
      if ( !data.errorInRowButContinue ) { // do not put out the row but continue
        putRowOut( r ); // false when limit is reached, functionality is there but we can not stop reading the hole file
                        // (slow but works)
      }
      data.nodesize = 0;
      data.nodenr = 0;
      return;
    } else {
      if ( !applyXPath() ) {
        throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.UnableApplyXPath" ) );
      }
    }
    // main loop through the data until limit is reached or transformation is stopped
    // similar functionality like in BaseTransform.runTransformThread
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.ProcessingRows" ) );
    }
    boolean cont = true;
    while ( data.nodenr < data.nodesize && cont && !isStopped() ) {
      Object[] r = getXmlRowPutRowWithErrorhandling();
      if ( data.errorInRowButContinue ) {
        continue; // do not put out the row but continue
      }
      cont = putRowOut( r ); // false when limit is reached, functionality is there but we can not stop reading the hole
                             // file (slow but works)
    }
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "GetXMLData.Log.StreamingMode.FreeMemory" ) );
    }
    // free allocated memory
    data.an.clear();
    data.nodesize = data.an.size();
    data.nodenr = 0;
  }

  public void prepareNSMap( Element l ) {
    @SuppressWarnings( "unchecked" )
    List<Namespace> namespacesList = l.declaredNamespaces();
    for ( Namespace ns : namespacesList ) {
      if ( ns.getPrefix().trim().length() == 0 ) {
        data.NAMESPACE.put( "pre" + data.NSPath.size(), ns.getURI() );
        String path = "";
        Element element = l;
        while ( element != null ) {
          if ( element.getNamespacePrefix() != null && element.getNamespacePrefix().length() > 0 ) {
            path = GetXmlDataMeta.N0DE_SEPARATOR + element.getNamespacePrefix() + ":" + element.getName() + path;
          } else {
            path = GetXmlDataMeta.N0DE_SEPARATOR + element.getName() + path;
          }
          element = element.getParent();
        }
        data.NSPath.add( path );
      } else {
        data.NAMESPACE.put( ns.getPrefix(), ns.getURI() );
      }
    }

    @SuppressWarnings( "unchecked" )
    List<Element> elementsList = l.elements();
    for ( Element e : elementsList ) {
      prepareNSMap( e );
    }
  }

  /**
   * Build an empty row based on the meta-data.
   * 
   * @return empty row built
   */
  private Object[] buildEmptyRow() {
    return RowDataUtil.allocateRowData( data.outputRowMeta.size() );
  }

  private void handleMissingFiles() throws HopException {
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();
    if ( nonExistantFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonExistantFiles );
      logError( BaseMessages.getString( PKG, "GetXMLData.Log.RequiredFilesTitle" ), BaseMessages.getString( PKG,
          "GetXMLData.Log.RequiredFiles", message ) );

      throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.RequiredFilesMissing", message ) );
    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if ( nonAccessibleFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonAccessibleFiles );
      logError( BaseMessages.getString( PKG, "GetXMLData.Log.RequiredFilesTitle" ), BaseMessages.getString( PKG,
          "GetXMLData.Log.RequiredNotAccessibleFiles", message ) );

      throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.RequiredNotAccessibleFilesMissing",
          message ) );
    }
  }

  private boolean ReadNextString() {

    try {
      // Grab another row ...
      data.readrow = getRow();

      if ( data.readrow == null ) {
        // finished processing!

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.FinishedProcessing" ) );
        }
        return false;
      }

      if ( first ) {
        first = false;

        data.nrReadRow = getInputRowMeta().size();
        data.inputRowMeta = getInputRowMeta();
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

        // Get total previous fields
        data.totalpreviousfields = data.inputRowMeta.size();

        // Create convert meta-data objects that will contain Date & Number formatters
        data.convertRowMeta = new RowMeta();
        for ( IValueMeta valueMeta : data.convertRowMeta.getValueMetaList() ) {
          data.convertRowMeta
              .addValueMeta( ValueMetaFactory.cloneValueMeta( valueMeta, IValueMeta.TYPE_STRING ) );
        }

        // For String to <type> conversions, we allocate a conversion meta data row as well...
        //
        data.convertRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );

        // Check is XML field is provided
        if ( Utils.isEmpty( meta.getXMLField() ) ) {
          logError( BaseMessages.getString( PKG, "GetXMLData.Log.NoField" ) );
          throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.NoField" ) );
        }

        // cache the position of the field
        if ( data.indexOfXmlField < 0 ) {
          data.indexOfXmlField = getInputRowMeta().indexOfValue( meta.getXMLField() );
          if ( data.indexOfXmlField < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "GetXMLData.Log.ErrorFindingField", meta.getXMLField() ) );
            throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Exception.CouldnotFindField", meta
                .getXMLField() ) );
          }
        }
      }

      if ( meta.isInFields() ) {
        // get XML field value
        String Fieldvalue = getInputRowMeta().getString( data.readrow, data.indexOfXmlField );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.XMLStream", meta.getXMLField(), Fieldvalue ) );
        }

        if ( meta.getIsAFile() ) {
          FileObject file = null;
          try {
            // XML source is a file.
            file = HopVfs.getFileObject( resolve( Fieldvalue ) );

            if ( meta.isIgnoreEmptyFile() && file.getContent().getSize() == 0 ) {
              logBasic( BaseMessages.getString( PKG, "GetXMLData.Error.FileSizeZero", "" + file.getName() ) );
              return ReadNextString();
            }

            // Open the XML document
            if ( !setDocument( null, file, false, false ) ) {
              throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.UnableCreateDocument" ) );
            }

            if ( !applyXPath() ) {
              throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.UnableApplyXPath" ) );
            }

            addFileToResultFilesname( file );

            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.LoopFileOccurences", "" + data.nodesize, file
                  .getName().getBaseName() ) );
            }

          } catch ( Exception e ) {
            throw new HopException( e );
          } finally {
            try {
              if ( file != null ) {
                file.close();
              }
            } catch ( Exception e ) {
              // Ignore close errors
            }
          }
        } else {
          boolean url = false;
          boolean xmltring = true;
          if ( meta.isReadUrl() ) {
            url = true;
            xmltring = false;
          }

          // Open the XML document
          if ( !setDocument( Fieldvalue, null, xmltring, url ) ) {
            throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.UnableCreateDocument" ) );
          }

          // Apply XPath and set node list
          if ( !applyXPath() ) {
            throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.UnableApplyXPath" ) );
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.LoopFileOccurences", "" + data.nodesize ) );
          }
        }
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "GetXMLData.Log.UnexpectedError", e.toString() ) );
      stopAll();
      logError( Const.getStackTracker( e ) );
      setErrors( 1 );
      return false;
    }
    return true;

  }

  private void addFileToResultFilesname( FileObject file ) throws Exception {
    if ( meta.addResultFile() ) {
      // Add this to the result file names...
      ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName() );
      resultFile.setComment( BaseMessages.getString( PKG, "GetXMLData.Log.FileAddedResult" ) );
      addResultFile( resultFile );
    }
  }

  public String addNSPrefix( String path, String loopPath ) {
    if ( data.NSPath.size() > 0 ) {
      String fullPath = loopPath;
      if ( !path.equals( fullPath ) ) {
        for ( String tmp : path.split( GetXmlDataMeta.N0DE_SEPARATOR ) ) {
          if ( tmp.equals("src/main") ) {
            fullPath = fullPath.substring( 0, fullPath.lastIndexOf( GetXmlDataMeta.N0DE_SEPARATOR ) );
          } else {
            fullPath += GetXmlDataMeta.N0DE_SEPARATOR + tmp;
          }
        }
      }
      int[] indexs = new int[fullPath.split( GetXmlDataMeta.N0DE_SEPARATOR ).length - 1];
      java.util.Arrays.fill( indexs, -1 );
      int length = 0;
      for ( int i = 0; i < data.NSPath.size(); i++ ) {
        if ( data.NSPath.get( i ).length() > length && fullPath.startsWith( data.NSPath.get( i ) ) ) {
          java.util.Arrays.fill( indexs, data.NSPath.get( i ).split( GetXmlDataMeta.N0DE_SEPARATOR ).length - 2,
              indexs.length, i );
          length = data.NSPath.get( i ).length();
        }
      }

      StringBuilder newPath = new StringBuilder();
      String[] pathStrs = path.split( GetXmlDataMeta.N0DE_SEPARATOR );
      for ( int i = 0; i < pathStrs.length; i++ ) {
        String tmp = pathStrs[i];
        if ( newPath.length() > 0 ) {
          newPath.append( GetXmlDataMeta.N0DE_SEPARATOR );
        }
        if ( tmp.length() > 0 && !tmp.contains( ":" ) && !tmp.contains( "." ) && !tmp.contains( GetXmlDataMeta.AT ) ) {
          int index = indexs[i + indexs.length - pathStrs.length];
          if ( index >= 0 ) {
            newPath.append( "pre" ).append( index ).append( ":" ).append( tmp );
          } else {
            newPath.append( tmp );
          }
        } else {
          newPath.append( tmp );
        }
      }
      return newPath.toString();
    }
    return path;
  }

  @SuppressWarnings( "unchecked" )
  private boolean applyXPath() {
    try {
      XPath xpath = data.document.createXPath( data.PathValue );
      if ( meta.isNamespaceAware() ) {
        xpath = data.document.createXPath( addNSPrefix( data.PathValue, data.PathValue ) );
        xpath.setNamespaceURIs( data.NAMESPACE );
      }
      // get nodes list
      data.an = xpath.selectNodes( data.document );
      data.nodesize = data.an.size();
      data.nodenr = 0;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "GetXMLData.Log.ErrorApplyXPath", e.getMessage() ) );
      return false;
    }
    return true;
  }

  private boolean openNextFile() {
    try {
      if ( data.filenr >= data.files.nrOfFiles() ) {
        // finished processing!

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.FinishedProcessing" ) );
        }
        return false;
      }
      // get file
      data.file = data.files.getFile( data.filenr );
      data.filename = HopVfs.getFilename( data.file );
      // Add additional fields?
      if ( meta.getShortFileNameField() != null && meta.getShortFileNameField().length() > 0 ) {
        data.shortFilename = data.file.getName().getBaseName();
      }
      if ( meta.getPathField() != null && meta.getPathField().length() > 0 ) {
        data.path = HopVfs.getFilename( data.file.getParent() );
      }
      if ( meta.isHiddenField() != null && meta.isHiddenField().length() > 0 ) {
        data.hidden = data.file.isHidden();
      }
      if ( meta.getExtensionField() != null && meta.getExtensionField().length() > 0 ) {
        data.extension = data.file.getName().getExtension();
      }
      if ( meta.getLastModificationDateField() != null && meta.getLastModificationDateField().length() > 0 ) {
        data.lastModificationDateTime = new Date( data.file.getContent().getLastModifiedTime() );
      }
      if ( meta.getUriField() != null && meta.getUriField().length() > 0 ) {
        data.uriName = data.file.getName().getURI();
      }
      if ( meta.getRootUriField() != null && meta.getRootUriField().length() > 0 ) {
        data.rootUriName = data.file.getName().getRootURI();
      }
      // Check if file is empty
      long fileSize;
      try {
        fileSize = data.file.getContent().getSize();
      } catch ( FileSystemException e ) {
        fileSize = -1;
      }

      if ( meta.getSizeField() != null && meta.getSizeField().length() > 0 ) {
        data.size = fileSize;
      }
      // Move file pointer ahead!
      data.filenr++;

      if ( meta.isIgnoreEmptyFile() && fileSize == 0 ) {
        // log only basic as a warning (was before logError)
        logBasic( BaseMessages.getString( PKG, "GetXMLData.Error.FileSizeZero", "" + data.file.getName() ) );
        openNextFile();

      } else {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.OpeningFile", data.file.toString() ) );
        }

        // Open the XML document
        if ( !setDocument( null, data.file, false, false ) ) {
          if ( data.stopPruning ) {
            return false; // ignore error when stopped while pruning
          }
          throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.UnableCreateDocument" ) );
        }

        // Apply XPath and set node list
        if ( data.prunePath == null ) { // this was already done in processStreaming()
          if ( !applyXPath() ) {
            throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.UnableApplyXPath" ) );
          }
        }

        addFileToResultFilesname( data.file );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.FileOpened", data.file.toString() ) );
          logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.LoopFileOccurences", "" + data.nodesize, data.file
              .getName().getBaseName() ) );
        }
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "GetXMLData.Log.UnableToOpenFile", "" + data.filenr, data.file.toString(),
          e.toString() ) );
      stopAll();
      setErrors( 1 );
      return false;
    }
    return true;
  }

  public boolean processRow( ) throws HopException {
    if ( first && !meta.isInFields() ) {
      first = false;

      data.files = meta.getFiles( this );

      if ( !meta.isdoNotFailIfNoFile() && data.files.nrOfFiles() == 0 ) {
        throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Log.NoFiles" ) );
      }

      handleMissingFiles();

      // Create the output row meta-data
      data.outputRowMeta = new RowMeta();

      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Create convert meta-data objects that will contain Date & Number formatters
      // For String to <type> conversions, we allocate a conversion meta data row as well...
      //
      data.convertRowMeta = data.outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );
    }
    // Grab a row
    Object[] r = getXmlRow();
    if ( data.errorInRowButContinue ) {
      return true; // continue without putting the row out
    }
    if ( r == null ) {
      setOutputDone(); // signal end to receiver(s)
      return false; // end of data or error.
    }

    return putRowOut( r );

  }

  private boolean putRowOut( Object[] r ) throws HopException {
    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "GetXMLData.Log.ReadRow", data.outputRowMeta.getString( r ) ) );
    }
    incrementLinesInput();
    data.rownr++;
    putRow( data.outputRowMeta, r ); // copy row to output rowset(s);

    if ( meta.getRowLimit() > 0 && data.rownr > meta.getRowLimit() ) {
      // limit has been reached: stop now.
      setOutputDone();
      return false;
    }

    return true;
  }

  private Object[] getXmlRow() throws HopException {

    if ( !meta.isInFields() ) {
      while ( ( data.nodenr >= data.nodesize || data.file == null ) ) {
        if ( !openNextFile() ) {
          data.errorInRowButContinue = false; // stop in all cases
          return null;
        }
      }
    }
    return getXmlRowPutRowWithErrorhandling();
  }

  private Object[] getXmlRowPutRowWithErrorhandling() throws HopException {
    // Build an empty row based on the meta-data
    Object[] r;
    data.errorInRowButContinue = false;
    try {
      if ( meta.isInFields() ) {
        while ( ( data.nodenr >= data.nodesize || data.readrow == null ) ) {
          if ( !ReadNextString() ) {
            return null;
          }
          if ( data.readrow == null ) {
            return null;
          }
        }
      }

      r = processPutRow( data.an.get( data.nodenr ) );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "GetXMLData.Error.UnableReadFile" ), e );
    }

    return r;
  }

  private Object[] processPutRow( Node node ) throws HopException {
    // Create new row...
    Object[] outputRowData = buildEmptyRow();

    // Create new row or clone
    if ( meta.isInFields() ) {
      System.arraycopy( data.readrow, 0, outputRowData, 0, data.nrReadRow );
    }
    try {
      data.nodenr++;

      // Read fields...
      for ( int i = 0; i < data.nrInputFields; i++ ) {
        // Get field
        GetXmlDataField xmlDataField = meta.getInputFields()[i];
        // Get the Path to look for
        String XPathValue = xmlDataField.getResolvedXPath();

        if ( meta.isuseToken() ) {
          // See if user use Token inside path field
          // The syntax is : @_Fieldname-
          // PDI will search for Fieldname value and replace it
          // Fieldname must be defined before the current node
          XPathValue = substituteToken( XPathValue, outputRowData );
          if ( isDetailed() ) {
            logDetailed( XPathValue );
          }
        }

        // Get node value
        String nodevalue;

        // Handle namespaces
        if ( meta.isNamespaceAware() ) {
          XPath xpathField = node.createXPath( addNSPrefix( XPathValue, data.PathValue ) );
          xpathField.setNamespaceURIs( data.NAMESPACE );
          if ( xmlDataField.getResultType() == GetXmlDataField.RESULT_TYPE_VALUE_OF ) {
            nodevalue = xpathField.valueOf( node );
          } else {
            // nodevalue=xpathField.selectSingleNode(node).asXML();
            Node n = xpathField.selectSingleNode( node );
            if ( n != null ) {
              nodevalue = n.asXML();
            } else {
              nodevalue = "";
            }
          }
        } else {
          if ( xmlDataField.getResultType() == GetXmlDataField.RESULT_TYPE_VALUE_OF ) {
            nodevalue = node.valueOf( XPathValue );
          } else {
            // nodevalue=node.selectSingleNode(XPathValue).asXML();
            Node n = node.selectSingleNode( XPathValue );
            if ( n != null ) {
              nodevalue = n.asXML();
            } else {
              nodevalue = "";
            }
          }
        }

        // Do trimming
        switch ( xmlDataField.getTrimType() ) {
          case GetXmlDataField.TYPE_TRIM_LEFT:
            nodevalue = Const.ltrim( nodevalue );
            break;
          case GetXmlDataField.TYPE_TRIM_RIGHT:
            nodevalue = Const.rtrim( nodevalue );
            break;
          case GetXmlDataField.TYPE_TRIM_BOTH:
            nodevalue = Const.trim( nodevalue );
            break;
          default:
            break;
        }

        // Do conversions
        //
        IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta( data.totalpreviousfields + i );
        IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta( data.totalpreviousfields + i );
        outputRowData[data.totalpreviousfields + i] = targetValueMeta.convertData( sourceValueMeta, nodevalue );

        // Do we need to repeat this field if it is null?
        if ( meta.getInputFields()[i].isRepeated() ) {
          if ( data.previousRow != null && Utils.isEmpty( nodevalue ) ) {
            outputRowData[data.totalpreviousfields + i] = data.previousRow[data.totalpreviousfields + i];
          }
        }
      } // End of loop over fields...

      int rowIndex = data.totalpreviousfields + data.nrInputFields;

      // See if we need to add the filename to the row...
      if ( meta.includeFilename() && !Utils.isEmpty( meta.getFilenameField() ) ) {
        outputRowData[rowIndex++] = data.filename;
      }
      // See if we need to add the row number to the row...
      if ( meta.includeRowNumber() && !Utils.isEmpty( meta.getRowNumberField() ) ) {
        outputRowData[rowIndex++] = data.rownr;
      }
      // Possibly add short filename...
      if ( meta.getShortFileNameField() != null && meta.getShortFileNameField().length() > 0 ) {
        outputRowData[rowIndex++] = data.shortFilename;
      }
      // Add Extension
      if ( meta.getExtensionField() != null && meta.getExtensionField().length() > 0 ) {
        outputRowData[rowIndex++] = data.extension;
      }
      // add path
      if ( meta.getPathField() != null && meta.getPathField().length() > 0 ) {
        outputRowData[rowIndex++] = data.path;
      }
      // Add Size
      if ( meta.getSizeField() != null && meta.getSizeField().length() > 0 ) {
        outputRowData[rowIndex++] = data.size;
      }
      // add Hidden
      if ( meta.isHiddenField() != null && meta.isHiddenField().length() > 0 ) {
        outputRowData[rowIndex++] = Boolean.valueOf( data.path );
      }
      // Add modification date
      if ( meta.getLastModificationDateField() != null && meta.getLastModificationDateField().length() > 0 ) {
        outputRowData[rowIndex++] = data.lastModificationDateTime;
      }
      // Add Uri
      if ( meta.getUriField() != null && meta.getUriField().length() > 0 ) {
        outputRowData[rowIndex++] = data.uriName;
      }
      // Add RootUri
      if ( meta.getRootUriField() != null && meta.getRootUriField().length() > 0 ) {
        outputRowData[rowIndex] = data.rootUriName;
      }

      IRowMeta irow = getInputRowMeta();

      if ( irow == null ) {
        data.previousRow = outputRowData;
      } else {
        // clone to previously allocated array to make sure next transform doesn't
        // change it in between...
        System.arraycopy( outputRowData, 0, this.prevRow, 0, outputRowData.length );
        // Pick up everything else that needs a real deep clone
        data.previousRow = irow.cloneRow( outputRowData, this.prevRow );
      }
    } catch ( Exception e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        // Simply add this row to the error row
        putError( data.outputRowMeta, outputRowData, 1, e.toString(), null, "GetXMLData001" );
        data.errorInRowButContinue = true;
        return null;
      } else {
        logError( e.toString() );
        throw new HopException( e.toString() );
      }
    }
    return outputRowData;
  }

  public String substituteToken( String aString, Object[] outputRowData ) {
    if ( aString == null ) {
      return null;
    }

    StringBuilder buffer = new StringBuilder();

    String rest = aString;

    // search for closing string
    int i = rest.indexOf( data.tokenStart );
    while ( i > -1 ) {
      int j = rest.indexOf( data.tokenEnd, i + data.tokenStart.length() );
      // search for closing string
      if ( j > -1 ) {
        String varName = rest.substring( i + data.tokenStart.length(), j );
        Object Value = varName;

        for ( int k = 0; k < data.nrInputFields; k++ ) {
          GetXmlDataField Tmp_xmlInputField = meta.getInputFields()[k];
          if ( Tmp_xmlInputField.getName().equalsIgnoreCase( varName ) ) {
            Value = "'" + outputRowData[data.totalpreviousfields + k] + "'";
          }
        }
        buffer.append( rest.substring( 0, i ) );
        buffer.append( Value );
        rest = rest.substring( j + data.tokenEnd.length() );
      } else {
        // no closing tag found; end the search
        buffer.append( rest );
        rest = "";
      }
      // keep searching
      i = rest.indexOf( data.tokenEnd );
    }
    buffer.append( rest );
    return buffer.toString();
  }

  public boolean init(  ) {

    if ( super.init( ) ) {
      data.rownr = 1L;
      data.nrInputFields = meta.getInputFields().length;

      // correct attribute path if needed
      // do it once
      for ( int i = 0; i < data.nrInputFields; i++ ) {
        GetXmlDataField xmlDataField = meta.getInputFields()[i];
        // Resolve variable substitution
        String XPathValue = resolve( xmlDataField.getXPath() );
        if ( xmlDataField.getElementType() == GetXmlDataField.ELEMENT_TYPE_ATTRIBUT ) {
          // We have an attribute
          // do we need to add leading @?
          // Only put @ to the last element in path, not in front at all
          int last = XPathValue.lastIndexOf( GetXmlDataMeta.N0DE_SEPARATOR );
          if ( last > -1 ) {
            last++;
            String attribut = XPathValue.substring( last, XPathValue.length() );
            if ( !attribut.startsWith( GetXmlDataMeta.AT ) ) {
              XPathValue = XPathValue.substring( 0, last ) + GetXmlDataMeta.AT + attribut;
            }
          } else {
            if ( !XPathValue.startsWith( GetXmlDataMeta.AT ) ) {
              XPathValue = GetXmlDataMeta.AT + XPathValue;
            }
          }
        }
        xmlDataField.setResolvedXPath( XPathValue );
      }

      data.PathValue = resolve( meta.getLoopXPath() );
      if ( Utils.isEmpty( data.PathValue ) ) {
        logError( BaseMessages.getString( PKG, "GetXMLData.Error.EmptyPath" ) );
        return false;
      }
      if ( !data.PathValue.substring( 0, 1 ).equals( GetXmlDataMeta.N0DE_SEPARATOR ) ) {
        data.PathValue = GetXmlDataMeta.N0DE_SEPARATOR + data.PathValue;
      }
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "GetXMLData.Log.LoopXPath", data.PathValue ) );
      }

      data.prunePath = resolve( meta.getPrunePath() );
      if ( data.prunePath != null ) {
        if ( Utils.isEmpty( data.prunePath.trim() ) ) {
          data.prunePath = null;
        } else {
          // ensure a leading slash
          if ( !data.prunePath.startsWith( GetXmlDataMeta.N0DE_SEPARATOR ) ) {
            data.prunePath = GetXmlDataMeta.N0DE_SEPARATOR + data.prunePath;
          }
          // check if other conditions apply that do not allow pruning
          if ( meta.isInFields() ) {
            data.prunePath = null; // not possible by design, could be changed later on
          }
        }
      }

      return true;
    }
    return false;
  }

  public void dispose( ) {
    if ( data.file != null ) {
      try {
        data.file.close();
      } catch ( Exception e ) {
        // Ignore close errors
      }
    }
    if ( data.an != null ) {
      data.an.clear();
      data.an = null;
    }
    if ( data.NAMESPACE != null ) {
      data.NAMESPACE.clear();
      data.NAMESPACE = null;
    }
    if ( data.NSPath != null ) {
      data.NSPath.clear();
      data.NSPath = null;
    }
    if ( data.readrow != null ) {
      data.readrow = null;
    }
    if ( data.document != null ) {
      data.document = null;
    }
    if ( data.fr != null ) {
      BaseTransform.closeQuietly( data.fr );
    }
    if ( data.is != null ) {
      BaseTransform.closeQuietly( data.is );
    }
    if ( data.files != null ) {
      data.files = null;
    }
    super.dispose(  );
  }

}
