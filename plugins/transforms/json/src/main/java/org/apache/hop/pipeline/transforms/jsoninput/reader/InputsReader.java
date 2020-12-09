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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransformData;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInput;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputData;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputMeta;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;

public class InputsReader implements Iterable<InputStream> {

  private JsonInput transform;
  private JsonInputMeta meta;
  private JsonInputData data;
  private ErrorHandler errorHandler;

  public InputsReader( JsonInput transform, JsonInputMeta meta, JsonInputData data, ErrorHandler errorHandler ) {
    this.transform = transform;
    this.meta = meta;
    this.data = data;
    this.errorHandler = errorHandler;
  }

  @Override
  public Iterator<InputStream> iterator() {
    if ( !meta.isInFields() || meta.getIsAFile() ) {
      Iterator<FileObject> files;
      if ( meta.inputFiles.acceptingFilenames ) {
        // paths from input
        files = new FileNamesIterator( transform, errorHandler, getFieldIterator() );
      } else {
        // from inner file list
        if ( data.files == null ) {
          data.files = meta.getFileInputList( transform );
        }
        files = data.files.getFiles().listIterator( data.currentFileIndex );
      }
      return new FileContentIterator( files, data, errorHandler );
    } else if ( meta.isReadUrl() ) {
      return  new URLContentIterator( errorHandler, getFieldIterator() );
    } else {
      // direct content
      return new ChainedIterator<InputStream, String>( getFieldIterator(), errorHandler ) {
        protected InputStream tryNext() throws IOException {
          String next = inner.next();
          return next == null ? null : IOUtils.toInputStream( next, meta.getEncoding() );
        }
      };
    }
  }

  protected StringFieldIterator getFieldIterator() {
    return new StringFieldIterator(
        new RowIterator( transform, data, errorHandler ), data.indexSourceField );
  }

  public static interface ErrorHandler {
    /**
     * Generic (unexpected errors)
     */
    void error( Exception thrown );

    void fileOpenError( FileObject file, FileSystemException exception );
    void fileCloseError( FileObject file, FileSystemException exception );

  }

  protected abstract class ChainedIterator<T, C> implements Iterator<T> {

    protected Iterator<C> inner;
    protected ErrorHandler handler;

    ChainedIterator( Iterator<C> inner, ErrorHandler handler ) {
      this.inner = inner;
      this.handler = handler;
    }

    @Override
    public boolean hasNext() {
      return inner.hasNext();
    }

    @Override
    public T next() {
      try {
        return tryNext();
      } catch ( Exception e ) {
        handler.error( e );
        return null;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException( "remove" );
    }

    protected abstract T tryNext() throws Exception;
  }

  protected class FileContentIterator extends ChainedIterator<InputStream, FileObject> {

    ErrorHandler handler;
    BaseFileInputTransformData data;
    FileContentIterator( Iterator<FileObject> inner, BaseFileInputTransformData data, ErrorHandler handler ) {
      super( inner, handler );
      this.data = data;
    }

    @Override
    public InputStream tryNext() {
      if ( hasNext() ) {
        if ( data.file != null ) {
          try {
            data.file.close();
          } catch ( FileSystemException e ) {
            handler.fileCloseError( data.file, e );
          }
        }
        try {
          data.file = inner.next();
          data.currentFileIndex++;
          if ( transform.onNewFile( data.file ) ) {
            return HopVfs.getInputStream( data.file );
          }
        } catch ( FileSystemException e ) {
          handler.fileOpenError( data.file, e );
        }
      }
      return null;
    }
  }

  protected class FileNamesIterator extends ChainedIterator<FileObject, String> {

    private IVariables vars;

    public FileNamesIterator( IVariables varSpace, ErrorHandler handler, Iterator<String> fileNames ) {
      super( fileNames, handler );
      vars = varSpace;
    }

    @Override
    public FileObject tryNext() throws HopFileException {
      String fileName = transform.resolve( inner.next() );
      return fileName == null ? null : HopVfs.getFileObject( fileName );
    }
  }

  protected class URLContentIterator extends ChainedIterator<InputStream, String> {

    public URLContentIterator( ErrorHandler handler, Iterator<String> urls ) {
      super( urls, handler );
    }

    @Override protected InputStream tryNext() throws Exception {
      if ( hasNext() ) {
        URL url = new URL( transform.resolve( inner.next() ) );
        URLConnection connection = url.openConnection();
        return connection.getInputStream();
      }
      return null;
    }
  }

  protected class StringFieldIterator implements Iterator<String> {

    private RowIterator rowIter;
    private int idx;

    public StringFieldIterator( RowIterator rowIter, int idx ) {
      this.rowIter = rowIter;
      this.idx = idx;
    }

    public boolean hasNext() {
      return rowIter.hasNext();
    }

    public String next() {
      Object[] row = rowIter.next();
      return ( row == null || row.length <= idx )
          ? null
          : (String) row[idx];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException( "remove" );
    }
  }

  protected class RowIterator implements Iterator<Object[]> {

    private ITransform transform;
    private ErrorHandler errorHandler;
    private boolean gotNext;

    public RowIterator( ITransform transform, JsonInputData data, ErrorHandler errorHandler ) {
      this.transform = transform;
      this.errorHandler = errorHandler;
      gotNext = data.readrow != null;
    }

    protected void fetchNext() {
      try {
        data.readrow = transform.getRow();
        gotNext = true;
      } catch ( HopException e ) {
        errorHandler.error( e );
      }
    }

    @Override
    public boolean hasNext() {
      if ( !gotNext ) {
        fetchNext();
      }
      return data.readrow != null;
    }

    @Override
    public Object[] next() {
      if ( hasNext() ) {
        gotNext = false;
        return data.readrow;
      }
      return null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException( "remove" );
    }
  }

}
