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

package org.apache.hop.pipeline.transforms.dropbox.output;

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.NetworkIOException;
import com.dropbox.core.RetryException;
import com.dropbox.core.util.IOUtil;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.*;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transforms.dropbox.input.DropboxInputData;
import org.apache.hop.pipeline.transforms.dropbox.input.DropboxInputMeta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DropboxOutput extends BaseTransform<DropboxOutputMeta, DropboxOutputData> implements ITransform<DropboxOutputMeta, DropboxOutputData> {
    private static Class<?> PKG = DropboxOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$


    public DropboxOutput(TransformMeta transformMeta, DropboxOutputMeta meta, DropboxOutputData data, int copyNr, PipelineMeta pipelineMeta,
                         Pipeline pipeline ) {
        super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    }
    /**
     * Initialize and do work where other transforms need to wait for...
     *
     */
    public boolean init() {

        if ( super.init( ) ) {
            if ( Utils.isEmpty( meta.getAccessTokenField() ) ) {
                logError( BaseMessages.getString( PKG, "DropboxOutput.Missing.AccessToken" ) );
                return false;
            }
            if ( Utils.isEmpty( meta.getSourceFilesField() ) ) {
                logError( BaseMessages.getString( PKG, "DropboxOutput.Missing.SourceFiles" ) );
                return false;
            }
            if ( Utils.isEmpty( meta.getTargetFilesField() ) ) {
                logError( BaseMessages.getString( PKG, "DropboxOutput.Missing.TargetFiles" ) );
                return false;
            }
            List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();
            data.chosesTargetTransforms =
                    targetStreams.get( 0 ).getTransformMeta() != null || targetStreams.get( 1 ).getTransformMeta() != null;
            return true;
        } else {
            return false;
        }
    }

    public boolean processRow( ) throws HopException {

        Object[] r = getRow(); // get row, set busy!
        if ( r == null ) {
            // no more input to be expected...
            setOutputDone();
            return false;
        }

        // We need to map the fields.
        if ( first ) {
            first = false;
            // Mapping Access Token field.
            data.accessTokenIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getAccessTokenField() );
            if ( data.accessTokenIdx < 0 ) {
                logError( BaseMessages.getString( PKG, "DropboxOutput.Invalid.AccessToken" ) );
                setErrors( 1 );
                stopAll();
                return false;
            }
            // Mapping Source Files field.
            data.sourceFileIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getSourceFilesField() );
            if ( data.sourceFileIdx < 0 ) {
                logError( BaseMessages.getString( PKG, "DropboxOutput.Invalid.SourceFiles" ) );
                setErrors( 1 );
                stopAll();
                return false;
            }
            // Mapping Target Files field.
            data.targetFilesIdx = Arrays.binarySearch( getInputRowMeta().getFieldNames( ), meta.getTargetFilesField() );
            if ( data.targetFilesIdx < 0 ) {
                logError( BaseMessages.getString( PKG, "DropboxOutput.Invalid.TargetFiles" ) );
                setErrors( 1 );
                stopAll();
                return false;
            }

            data.outputRowMeta = getInputRowMeta().clone();

            // Cache the position of the RowSet for the output.
            if ( data.chosesTargetTransforms ) {
                List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();
                if ( !Utils.isEmpty( targetStreams.get( 0 ).getTransformName() ) ) {
                    data.successfulRowSet = findOutputRowSet( getTransformName(), getCopy(), targetStreams.get( 0 ).getTransformName(), 0 );
                    if ( data.successfulRowSet == null ) {
                        throw new HopException( BaseMessages.getString(
                                PKG, "DropboxOutput.Log.TargetTransformInvalid", targetStreams.get( 0 ).getTransformName() ) );
                    }
                } else {
                    data.successfulRowSet = null;
                }

                if ( !Utils.isEmpty( targetStreams.get( 1 ).getTransformName() ) ) {
                    data.failedRowSet = findOutputRowSet( getTransformName(), getCopy(), targetStreams.get( 1 ).getTransformName(), 0 );
                    if ( data.failedRowSet == null ) {
                        throw new HopException( BaseMessages.getString(
                                PKG, "DropboxOutput.Log.TargetTransformInvalid", targetStreams.get( 1 ).getTransformName() ) );
                    }
                } else {
                    data.failedRowSet = null;
                }
            }
        }

        // Get Values from Input Row.
        String accessToken = (String) r[data.accessTokenIdx];
        String sourceFile = (String) r[data.sourceFileIdx];
        String targetFile = (String) r[data.targetFilesIdx ];

        if ( Utils.isEmpty( accessToken ) ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Null.AccessToken" ) );
            putFailedTransferRow( r );
            return true;
        }

        if ( Utils.isEmpty( sourceFile ) ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Null.SourceFiles" ) );
            putFailedTransferRow( r );
            return true;
        }

        if ( Utils.isEmpty( targetFile ) ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Null.TargetFiles" ) );
            putFailedTransferRow( r );
            return true;
        }

        // Check Source Files.
        File localFile = new File( sourceFile );
        if ( !localFile.exists() ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Log.InvalidSourceFile.NotExist", sourceFile ) );
            putFailedTransferRow( r );
            return true;
        }
        if ( !localFile.isFile() ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Log.InvalidSourceFile.NotAFile", sourceFile ) );
            putFailedTransferRow( r );
            return true;
        }

        // Create a DbxClientV2 to make API calls.
        DbxRequestConfig requestConfig = new DbxRequestConfig( "examples-upload-file" );
        DbxClientV2 dbxClient = new DbxClientV2( requestConfig, accessToken );

        // upload the file with simple upload API if it is small enough, otherwise use chunked
        // upload API for better performance. Arbitrarily chose 2 times our chunk size as the
        // deciding factor. This should really depend on your network.
        log.logBasic( BaseMessages.getString( PKG, "DropboxOutput.log.Uploading", sourceFile ) );
        if ( localFile.length() <= ( 2 * data.CHUNKED_UPLOAD_CHUNK_SIZE ) ) {
            if ( !uploadFile( dbxClient, localFile, targetFile ) ) {
                putFailedTransferRow( r );
                return true;
            }
        } else {
            if ( !chunkedUploadFile( dbxClient, localFile, targetFile ) ) {
                putFailedTransferRow( r );
                return true;
            }
        }
        log.logBasic( BaseMessages.getString( PKG, "DropboxOutput.log.Uploaded", targetFile ) );

        putSuccessfulTransferRow( r ); // Transfer has succeeded.

        if ( checkFeedback( getLinesRead() ) ) {
            logBasic( BaseMessages.getString( PKG, "DropboxOutput.Log.LineNumber" ) + getLinesRead() );
        }
        return true;
    }

    private void putFailedTransferRow( Object[] r ) throws HopTransformException {
        if ( !data.chosesTargetTransforms ) {
            putRow( data.outputRowMeta, r );
        } else {
            putRowTo( data.outputRowMeta, r, data.failedRowSet );
        }
    }

    private void putSuccessfulTransferRow( Object[] r ) throws HopTransformException {
        if ( !data.chosesTargetTransforms ) {
            putRow( data.outputRowMeta, r );
        } else {
            putRowTo( data.outputRowMeta, r, data.successfulRowSet );
        }
    }

    /**
     * Uploads a file in a single request. This approach is preferred for small files since it
     * eliminates unnecessary round-trips to the servers.
     *
     * @param dbxClient Dropbox user authenticated client
     * @param localFile local file to upload
     * @param dropboxPath Where to upload the file to within Dropbox
     */
    private boolean uploadFile( DbxClientV2 dbxClient, File localFile, String dropboxPath ) {
        try ( InputStream in = new FileInputStream( localFile ) ) {
            IOUtil.ProgressListener progressListener = l -> printProgress( l, localFile.length() );

            FileMetadata metadata = dbxClient.files().uploadBuilder( dropboxPath )
                    .withMode( WriteMode.ADD )
                    .withClientModified( new Date( localFile.lastModified() ) )
                    .uploadAndFinish( in, progressListener );

            log.logDetailed( metadata.toStringMultiline() );
        } catch ( UploadErrorException ex ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Log.UploadError", ex.getMessage() ) );
            return false;
        } catch ( DbxException ex ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Log.UploadError", ex.getMessage() ) );
            return false;
        } catch ( IOException ex ) {
            logError( BaseMessages.getString( PKG, "DropboxOutput.Log.ErrorReadingFile", localFile.getName(), ex.getMessage() ) );
            return false;
        }
        return true;
    }

    /**
     * Uploads a file in chunks using multiple requests. This approach is preferred for larger files
     * since it allows for more efficient processing of the file contents on the server side and
     * also allows partial uploads to be retried (e.g. network connection problem will not cause you
     * to re-upload all the bytes).
     *
     * @param dbxClient Dropbox user authenticated client
     * @param localFile local file to upload
     * @param dropboxPath Where to upload the file to within Dropbox
     */
    private boolean chunkedUploadFile( DbxClientV2 dbxClient, File localFile, String dropboxPath ) {
        long size = localFile.length();

        // assert our file is at least the chunk upload size. We make this assumption in the code
        // below to simplify the logic.
        if ( size < data.CHUNKED_UPLOAD_CHUNK_SIZE ) {
            log.logError( BaseMessages.getString( PKG, "DropboxOutput.Log.FileTooSmall" ) );
            return false;
        }

        long uploaded = 0L;
        DbxException thrown = null;

        IOUtil.ProgressListener progressListener = new IOUtil.ProgressListener() {
            long uploadedBytes = 0;
            @Override
            public void onProgress( long l ) {
                printProgress( l + uploadedBytes, size );
                if ( l == data.CHUNKED_UPLOAD_CHUNK_SIZE ) {
                    uploadedBytes += data.CHUNKED_UPLOAD_CHUNK_SIZE;
                }
            }
        };

        // Chunked uploads have 3 phases, each of which can accept uploaded bytes:
        //
        //    (1)  Start: initiate the upload and get an upload session ID
        //    (2) Append: upload chunks of the file to append to our session
        //    (3) Finish: commit the upload and close the session
        //
        // We track how many bytes we uploaded to determine which phase we should be in.
        String sessionId = null;
        for ( int i = 0; i < data.CHUNKED_UPLOAD_MAX_ATTEMPTS; ++i ) {
            if ( i > 0 ) {
                log.logDetailed( String.format( "Retrying chunked upload (%d / %d attempts)\n", i + 1, data.CHUNKED_UPLOAD_MAX_ATTEMPTS ) );
            }

            try ( InputStream in = new FileInputStream( localFile ) ) {
                // if this is a retry, make sure seek to the correct offset
                in.skip( uploaded );

                // (1) Start
                if ( sessionId == null ) {
                    sessionId = dbxClient.files().uploadSessionStart()
                            .uploadAndFinish( in, data.CHUNKED_UPLOAD_CHUNK_SIZE, progressListener )
                            .getSessionId();
                    uploaded += data.CHUNKED_UPLOAD_CHUNK_SIZE;
                    printProgress( uploaded, size );
                }

                UploadSessionCursor cursor = new UploadSessionCursor( sessionId, uploaded );

                // (2) Append
                while ( ( size - uploaded ) > data.CHUNKED_UPLOAD_CHUNK_SIZE ) {
                    dbxClient.files().uploadSessionAppendV2( cursor )
                            .uploadAndFinish( in, data.CHUNKED_UPLOAD_CHUNK_SIZE, progressListener );
                    uploaded += data.CHUNKED_UPLOAD_CHUNK_SIZE;
                    printProgress( uploaded, size );
                    cursor = new UploadSessionCursor( sessionId, uploaded );
                }

                // (3) Finish
                long remaining = size - uploaded;
                CommitInfo commitInfo = CommitInfo.newBuilder( dropboxPath )
                        .withMode( WriteMode.ADD )
                        .withClientModified( new Date( localFile.lastModified() ) )
                        .build();
                FileMetadata metadata = dbxClient.files().uploadSessionFinish( cursor, commitInfo )
                        .uploadAndFinish( in, remaining, progressListener );

                log.logBasic( metadata.toStringMultiline() );
                return true;
            } catch ( RetryException ex ) {
                thrown = ex;
                // RetryExceptions are never automatically retried by the client for uploads. Must
                // catch this exception even if DbxRequestConfig.getMaxRetries() > 0.
                sleepQuietly( ex.getBackoffMillis() );
                continue;
            } catch ( NetworkIOException ex ) {
                thrown = ex;
                // network issue with Dropbox (maybe a timeout?) try again
                continue;
            } catch ( UploadSessionLookupErrorException ex ) {
                if ( ex.errorValue.isIncorrectOffset() ) {
                    thrown = ex;
                    // server offset into the stream doesn't match our offset (uploaded). Seek to
                    // the expected offset according to the server and try again.
                    uploaded = ex.errorValue
                            .getIncorrectOffsetValue()
                            .getCorrectOffset();
                    continue;
                } else {
                    // Some other error occurred, give up.
                    log.logError( BaseMessages.getString( PKG, "DropboxOutput.Log.UploadError", ex.getMessage() ) );
                    return false;
                }
            } catch ( UploadSessionFinishErrorException ex ) {
                if ( ex.errorValue.isLookupFailed() && ex.errorValue.getLookupFailedValue().isIncorrectOffset() ) {
                    thrown = ex;
                    // server offset into the stream doesn't match our offset (uploaded). Seek to
                    // the expected offset according to the server and try again.
                    uploaded = ex.errorValue
                            .getLookupFailedValue()
                            .getIncorrectOffsetValue()
                            .getCorrectOffset();
                    continue;
                } else {
                    // some other error occurred, give up.
                    log.logError( BaseMessages.getString( PKG, "DropboxOutput.Log.UploadError", ex.getMessage() ) );
                    return false;
                }
            } catch ( DbxException ex ) {
                log.logError( BaseMessages.getString( PKG, "DropboxOutput.Log.UploadError", ex.getMessage() ) );
                return false;
            } catch ( IOException ex ) {
                log.logError( BaseMessages.getString( PKG, "DropboxOutput.Log.ErrorReadingFile", localFile.getName(), ex.getMessage() ) );
                return false;
            }
        }

        // if we made it here, then we must have run out of attempts
        log.logError( BaseMessages.getString( PKG, "DropboxOutput.Log.TooManyAttempts", thrown.getMessage() ) );
        return false;
    }

    private void printProgress( long uploaded, long size ) {
        logDetailed( String.format( "Uploaded %12d / %12d bytes (%5.2f%%)\n", uploaded, size, 100 * ( uploaded / (double) size ) ) );
    }
    private void sleepQuietly( long millis ) {
        try {
            Thread.sleep( millis );
        } catch ( InterruptedException ex ) {
            // just exit
            log.logError( BaseMessages.getString( PKG, "DropboxOutput.Log.Error.Interrupted" ) );
        }
    }
    

}
