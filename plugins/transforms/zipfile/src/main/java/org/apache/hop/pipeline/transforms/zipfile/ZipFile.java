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

package org.apache.hop.pipeline.transforms.zipfile;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Zip file * */
public class ZipFile extends BaseTransform<ZipFileMeta, ZipFileData> {
  private static final Class<?> PKG = ZipFileMeta.class;
  private static final String ZIP_COULD_NOT_FIND_FIELD = "ZipFile.Exception.CouldnotFindField";

  public ZipFile(
      TransformMeta transformMeta,
      ZipFileMeta meta,
      ZipFileData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta,
          getTransformName(),
          null,
          null,
          this,
          getPipeline().getMetadataProvider());

      // Check is source filename field is provided
      if (Utils.isEmpty(meta.getSourceFilenameField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "ZipFile.Error.SourceFilenameFieldMissing"));
      }
      // Check is target filename field is provided
      if (Utils.isEmpty(meta.getTargetFilenameField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "ZipFile.Error.TargetFilenameFieldMissing"));
      }

      // cache the position of the source filename field
      if (data.indexOfSourceFilename < 0) {
        data.indexOfSourceFilename = getInputRowMeta().indexOfValue(meta.getSourceFilenameField());
        if (data.indexOfSourceFilename < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(PKG, ZIP_COULD_NOT_FIND_FIELD, meta.getSourceFilenameField()));
        }
      }

      data.indexOfZipFilename = getInputRowMeta().indexOfValue(meta.getTargetFilenameField());
      if (data.indexOfZipFilename < 0) {
        // The field is unreachable !
        throw new HopException(
            BaseMessages.getString(PKG, ZIP_COULD_NOT_FIND_FIELD, meta.getTargetFilenameField()));
      }

      if (meta.isKeepSourceFolder() && !Utils.isEmpty(meta.getBaseFolderField())) {
        // cache the position of the source filename field
        data.indexOfBaseFolder = getInputRowMeta().indexOfValue(meta.getBaseFolderField());
        if (data.indexOfBaseFolder < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(PKG, ZIP_COULD_NOT_FIND_FIELD, meta.getBaseFolderField()));
        }
      }

      // Move to folder
      if (meta.getOperationType() == ZipFileMeta.OPERATION_TYPE_MOVE) {
        if (Utils.isEmpty(meta.getMoveToFolderField())) {
          throw new HopException(
              BaseMessages.getString(PKG, "ZipFile.Exception.EmptyMovetoFolder"));
        }
        data.indexOfMoveToFolder = getInputRowMeta().indexOfValue(meta.getMoveToFolderField());
        if (data.indexOfMoveToFolder < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(PKG, ZIP_COULD_NOT_FIND_FIELD, meta.getMoveToFolderField()));
        }
      }
    } // End If first

    boolean sendToErrorRow = false;
    String errorMessage = null;

    try {
      // get source filename
      String sourceFilename = getInputRowMeta().getString(r, data.indexOfSourceFilename);

      if (Utils.isEmpty(sourceFilename)) {
        logError(toString(), BaseMessages.getString(PKG, "ZipFile.Error.SourceFileEmpty"));
        throw new HopException(BaseMessages.getString(PKG, "ZipFile.Error.SourceFileEmpty"));
      }
      data.sourceFile = HopVfs.getFileObject(sourceFilename, variables);

      // Check sourcefile
      boolean skip = false;
      if (!data.sourceFile.exists()) {
        logError(
            toString(),
            BaseMessages.getString(PKG, "ZipFile.Error.SourceFileNotExist", sourceFilename));
        throw new HopException(
            BaseMessages.getString(PKG, "ZipFile.Error.SourceFileNotExist", sourceFilename));
      } else {
        if (data.sourceFile.getType() != FileType.FILE) {
          logError(
              toString(),
              BaseMessages.getString(PKG, "ZipFile.Error.SourceFileNotFile", sourceFilename));
          throw new HopException(
              BaseMessages.getString(PKG, "ZipFile.Error.SourceFileNotFile", sourceFilename));
        }
      }

      // get basefolder
      if (data.indexOfBaseFolder > -1) {
        data.baseFolder = getInputRowMeta().getString(r, data.indexOfBaseFolder);
      }

      // get destination folder
      String moveToFolder = null;
      if (data.indexOfMoveToFolder > -1) {
        moveToFolder = getInputRowMeta().getString(r, data.indexOfMoveToFolder);
        if (Utils.isEmpty(moveToFolder)) {
          throw new HopException(BaseMessages.getString(PKG, "ZipFile.Error.EmptyMoveToFolder"));
        }
      }

      if (!skip) {
        // get value for target filename
        String targetFilename = getInputRowMeta().getString(r, data.indexOfZipFilename);

        if (Utils.isEmpty(targetFilename)) {
          logError(toString(), BaseMessages.getString(PKG, "ZipFile.Error.TargetFileEmpty"));
          throw new HopException(BaseMessages.getString(PKG, "ZipFile.Error.TargetFileEmpty"));
        }
        data.zipFile = HopVfs.getFileObject(targetFilename, variables);
        if (data.zipFile.exists()) {
          if (isDetailed()) {
            logDetailed(
                toString(),
                BaseMessages.getString(PKG, "ZipFile.Log.TargetFileExists", targetFilename));
          }
        } else {
          // let's check parent folder
          FileObject parentFolder = data.zipFile.getParent();
          if (!parentFolder.exists()) {
            if (!meta.isCreateParentFolder()) {
              // Parent folder not exist
              // So we will fail
              throw new HopException(
                  BaseMessages.getString(
                      PKG, "ZipFile.Error.TargetParentFolderNotExists", parentFolder.toString()));
            } else {
              // Create parent folder
              parentFolder.createFolder();
            }
          }
          if (parentFolder != null) {
            parentFolder.close();
          }
        }

        // Let's zip
        zipFile();

        // file was zipped, let's see if we need to move or delete it
        processFile(moveToFolder);

        // add filename to result filenames?
        addFilenameToResult();
      }

      getLinesInput();
      putRow(data.outputRowMeta, r); // copy row to output rowset(s)

      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ZipFile.LineNumber", "" + getLinesRead()));
      }
    } catch (Exception e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "ZipFile.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "ZipFile001");
      }
    } finally {
      try {
        if (data.sourceFile != null) {
          data.sourceFile.close();
        }
        if (data.zipFile != null) {
          data.zipFile.close();
        }
      } catch (Exception e) {
        /* Ignore */
      }
    }

    return true;
  }

  private void processFile(String folder) throws Exception {

    switch (meta.getOperationType()) {
      case ZipFileMeta.OPERATION_TYPE_MOVE:
        FileObject file = null;
        FileObject moveToFolder = null;
        try {
          // Move to folder
          moveToFolder = HopVfs.getFileObject(folder, variables);

          if (moveToFolder.exists()) {
            if (moveToFolder.getType() != FileType.FOLDER) {
              throw new HopException(
                  BaseMessages.getString(PKG, "ZipFile.Error.NotAFolder", folder));
            }
          } else {
            moveToFolder.createFolder();
          }

          // get target filename
          String targetfilename =
              HopVfs.getFilename(moveToFolder)
                  + Const.FILE_SEPARATOR
                  + data.sourceFile.getName().getBaseName();
          file = HopVfs.getFileObject(targetfilename, variables);

          // Move file
          HopVfs.moveFile(data.sourceFile, file);

        } finally {
          if (file != null) {
            try {
              file.close();
            } catch (Exception e) {
              /* Ignore */
            }
          }
          if (moveToFolder != null) {
            try {
              moveToFolder.close();
            } catch (Exception e) {
              /* Ignore */
            }
          }
        }
        break;
      case ZipFileMeta.OPERATION_TYPE_DELETE:
        data.sourceFile.delete();
        break;
      default:
        break;
    }
  }

  private void addFilenameToResult() {
    if (meta.isAddResultFilenames()) {
      // Add this to the result file names...
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              data.zipFile,
              getPipelineMeta().getName(),
              getTransformName());
      resultFile.setComment(BaseMessages.getString(PKG, "ZipFile.Log.FileAddedResult"));
      addResultFile(resultFile);

      if (isDetailed()) {
        logDetailed(
            toString(),
            BaseMessages.getString(
                PKG, "ZipFile.Log.FilenameAddResult", data.sourceFile.toString()));
      }
    }
  }

  private void zipFile() throws HopException {

    // Update an existing zip by writing the new version next to it and moving it over the
    // original once complete. If anything fails, the original zip is left untouched.
    boolean updateZip = false;
    FileObject output = data.zipFile;
    CountingInputStream countingIn = null;
    CountingOutputStream countingOut = null;

    try {
      updateZip = data.zipFile.exists() && meta.isOverwriteZipEntry();
      if (updateZip) {
        output = createTemporaryZipFile(data.zipFile);
      }

      String entryName = getEntryName();
      byte[] buffer = new byte[18024];

      try (OutputStream dest = HopVfs.getOutputStream(output, false)) {
        countingOut = new CountingOutputStream(dest);
        try (ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(countingOut))) {
          if (updateZip) {
            // Copy the existing entries, except the one the source file replaces
            copyZipEntries(data.zipFile, out, entryName, buffer);
          }

          // Set the method
          out.setMethod(ZipOutputStream.DEFLATED);
          out.setLevel(Deflater.BEST_COMPRESSION);

          // Add ZIP entry to output stream.
          //
          out.putNextEntry(new ZipEntry(entryName));
          try (InputStream in = HopVfs.getInputStream(data.sourceFile)) {
            countingIn = new CountingInputStream(in);
            int len;
            while ((len = countingIn.read(buffer)) > 0) {
              out.write(buffer, 0, len);
            }
          }
          out.closeEntry();
        }
      }

      if (updateZip) {
        HopVfs.moveFile(output, data.zipFile);
      }
    } catch (Exception e) {
      if (updateZip && output != data.zipFile) {
        deleteQuietly(output);
      }
      throw new HopException(BaseMessages.getString(PKG, "ZipFile.ErrorCreatingZip"), e);
    } finally {
      if (countingIn != null) {
        dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + countingIn.getCount();
      }
      if (countingOut != null) {
        dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + countingOut.getCount();
      }
    }
  }

  /** The name of the source file in the zip. */
  private String getEntryName() throws HopException, FileSystemException {
    FileName sourceName = data.sourceFile.getName();
    if (!meta.isKeepSourceFolder()) {
      return sourceName.getBaseName();
    }
    if (!Utils.isEmpty(data.baseFolder)) {
      FileName baseName = HopVfs.getFileObject(data.baseFolder, variables).getName();
      if (baseName.isDescendent(sourceName)) {
        // Relative to the base folder
        return baseName.getRelativeName(sourceName);
      }
    }
    // The full path. Don't put the scheme of a VFS file system in the zip.
    if ("file".equals(sourceName.getScheme())) {
      return HopVfs.getFilename(data.sourceFile);
    }
    return sourceName.getPath();
  }

  /** A new file in the folder of the zip file, on the same file system. */
  private static FileObject createTemporaryZipFile(FileObject zipFile) throws IOException {
    FileObject temporaryFile;
    do {
      temporaryFile =
          zipFile
              .getParent()
              .resolveFile(zipFile.getName().getBaseName() + "_" + UUID.randomUUID() + ".tmp");
    } while (temporaryFile.exists());
    return temporaryFile;
  }

  private static void copyZipEntries(
      FileObject zipFile, ZipOutputStream out, String skipEntryName, byte[] buffer)
      throws IOException {
    try (ZipInputStream zin = new ZipInputStream(HopVfs.getInputStream(zipFile))) {
      ZipEntry entry;
      while ((entry = zin.getNextEntry()) != null) {
        String name = entry.getName();
        if (!name.equals(skipEntryName)) {
          out.putNextEntry(new ZipEntry(name));
          int len;
          while ((len = zin.read(buffer)) > 0) {
            out.write(buffer, 0, len);
          }
          out.closeEntry();
        }
      }
    }
  }

  private void deleteQuietly(FileObject file) {
    try {
      file.delete();
    } catch (Exception e) {
      logError("Unable to delete temporary file " + HopVfs.getFriendlyURI(file), e);
    }
  }

  @Override
  public boolean init() {
    return super.init();
  }
}
