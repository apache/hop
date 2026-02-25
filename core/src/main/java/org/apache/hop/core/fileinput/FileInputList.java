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

package org.apache.hop.core.fileinput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.compressed.CompressedFileFileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

public class FileInputList {
  private final List<FileObject> files = new ArrayList<>();
  private final List<FileObject> nonExistentFiles = new ArrayList<>(1);
  private final List<FileObject> nonAccessibleFiles = new ArrayList<>(1);

  private static final ILogChannel log = new LogChannel("FileInputList");

  public enum FileTypeFilter {
    FILES_AND_FOLDERS("all_files", FileType.FILE, FileType.FOLDER),
    ONLY_FILES("only_files", FileType.FILE),
    ONLY_FOLDERS("only_folders", FileType.FOLDER);

    private final String name;
    private final Collection<FileType> allowedFileTypes;

    FileTypeFilter(String name, FileType... allowedFileTypes) {
      this.name = name;
      this.allowedFileTypes = Collections.unmodifiableCollection(Arrays.asList(allowedFileTypes));
    }

    public boolean isFileTypeAllowed(FileType fileType) {
      return allowedFileTypes.contains(fileType);
    }

    @Override
    public String toString() {
      return name;
    }

    public static FileTypeFilter getByOrdinal(int ordinal) {
      for (FileTypeFilter filter : FileTypeFilter.values()) {
        if (filter.ordinal() == ordinal) {
          return filter;
        }
      }
      return ONLY_FILES;
    }

    public static FileTypeFilter getByName(String name) {
      for (FileTypeFilter filter : FileTypeFilter.values()) {
        if (filter.name.equals(name)) {
          return filter;
        }
      }
      return ONLY_FILES;
    }
  }

  private static final String YES = "Y";

  public static String getRequiredFilesDescription(List<FileObject> nonExistantFiles) {
    StringBuilder buffer = new StringBuilder();
    for (FileObject file : nonExistantFiles) {
      try {
        buffer.append(file.getName().getURI());
      } catch (Exception e) {
        buffer.append(file.getPublicURIString());
      }
      buffer.append(Const.CR);
    }
    return buffer.toString();
  }

  private static boolean[] includeSubDirsFalse(int iLength) {
    boolean[] includeSubDirs = new boolean[iLength];
    for (int i = 0; i < iLength; i++) {
      includeSubDirs[i] = false;
    }
    return includeSubDirs;
  }

  public static String[] createFilePathList(
      IVariables variables,
      String[] fileName,
      String[] fileMask,
      String[] excludeFileMask,
      String[] fileRequired) {
    boolean[] includeSubDirs = includeSubDirsFalse(fileName.length);
    return createFilePathList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubDirs, null);
  }

  public static String[] createFilePathList(
      IVariables variables,
      String[] fileName,
      String[] fileMask,
      String[] excludeFileMask,
      String[] fileRequired,
      boolean[] includeSubDirs) {
    return createFilePathList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubDirs, null);
  }

  public static String[] createFilePathList(
      IVariables variables,
      String[] fileName,
      String[] fileMask,
      String[] excludeFileMask,
      String[] fileRequired,
      boolean[] includeSubDirs,
      FileTypeFilter[] filters) {
    List<FileObject> fileList =
        createFileList(
                variables,
                fileName,
                fileMask,
                excludeFileMask,
                fileRequired,
                includeSubDirs,
                filters)
            .getFiles();
    String[] filePaths = new String[fileList.size()];
    for (int i = 0; i < filePaths.length; i++) {
      filePaths[i] = fileList.get(i).getName().getURI();
    }
    return filePaths;
  }

  public static FileInputList createFileList(
      IVariables variables,
      String[] fileName,
      String[] fileMask,
      String[] excludeFileMask,
      String[] fileRequired) {
    boolean[] includeSubDirs = includeSubDirsFalse(fileName.length);
    return createFileList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubDirs, null);
  }

  public static FileInputList createFileList(
      IVariables variables,
      String[] fileName,
      String[] fileMask,
      String[] excludeFileMask,
      String[] fileRequired,
      boolean[] includeSubDirs) {
    return createFileList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubDirs, null);
  }

  public static FileInputList createFileList(
      IVariables variables,
      String[] fileName,
      String[] fileMask,
      String[] excludeFileMask,
      String[] fileRequired,
      boolean[] includeSubDirs,
      FileTypeFilter[] fileTypeFilters) {
    FileInputList fileInputList = new FileInputList();

    // Replace possible environment variables...
    final String[] realFile = variables.resolve(fileName);
    final String[] realMask = variables.resolve(fileMask);
    final String[] realExcludeMask = variables.resolve(excludeFileMask);

    for (int i = 0; i < realFile.length; i++) {
      final String oneFile = realFile[i];
      final String oneMask = realMask[i];
      final String excludeOneMask = realExcludeMask[i];
      final boolean oneRequired = YES.equalsIgnoreCase(fileRequired[i]);
      final boolean subDirs = includeSubDirs[i];
      final FileTypeFilter filter =
          ((fileTypeFilters == null || fileTypeFilters[i] == null)
              ? FileTypeFilter.ONLY_FILES
              : fileTypeFilters[i]);

      if (Utils.isEmpty(oneFile)) {
        continue;
      }

      try {
        FileObject directoryFileObject = HopVfs.getFileObject(oneFile, variables);
        boolean processFolder = true;
        if (oneRequired) {
          if (!directoryFileObject.exists()) {
            // if we don't find folder: no need to continue
            fileInputList.addNonExistantFile(directoryFileObject);
            processFolder = false;
          } else {
            if (!directoryFileObject.isReadable()) {
              fileInputList.addNonAccessibleFile(directoryFileObject);
              processFolder = false;
            }
          }
        }

        // Find all file names that match the wildcard in this directory
        //
        if (processFolder) {
          if (directoryFileObject != null
              && directoryFileObject.getType() == FileType.FOLDER) { // it's a directory
            FileObject[] fileObjects =
                directoryFileObject.findFiles(
                    new AllFileSelector() {
                      @Override
                      public boolean traverseDescendents(FileSelectInfo info) {
                        return info.getDepth() == 0 || subDirs;
                      }

                      @Override
                      public boolean includeFile(FileSelectInfo info) {
                        // Never return the parent directory of a file list.
                        if (info.getDepth() == 0) {
                          return false;
                        }

                        FileObject fileObject = info.getFile();
                        try {
                          if (fileObject != null
                              && filter.isFileTypeAllowed(fileObject.getType())) {
                            String name = info.getFile().getName().getBaseName();
                            boolean matches = true;
                            if (!Utils.isEmpty(oneMask)) {
                              matches = Pattern.matches(oneMask, name);
                            }
                            boolean excludematches = false;
                            if (!Utils.isEmpty(excludeOneMask)) {
                              excludematches = Pattern.matches(excludeOneMask, name);
                            }
                            return (matches && !excludematches);
                          }
                          return false;
                        } catch (IOException ex) {
                          // Upon error don't process the file.
                          return false;
                        }
                      }
                    });
            if (fileObjects != null) {
              for (FileObject fileObject : fileObjects) {
                if (fileObject.exists()) {
                  fileInputList.addFile(fileObject);
                }
              }
            }
            if (Utils.isEmpty(fileObjects) && oneRequired) {

              fileInputList.addNonAccessibleFile(directoryFileObject);
            }

            // Sort the list: quicksort, only for regular files
            fileInputList.sortFiles();
          } else if (directoryFileObject instanceof CompressedFileFileObject) {
            FileObject[] children = directoryFileObject.getChildren();
            for (FileObject child : children) {
              // See if the wildcard (regexp) matches...
              String name = child.getName().getBaseName();
              boolean matches = true;
              if (!Utils.isEmpty(oneMask)) {
                matches = Pattern.matches(oneMask, name);
              }
              boolean excludeMatches = false;
              if (!Utils.isEmpty(excludeOneMask)) {
                excludeMatches = Pattern.matches(excludeOneMask, name);
              }
              if (matches && !excludeMatches) {
                fileInputList.addFile(child);
              }
            }
            // We don't sort here, keep the order of the files in the archive.
          } else {
            FileObject fileObject = HopVfs.getFileObject(oneFile, variables);
            if (fileObject.exists()) {
              if (fileObject.isReadable()) {
                fileInputList.addFile(fileObject);
              } else {
                if (oneRequired) {
                  fileInputList.addNonAccessibleFile(fileObject);
                }
              }
            } else {
              if (oneRequired) {
                fileInputList.addNonExistantFile(fileObject);
              }
            }
          }
        }
      } catch (Exception e) {
        if (oneRequired) {
          fileInputList.addNonAccessibleFile(new NonAccessibleFileObject(oneFile));
        }
        log.logError(Const.getStackTracker(e));
      }
    }

    return fileInputList;
  }

  public static FileInputList createFolderList(
      IVariables variables, String[] folderName, String[] folderRequired) {
    FileInputList fileInputList = new FileInputList();

    // Replace possible environment variables...
    final String[] realFolder = variables.resolve(folderName);

    for (int i = 0; i < realFolder.length; i++) {
      final String oneFile = realFolder[i];
      final boolean oneRequired = YES.equalsIgnoreCase(folderRequired[i]);
      final FileTypeFilter filter = FileTypeFilter.ONLY_FOLDERS;

      if (Utils.isEmpty(oneFile)) {
        continue;
      }

      try (FileObject directoryFileObject = HopVfs.getFileObject(oneFile, variables)) {
        // Find all folder names in this directory
        //
        if (directoryFileObject != null
            && directoryFileObject.getType() == FileType.FOLDER) { // it's a directory
          FileObject[] fileObjects =
              directoryFileObject.findFiles(
                  new AllFileSelector() {
                    @Override
                    public boolean includeFile(FileSelectInfo info) {
                      // Never return the parent directory of a file list.
                      if (info.getDepth() == 0) {
                        return false;
                      }

                      FileObject fileObject = info.getFile();
                      try {
                        return fileObject != null && filter.isFileTypeAllowed(fileObject.getType());
                      } catch (IOException ex) {
                        // Upon error don't process the file.
                        return false;
                      }
                    }
                  });
          if (fileObjects != null) {
            for (FileObject fileObject : fileObjects) {
              if (fileObject.exists()) {
                fileInputList.addFile(fileObject);
              }
            }
          }
          if (Utils.isEmpty(fileObjects) && oneRequired) {
            fileInputList.addNonAccessibleFile(directoryFileObject);
          }

          // Sort the list: quicksort, only for regular files
          fileInputList.sortFiles();
        } else {
          if (oneRequired && (directoryFileObject == null || !directoryFileObject.exists())) {
            fileInputList.addNonExistantFile(directoryFileObject);
          }
        }
      } catch (Exception e) {
        log.logError(Const.getStackTracker(e));
      }
      // Ignore
    }

    return fileInputList;
  }

  public List<FileObject> getFiles() {
    return files;
  }

  public String[] getFileStrings() {
    String[] fileStrings = new String[files.size()];
    for (int i = 0; i < fileStrings.length; i++) {
      fileStrings[i] = HopVfs.getFilename(files.get(i));
    }
    return fileStrings;
  }

  public String[] getUrlStrings() {
    String[] fileStrings = new String[files.size()];
    for (int i = 0; i < fileStrings.length; i++) {
      fileStrings[i] = files.get(i).getPublicURIString();
    }
    return fileStrings;
  }

  public List<FileObject> getNonAccessibleFiles() {
    return nonAccessibleFiles;
  }

  public List<FileObject> getNonExistentFiles() {
    return nonExistentFiles;
  }

  public void addFile(FileObject file) {
    files.add(file);
  }

  public void addNonAccessibleFile(FileObject file) {
    nonAccessibleFiles.add(file);
  }

  public void addNonExistantFile(FileObject file) {
    nonExistentFiles.add(file);
  }

  public void sortFiles() {
    files.sort(HopVfs.getComparator());
    nonAccessibleFiles.sort(HopVfs.getComparator());
    nonExistentFiles.sort(HopVfs.getComparator());
  }

  public FileObject getFile(int i) {
    return files.get(i);
  }

  public int nrOfFiles() {
    return files.size();
  }

  public int nrOfMissingFiles() {
    return nonAccessibleFiles.size() + nonExistentFiles.size();
  }

  public static FileInputList createFileList(
      IVariables variables,
      String[] fileName,
      String[] fileMask,
      String[] fileRequired,
      boolean[] includeSubDirs) {
    return createFileList(
        variables,
        fileName,
        fileMask,
        new String[fileName.length],
        fileRequired,
        includeSubDirs,
        null);
  }

  public static String[] createFilePathList(
      IVariables variables, String[] fileName, String[] fileMask, String[] fileRequired) {
    boolean[] includeSubDirs = includeSubDirsFalse(fileName.length);
    return createFilePathList(
        variables,
        fileName,
        fileMask,
        new String[fileName.length],
        fileRequired,
        includeSubDirs,
        null);
  }
}
