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

package org.apache.hop.workflow.actions.copyfiles;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'copy files' action.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
@Action(
    id = "COPY_FILES",
    name = "i18n::ActionCopyFiles.Name",
    description = "i18n::ActionCopyFiles.Description",
    image = "CopyFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/copyfiles.html")
public class ActionCopyFiles extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCopyFiles.class; // For Translator

  public static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  public static final String SOURCE_FILE_FOLDER = "source_filefolder";

  public static final String DESTINATION_CONFIGURATION_NAME = "destination_configuration_name";
  public static final String DESTINATION_FILE_FOLDER = "destination_filefolder";

  public static final String LOCAL_SOURCE_FILE = "LOCAL-SOURCE-FILE-";
  public static final String LOCAL_DEST_FILE = "LOCAL-DEST-FILE-";

  public static final String STATIC_SOURCE_FILE = "STATIC-SOURCE-FILE-";
  public static final String STATIC_DEST_FILE = "STATIC-DEST-FILE-";

  public static final String DEST_URL = "EMPTY_DEST_URL-";
  public static final String SOURCE_URL = "EMPTY_SOURCE_URL-";

  public boolean copyEmptyFolders;
  public boolean argFromPrevious;
  public boolean overwriteFiles;
  public boolean includeSubFolders;
  public boolean addResultFilenames;
  public boolean removeSourceFiles;
  public boolean destinationIsAFile;
  public boolean createDestinationFolder;
  public String[] sourceFileFolder;
  public String[] destinationFileFolder;
  public String[] wildcard;

  private HashSet<String> listFilesRemove = new HashSet<>();
  private HashSet<String> listAddResult = new HashSet<>();
  private int nbrFail = 0;

  private Map<String, String> configurationMappings = new HashMap<>();

  public ActionCopyFiles(String n) {
    super(n, "");
    copyEmptyFolders = true;
    argFromPrevious = false;
    sourceFileFolder = null;
    removeSourceFiles = false;
    destinationFileFolder = null;
    wildcard = null;
    overwriteFiles = false;
    includeSubFolders = false;
    addResultFilenames = false;
    destinationIsAFile = false;
    createDestinationFolder = false;
  }

  public ActionCopyFiles() {
    this("");
  }

  public void allocate(int nrFields) {
    sourceFileFolder = new String[nrFields];
    destinationFileFolder = new String[nrFields];
    wildcard = new String[nrFields];
  }

  public Object clone() {
    ActionCopyFiles je = (ActionCopyFiles) super.clone();
    if (sourceFileFolder != null) {
      int nrFields = sourceFileFolder.length;
      je.allocate(nrFields);
      System.arraycopy(sourceFileFolder, 0, je.sourceFileFolder, 0, nrFields);
      System.arraycopy(destinationFileFolder, 0, je.destinationFileFolder, 0, nrFields);
      System.arraycopy(wildcard, 0, je.wildcard, 0, nrFields);
    }
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("copy_empty_folders", copyEmptyFolders));
    retval.append("      ").append(XmlHandler.addTagValue("arg_from_previous", argFromPrevious));
    retval.append("      ").append(XmlHandler.addTagValue("overwrite_files", overwriteFiles));
    retval.append("      ").append(XmlHandler.addTagValue("include_subfolders", includeSubFolders));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("remove_source_files", removeSourceFiles));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("add_result_filesname", addResultFilenames));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("destination_is_a_file", destinationIsAFile));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("create_destination_folder", createDestinationFolder));

    retval.append("      <fields>").append(Const.CR);

    // Get source and destination files, also wildcard
    String[] vSourceFileFolder = preprocessfilefilder(sourceFileFolder);
    String[] vDestinationFileFolder = preprocessfilefilder(destinationFileFolder);
    if (sourceFileFolder != null) {
      for (int i = 0; i < sourceFileFolder.length; i++) {
        retval.append("        <field>").append(Const.CR);
        saveSource(retval, sourceFileFolder[i]);
        saveDestination(retval, destinationFileFolder[i]);
        retval.append("          ").append(XmlHandler.addTagValue("wildcard", wildcard[i]));
        retval.append("        </field>").append(Const.CR);
      }
    }
    retval.append("      </fields>").append(Const.CR);

    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      copyEmptyFolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "copy_empty_folders"));
      argFromPrevious =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "arg_from_previous"));
      overwriteFiles = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "overwrite_files"));
      includeSubFolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "include_subfolders"));
      removeSourceFiles =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "remove_source_files"));
      addResultFilenames =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_result_filesname"));
      destinationIsAFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "destination_is_a_file"));
      createDestinationFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "create_destination_folder"));

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      // How many field arguments?
      int nrFields = XmlHandler.countNodes(fields, "field");
      allocate(nrFields);

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        sourceFileFolder[i] = loadSource(fnode);
        destinationFileFolder[i] = loadDestination(fnode);
        wildcard[i] = XmlHandler.getTagValue(fnode, "wildcard");
      }
    } catch (HopXmlException xe) {

      throw new HopXmlException(
          BaseMessages.getString(PKG, "JobCopyFiles.Error.Exception.UnableLoadXML"), xe);
    }
  }

  protected String loadSource(Node fnode) {
    String sourceFileFolder = XmlHandler.getTagValue(fnode, SOURCE_FILE_FOLDER);
    String ncName = XmlHandler.getTagValue(fnode, SOURCE_CONFIGURATION_NAME);
    return loadURL(sourceFileFolder, ncName, getMetadataProvider(), configurationMappings);
  }

  protected String loadDestination(Node fnode) {
    String destinationFileFolder = XmlHandler.getTagValue(fnode, DESTINATION_FILE_FOLDER);
    String ncName = XmlHandler.getTagValue(fnode, DESTINATION_CONFIGURATION_NAME);
    return loadURL(destinationFileFolder, ncName, getMetadataProvider(), configurationMappings);
  }

  protected void saveSource(StringBuilder retval, String source) {
    String namedCluster = configurationMappings.get(source);
    retval.append("          ").append(XmlHandler.addTagValue(SOURCE_FILE_FOLDER, source));
    retval
        .append("          ")
        .append(XmlHandler.addTagValue(SOURCE_CONFIGURATION_NAME, namedCluster));
  }

  protected void saveDestination(StringBuilder retval, String destination) {
    String namedCluster = configurationMappings.get(destination);
    retval
        .append("          ")
        .append(XmlHandler.addTagValue(DESTINATION_FILE_FOLDER, destination));
    retval
        .append("          ")
        .append(XmlHandler.addTagValue(DESTINATION_CONFIGURATION_NAME, namedCluster));
  }

  String[] preprocessfilefilder(String[] folders) {
    List<String> nfolders = new ArrayList<>();
    if (folders != null) {
      for (int i = 0; i < folders.length; i++) {
        nfolders.add(
            folders[i]
                .replace(ActionCopyFiles.SOURCE_URL + i + "-", "")
                .replace(ActionCopyFiles.DEST_URL + i + "-", ""));
      }
    }
    return nfolders.toArray(new String[nfolders.size()]);
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;

    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    int NbrFail = 0;

    NbrFail = 0;

    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "JobCopyFiles.Log.Starting"));
    }

    try {
      // Get source and destination files, also wildcard
      String[] vSourceFileFolder = preprocessfilefilder(sourceFileFolder);
      String[] vDestinationFileFolder = preprocessfilefilder(destinationFileFolder);
      String[] vwildcard = wildcard;

      result.setResult(false);
      result.setNrErrors(1);

      if (argFromPrevious) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "JobCopyFiles.Log.ArgFromPrevious.Found",
                  (rows != null ? rows.size() : 0) + ""));
        }
      }

      if (argFromPrevious && rows != null) { // Copy the input row to the (command line) arguments
        for (int iteration = 0;
            iteration < rows.size() && !parentWorkflow.isStopped();
            iteration++) {
          resultRow = rows.get(iteration);

          // Get source and destination file names, also wildcard
          String vSourceFileFolderPrevious = resultRow.getString(0, null);
          String vDestinationFileFolderPrevious = resultRow.getString(1, null);
          String vWildcardPrevious = resultRow.getString(2, null);

          if (!Utils.isEmpty(vSourceFileFolderPrevious)
              && !Utils.isEmpty(vDestinationFileFolderPrevious)) {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "JobCopyFiles.Log.ProcessingRow",
                      HopVfs.getFriendlyURI(resolve(vSourceFileFolderPrevious)),
                      HopVfs.getFriendlyURI(resolve(vDestinationFileFolderPrevious)),
                      resolve(vWildcardPrevious)));
            }

            if (!processFileFolder(
                vSourceFileFolderPrevious,
                vDestinationFileFolderPrevious,
                vWildcardPrevious,
                parentWorkflow,
                result)) {
              // The copy process fail
              NbrFail++;
            }
          } else {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "JobCopyFiles.Log.IgnoringRow",
                      HopVfs.getFriendlyURI(resolve(vSourceFileFolder[iteration])),
                      HopVfs.getFriendlyURI(resolve(vDestinationFileFolder[iteration])),
                      vwildcard[iteration]));
            }
          }
        }
      } else if (vSourceFileFolder != null && vDestinationFileFolder != null) {
        for (int i = 0; i < vSourceFileFolder.length && !parentWorkflow.isStopped(); i++) {
          if (!Utils.isEmpty(vSourceFileFolder[i]) && !Utils.isEmpty(vDestinationFileFolder[i])) {

            // ok we can process this file/folder

            if (isBasic()) {
              logBasic(
                  BaseMessages.getString(
                      PKG,
                      "JobCopyFiles.Log.ProcessingRow",
                      HopVfs.getFriendlyURI(resolve(vSourceFileFolder[i])),
                      HopVfs.getFriendlyURI(resolve(vDestinationFileFolder[i])),
                      resolve(vwildcard[i])));
            }

            if (!processFileFolder(
                vSourceFileFolder[i],
                vDestinationFileFolder[i],
                vwildcard[i],
                parentWorkflow,
                result)) {
              // The copy process fail
              NbrFail++;
            }
          } else {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "JobCopyFiles.Log.IgnoringRow",
                      HopVfs.getFriendlyURI(resolve(vSourceFileFolder[i])),
                      HopVfs.getFriendlyURI(resolve(vDestinationFileFolder[i])),
                      vwildcard[i]));
            }
          }
        }
      }
    } finally {
      listAddResult = null;
      listFilesRemove = null;
    }

    // Check if all files was process with success
    if (NbrFail == 0) {
      result.setResult(true);
      result.setNrErrors(0);
    } else {
      result.setNrErrors(NbrFail);
    }

    return result;
  }

  boolean processFileFolder(
      String sourcefilefoldername,
      String destinationfilefoldername,
      String wildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result) {
    boolean entrystatus = false;
    FileObject sourcefilefolder = null;
    FileObject destinationfilefolder = null;

    // Clear list files to remove after copy process
    // This list is also added to result files name
    listFilesRemove.clear();
    listAddResult.clear();

    // Get real source, destination file and wildcard
    String realSourceFilefoldername = resolve(sourcefilefoldername);
    String realDestinationFilefoldername = resolve(destinationfilefoldername);
    String realWildcard = resolve(wildcard);

    try {
      sourcefilefolder = HopVfs.getFileObject(realSourceFilefoldername);
      destinationfilefolder = HopVfs.getFileObject(realDestinationFilefoldername);

      if (sourcefilefolder.exists()) {

        // Check if destination folder/parent folder exists !
        // If user wanted and if destination folder does not exist
        // PDI will create it
        if (createDestinationFolder(destinationfilefolder)) {

          // Basic Tests
          if (sourcefilefolder.getType().equals(FileType.FOLDER) && destinationIsAFile) {
            // Source is a folder, destination is a file
            // WARNING !!! CAN NOT COPY FOLDER TO FILE !!!

            logError(
                BaseMessages.getString(
                    PKG,
                    "JobCopyFiles.Log.CanNotCopyFolderToFile",
                    HopVfs.getFriendlyURI(realSourceFilefoldername),
                    HopVfs.getFriendlyURI(realDestinationFilefoldername)));

            nbrFail++;

          } else {

            if (destinationfilefolder.getType().equals(FileType.FOLDER)
                && sourcefilefolder.getType().equals(FileType.FILE)) {
              // Source is a file, destination is a folder
              // Copy the file to the destination folder

              destinationfilefolder.copyFrom(
                  sourcefilefolder.getParent(),
                  new TextOneFileSelector(
                      sourcefilefolder.getParent().toString(),
                      sourcefilefolder.getName().getBaseName(),
                      destinationfilefolder.toString()));
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        "JobCopyFiles.Log.FileCopied",
                        HopVfs.getFriendlyURI(sourcefilefolder),
                        HopVfs.getFriendlyURI(destinationfilefolder)));
              }

            } else if (sourcefilefolder.getType().equals(FileType.FILE) && destinationIsAFile) {
              // Source is a file, destination is a file

              destinationfilefolder.copyFrom(
                  sourcefilefolder, new TextOneToOneFileSelector(destinationfilefolder));
            } else {
              // Both source and destination are folders
              if (isDetailed()) {
                logDetailed("  ");
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        "JobCopyFiles.Log.FetchFolder",
                        HopVfs.getFriendlyURI(sourcefilefolder)));
              }

              TextFileSelector textFileSelector =
                  new TextFileSelector(
                      sourcefilefolder, destinationfilefolder, realWildcard, parentWorkflow);
              try {
                destinationfilefolder.copyFrom(sourcefilefolder, textFileSelector);
              } finally {
                textFileSelector.shutdown();
              }
            }

            // Remove Files if needed
            if (removeSourceFiles && !listFilesRemove.isEmpty()) {
              String sourceFilefoldername = sourcefilefolder.toString();
              int trimPathLength = sourceFilefoldername.length() + 1;
              FileObject removeFile;

              for (Iterator<String> iter = listFilesRemove.iterator();
                  iter.hasNext() && !parentWorkflow.isStopped(); ) {
                String fileremoventry = iter.next();
                removeFile = null; // re=null each iteration
                // Try to get the file relative to the existing connection
                if (fileremoventry.startsWith(sourceFilefoldername)) {
                  if (trimPathLength < fileremoventry.length()) {
                    removeFile =
                        sourcefilefolder.getChild(fileremoventry.substring(trimPathLength));
                  }
                }

                // Unable to retrieve file through existing connection; Get the file through a new
                // VFS connection
                if (removeFile == null) {
                  removeFile = HopVfs.getFileObject(fileremoventry);
                }

                // Remove ONLY Files
                if (removeFile.getType() == FileType.FILE) {
                  boolean deletefile = removeFile.delete();
                  logBasic(" ------ ");
                  if (!deletefile) {
                    logError(
                        "      "
                            + BaseMessages.getString(
                                PKG,
                                "JobCopyFiles.Error.Exception.CanRemoveFileFolder",
                                HopVfs.getFriendlyURI(fileremoventry)));
                  } else {
                    if (isDetailed()) {
                      logDetailed(
                          "      "
                              + BaseMessages.getString(
                                  PKG,
                                  "JobCopyFiles.Log.FileFolderRemoved",
                                  HopVfs.getFriendlyURI(fileremoventry)));
                    }
                  }
                }
              }
            }

            // Add files to result files name
            if (addResultFilenames && !listAddResult.isEmpty()) {
              String destinationFilefoldername = destinationfilefolder.toString();
              int trimPathLength = destinationFilefoldername.length() + 1;
              FileObject addFile;

              for (Iterator<String> iter = listAddResult.iterator(); iter.hasNext(); ) {
                String fileaddentry = iter.next();
                addFile = null; // re=null each iteration

                // Try to get the file relative to the existing connection
                if (fileaddentry.startsWith(destinationFilefoldername)) {
                  if (trimPathLength < fileaddentry.length()) {
                    addFile =
                        destinationfilefolder.getChild(fileaddentry.substring(trimPathLength));
                  }
                }

                // Unable to retrieve file through existing connection; Get the file through a new
                // VFS connection
                if (addFile == null) {
                  addFile = HopVfs.getFileObject(fileaddentry);
                }

                // Add ONLY Files
                if (addFile.getType() == FileType.FILE) {
                  ResultFile resultFile =
                      new ResultFile(
                          ResultFile.FILE_TYPE_GENERAL,
                          addFile,
                          parentWorkflow.getWorkflowName(),
                          toString());
                  result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
                  if (isDetailed()) {
                    logDetailed(" ------ ");
                    logDetailed(
                        "      "
                            + BaseMessages.getString(
                                PKG,
                                "JobCopyFiles.Log.FileAddedToResultFilesName",
                                HopVfs.getFriendlyURI(fileaddentry)));
                  }
                }
              }
            }
          }
          entrystatus = true;
        } else {
          // Destination Folder or Parent folder is missing
          logError(
              BaseMessages.getString(
                  PKG,
                  "JobCopyFiles.Error.DestinationFolderNotFound",
                  HopVfs.getFriendlyURI(realDestinationFilefoldername)));
        }
      } else {
        logError(
            BaseMessages.getString(
                PKG,
                "JobCopyFiles.Error.SourceFileNotExists",
                HopVfs.getFriendlyURI(realSourceFilefoldername)));
      }
    } catch (FileSystemException fse) {
      logError(
          BaseMessages.getString(
              PKG,
              "JobCopyFiles.Error.Exception.CopyProcessFileSystemException",
              fse.getMessage()));
      Throwable throwable = fse.getCause();
      while (throwable != null) {
        logError(BaseMessages.getString(PKG, "JobCopyFiles.Log.CausedBy", throwable.getMessage()));
        throwable = throwable.getCause();
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "JobCopyFiles.Error.Exception.CopyProcess",
              HopVfs.getFriendlyURI(realSourceFilefoldername),
              HopVfs.getFriendlyURI(realDestinationFilefoldername),
              e.getMessage()),
          e);
    } finally {
      if (sourcefilefolder != null) {
        try {
          sourcefilefolder.close();
          sourcefilefolder = null;
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (destinationfilefolder != null) {
        try {
          destinationfilefolder.close();
          destinationfilefolder = null;
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }

    return entrystatus;
  }

  private class TextOneToOneFileSelector implements FileSelector {
    FileObject destfile = null;

    public TextOneToOneFileSelector(FileObject destinationfile) {

      if (destinationfile != null) {
        destfile = destinationfile;
      }
    }

    public boolean includeFile(FileSelectInfo info) {
      boolean resultat = false;
      String filename = null;

      try {
        // check if the destination file exists

        if (destfile.exists()) {
          if (isDetailed()) {
            logDetailed(
                "      "
                    + BaseMessages.getString(
                        PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI(destfile)));
          }

          if (overwriteFiles) {
            if (isDetailed()) {
              logDetailed(
                  "      "
                      + BaseMessages.getString(
                          PKG, "JobCopyFiles.Log.FileOverwrite", HopVfs.getFriendlyURI(destfile)));
            }

            resultat = true;
          }
        } else {
          if (isDetailed()) {
            logDetailed(
                "      "
                    + BaseMessages.getString(
                        PKG,
                        "JobCopyFiles.Log.FileCopied",
                        HopVfs.getFriendlyURI(info.getFile()),
                        HopVfs.getFriendlyURI(destfile)));
          }

          resultat = true;
        }

        if (resultat && removeSourceFiles) {
          // add this folder/file to remove files
          // This list will be fetched and all entries files
          // will be removed
          listFilesRemove.add(info.getFile().toString());
        }

        if (resultat && addResultFilenames) {
          // add this folder/file to result files name
          listAddResult.add(destfile.toString());
        }

      } catch (Exception e) {

        logError(
            BaseMessages.getString(
                PKG,
                "JobCopyFiles.Error.Exception.CopyProcess",
                HopVfs.getFriendlyURI(info.getFile()),
                filename,
                e.getMessage()));
      }

      return resultat;
    }

    public boolean traverseDescendents(FileSelectInfo info) {
      return false;
    }
  }

  private boolean createDestinationFolder(FileObject filefolder) {
    FileObject folder = null;
    try {
      if (destinationIsAFile) {
        folder = filefolder.getParent();
      } else {
        folder = filefolder;
      }

      if (!folder.exists()) {
        if (createDestinationFolder) {
          if (isDetailed()) {
            logDetailed("Folder  " + HopVfs.getFriendlyURI(folder) + " does not exist !");
          }
          folder.createFolder();
          if (isDetailed()) {
            logDetailed("Folder parent was created.");
          }
        } else {
          logError("Folder  " + HopVfs.getFriendlyURI(folder) + " does not exist !");
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      logError("Couldn't created parent folder " + HopVfs.getFriendlyURI(folder), e);
    } finally {
      if (folder != null) {
        try {
          folder.close();
          folder = null;
        } catch (Exception ex) {
          /* Ignore */
        }
      }
    }
    return false;
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;
    String destinationFolder = null;
    IWorkflowEngine<WorkflowMeta> parentjob;
    Pattern pattern;
    private int traverseCount;

    // Store connection to destination source for improved performance to remote hosts
    FileObject destinationFolderObject = null;

    /**********************************************************
     *
     * @param selectedfile
     * @return True if the selectedfile matches the wildcard
     **********************************************************/
    private boolean GetFileWildcard(String selectedfile) {
      boolean getIt = true;
      // First see if the file matches the regular expression!
      if (pattern != null) {
        Matcher matcher = pattern.matcher(selectedfile);
        getIt = matcher.matches();
      }
      return getIt;
    }

    public TextFileSelector(
        FileObject sourcefolderin,
        FileObject destinationfolderin,
        String filewildcard,
        IWorkflowEngine<WorkflowMeta> parentWorkflow) {

      if (sourcefolderin != null) {
        sourceFolder = sourcefolderin.toString();
      }
      if (destinationfolderin != null) {
        destinationFolderObject = destinationfolderin;
        destinationFolder = destinationFolderObject.toString();
      }
      if (!Utils.isEmpty(filewildcard)) {
        fileWildcard = filewildcard;
        pattern = Pattern.compile(fileWildcard);
      }
      parentjob = parentWorkflow;
    }

    public boolean includeFile(FileSelectInfo info) {
      boolean returncode = false;
      FileObject filename = null;
      String addFileNameString = null;
      try {

        if (!info.getFile().toString().equals(sourceFolder) && !parentjob.isStopped()) {
          // Pass over the Base folder itself

          String shortFilename = info.getFile().getName().getBaseName();
          // Built destination filename
          if (destinationFolderObject == null) {
            // Resolve the destination folder
            destinationFolderObject = HopVfs.getFileObject(destinationFolder);
          }

          String fullName = info.getFile().toString();
          String baseFolder = info.getBaseFolder().toString();
          String path = fullName.substring(fullName.indexOf(baseFolder) + baseFolder.length() + 1);
          filename = destinationFolderObject.resolveFile(path, NameScope.DESCENDENT);

          if (!info.getFile().getParent().equals(info.getBaseFolder())) {

            // Not in the Base Folder..Only if include sub folders
            if (includeSubFolders) {
              // Folders..only if include subfolders
              if (info.getFile().getType() == FileType.FOLDER) {
                if (includeSubFolders && copyEmptyFolders && Utils.isEmpty(fileWildcard)) {
                  if ((filename == null) || (!filename.exists())) {
                    if (isDetailed()) {
                      logDetailed(" ------ ");
                      logDetailed(
                          "      "
                              + BaseMessages.getString(
                                  PKG,
                                  "JobCopyFiles.Log.FolderCopied",
                                  HopVfs.getFriendlyURI(info.getFile()),
                                  filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                    }
                    returncode = true;
                  } else {
                    if (isDetailed()) {
                      logDetailed(" ------ ");
                      logDetailed(
                          "      "
                              + BaseMessages.getString(
                                  PKG,
                                  "JobCopyFiles.Log.FolderExists",
                                  HopVfs.getFriendlyURI(filename)));
                    }
                    if (overwriteFiles) {
                      if (isDetailed()) {
                        logDetailed(
                            "      "
                                + BaseMessages.getString(
                                    PKG,
                                    "JobCopyFiles.Log.FolderOverwrite",
                                    HopVfs.getFriendlyURI(info.getFile()),
                                    HopVfs.getFriendlyURI(filename)));
                      }
                      returncode = true;
                    }
                  }
                }

              } else {
                if (GetFileWildcard(shortFilename)) {
                  // Check if the file exists
                  if ((filename == null) || (!filename.exists())) {
                    if (isDetailed()) {
                      logDetailed(" ------ ");
                      logDetailed(
                          "      "
                              + BaseMessages.getString(
                                  PKG,
                                  "JobCopyFiles.Log.FileCopied",
                                  HopVfs.getFriendlyURI(info.getFile()),
                                  filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                    }
                    returncode = true;
                  } else {
                    if (isDetailed()) {
                      logDetailed(" ------ ");
                      logDetailed(
                          "      "
                              + BaseMessages.getString(
                                  PKG,
                                  "JobCopyFiles.Log.FileExists",
                                  HopVfs.getFriendlyURI(filename)));
                    }
                    if (overwriteFiles) {
                      if (isDetailed()) {
                        logDetailed(
                            "       "
                                + BaseMessages.getString(
                                    PKG,
                                    "JobCopyFiles.Log.FileExists",
                                    HopVfs.getFriendlyURI(info.getFile()),
                                    HopVfs.getFriendlyURI(filename)));
                      }

                      returncode = true;
                    }
                  }
                }
              }
            }
          } else {
            // In the Base Folder...
            // Folders..only if include subfolders
            if (info.getFile().getType() == FileType.FOLDER) {
              if (includeSubFolders && copyEmptyFolders && Utils.isEmpty(fileWildcard)) {
                if ((filename == null) || (!filename.exists())) {
                  if (isDetailed()) {
                    logDetailed("", " ------ ");
                    logDetailed(
                        "      "
                            + BaseMessages.getString(
                                PKG,
                                "JobCopyFiles.Log.FolderCopied",
                                HopVfs.getFriendlyURI(info.getFile()),
                                filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                  }

                  returncode = true;
                } else {
                  if (isDetailed()) {
                    logDetailed(" ------ ");
                    logDetailed(
                        "      "
                            + BaseMessages.getString(
                                PKG,
                                "JobCopyFiles.Log.FolderExists",
                                HopVfs.getFriendlyURI(filename)));
                  }
                  if (overwriteFiles) {
                    if (isDetailed()) {
                      logDetailed(
                          "      "
                              + BaseMessages.getString(
                                  PKG,
                                  "JobCopyFiles.Log.FolderOverwrite",
                                  HopVfs.getFriendlyURI(info.getFile()),
                                  HopVfs.getFriendlyURI(filename)));
                    }

                    returncode = true;
                  }
                }
              }
            } else {
              // file...Check if exists
              filename =
                  HopVfs.getFileObject(destinationFolder + Const.FILE_SEPARATOR + shortFilename);

              if (GetFileWildcard(shortFilename)) {
                if ((filename == null) || (!filename.exists())) {
                  if (isDetailed()) {
                    logDetailed(" ------ ");
                    logDetailed(
                        "      "
                            + BaseMessages.getString(
                                PKG,
                                "JobCopyFiles.Log.FileCopied",
                                HopVfs.getFriendlyURI(info.getFile()),
                                filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                  }
                  returncode = true;

                } else {
                  if (isDetailed()) {
                    logDetailed(" ------ ");
                    logDetailed(
                        "      "
                            + BaseMessages.getString(
                                PKG,
                                "JobCopyFiles.Log.FileExists",
                                HopVfs.getFriendlyURI(filename)));
                  }

                  if (overwriteFiles) {
                    if (isDetailed()) {
                      logDetailed(
                          "      "
                              + BaseMessages.getString(PKG, "JobCopyFiles.Log.FileExistsInfos"),
                          BaseMessages.getString(
                              PKG,
                              "JobCopyFiles.Log.FileExists",
                              HopVfs.getFriendlyURI(info.getFile()),
                              HopVfs.getFriendlyURI(filename)));
                    }

                    returncode = true;
                  }
                }
              }
            }
          }
        }
      } catch (Exception e) {

        logError(
            BaseMessages.getString(
                PKG,
                "JobCopyFiles.Error.Exception.CopyProcess",
                HopVfs.getFriendlyURI(info.getFile()),
                filename != null ? HopVfs.getFriendlyURI(filename) : null,
                e.getMessage()));

        returncode = false;
      } finally {
        if (filename != null) {
          try {
            if (returncode && addResultFilenames) {
              addFileNameString = filename.toString();
            }
            filename.close();
            filename = null;
          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }
      if (returncode && removeSourceFiles) {
        // add this folder/file to remove files
        // This list will be fetched and all entries files
        // will be removed
        listFilesRemove.add(info.getFile().toString());
      }

      if (returncode && addResultFilenames) {
        // add this folder/file to result files name
        listAddResult.add(
            addFileNameString); // was a NPE before with the file_name=null above in the finally
      }

      return returncode;
    }

    public boolean traverseDescendents(FileSelectInfo info) {
      return (traverseCount++ == 0 || includeSubFolders);
    }

    public void shutdown() {
      if (destinationFolderObject != null) {
        try {
          destinationFolderObject.close();

        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
  }

  private class TextOneFileSelector implements FileSelector {
    String filename = null;
    String folderName = null;
    String destfolder = null;
    private int traverseCount;

    public TextOneFileSelector(
        String sourcefolderin, String sourcefilenamein, String destfolderin) {
      if (!Utils.isEmpty(sourcefilenamein)) {
        filename = sourcefilenamein;
      }

      if (!Utils.isEmpty(sourcefolderin)) {
        folderName = sourcefolderin;
      }
      if (!Utils.isEmpty(destfolderin)) {
        destfolder = destfolderin;
      }
    }

    public boolean includeFile(FileSelectInfo info) {
      boolean resultat = false;
      String filename = null;

      try {
        if (info.getFile().getType() == FileType.FILE) {
          if (info.getFile().getName().getBaseName().equals(filename)
              && (info.getFile().getParent().toString().equals(folderName))) {
            // check if the file exists
            filename = destfolder + Const.FILE_SEPARATOR + filename;

            if (HopVfs.getFileObject(filename).exists()) {
              if (isDetailed()) {
                logDetailed(
                    "      "
                        + BaseMessages.getString(
                            PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI(filename)));
              }

              if (overwriteFiles) {
                if (isDetailed()) {
                  logDetailed(
                      "      "
                          + BaseMessages.getString(
                              PKG,
                              "JobCopyFiles.Log.FileOverwrite",
                              HopVfs.getFriendlyURI(info.getFile()),
                              HopVfs.getFriendlyURI(filename)));
                }

                resultat = true;
              }
            } else {
              if (isDetailed()) {
                logDetailed(
                    "      "
                        + BaseMessages.getString(
                            PKG,
                            "JobCopyFiles.Log.FileCopied",
                            HopVfs.getFriendlyURI(info.getFile()),
                            HopVfs.getFriendlyURI(filename)));
              }

              resultat = true;
            }
          }

          if (resultat && removeSourceFiles) {
            // add this folder/file to remove files
            // This list will be fetched and all entries files
            // will be removed
            listFilesRemove.add(info.getFile().toString());
          }

          if (resultat && addResultFilenames) {
            // add this folder/file to result files name
            listAddResult.add(HopVfs.getFileObject(filename).toString());
          }
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG,
                "JobCopyFiles.Error.Exception.CopyProcess",
                HopVfs.getFriendlyURI(info.getFile()),
                HopVfs.getFriendlyURI(filename),
                e.getMessage()));

        resultat = false;
      }

      return resultat;
    }

    public boolean traverseDescendents(FileSelectInfo info) {
      return (traverseCount++ == 0 || includeSubFolders);
    }
  }

  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    boolean res =
        ActionValidatorUtils.andValidator()
            .validate(
                this,
                "arguments",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!res) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < sourceFileFolder.length; i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }

  public boolean isEvaluation() {
    return true;
  }

  public String loadURL(
      String url,
      String ncName,
      IHopMetadataProvider metadataProvider,
      Map<String, String> mappings) {
    if (!Utils.isEmpty(ncName) && !Utils.isEmpty(url)) {
      mappings.put(url, ncName);
    }
    return url;
  }

  public void setConfigurationMappings(Map<String, String> mappings) {
    this.configurationMappings = mappings;
  }

  public String getConfigurationBy(String url) {
    return this.configurationMappings.get(url);
  }

  public String getUrlPath(String incomingURL) {
    String path = null;
    try {
      String noVariablesURL = incomingURL.replaceAll("[${}]", "/");
      FileName fileName = HopVfs.getFileSystemManager().resolveURI(noVariablesURL);
      String root = fileName.getRootURI();
      path = incomingURL.substring(root.length() - 1);
    } catch (FileSystemException e) {
      path = null;
    }
    return path;
  }

  /**
   * Gets copyEmptyFolders
   *
   * @return value of copyEmptyFolders
   */
  public boolean isCopyEmptyFolders() {
    return copyEmptyFolders;
  }

  /** @param copyEmptyFolders The copyEmptyFolders to set */
  public void setCopyEmptyFolders(boolean copyEmptyFolders) {
    this.copyEmptyFolders = copyEmptyFolders;
  }

  /**
   * Gets argFromPrevious
   *
   * @return value of argFromPrevious
   */
  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  /** @param argFromPrevious The argFromPrevious to set */
  public void setArgFromPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  /**
   * Gets overwriteFiles
   *
   * @return value of overwriteFiles
   */
  public boolean isOverwriteFiles() {
    return overwriteFiles;
  }

  /** @param overwriteFiles The overwriteFiles to set */
  public void setOverwriteFiles(boolean overwriteFiles) {
    this.overwriteFiles = overwriteFiles;
  }

  /**
   * Gets includeSubFolders
   *
   * @return value of includeSubFolders
   */
  public boolean isIncludeSubFolders() {
    return includeSubFolders;
  }

  /** @param includeSubFolders The includeSubFolders to set */
  public void setIncludeSubFolders(boolean includeSubFolders) {
    this.includeSubFolders = includeSubFolders;
  }

  /**
   * Gets addResultFilenames
   *
   * @return value of addResultFilenames
   */
  public boolean isAddResultFilenames() {
    return addResultFilenames;
  }

  /** @param addResultFilenames The addResultFilenames to set */
  public void setAddResultFilenames(boolean addResultFilenames) {
    this.addResultFilenames = addResultFilenames;
  }

  /**
   * Gets removeSourceFiles
   *
   * @return value of removeSourceFiles
   */
  public boolean isRemoveSourceFiles() {
    return removeSourceFiles;
  }

  /** @param removeSourceFiles The removeSourceFiles to set */
  public void setRemoveSourceFiles(boolean removeSourceFiles) {
    this.removeSourceFiles = removeSourceFiles;
  }

  /**
   * Gets destinationIsAFile
   *
   * @return value of destinationIsAFile
   */
  public boolean isDestinationIsAFile() {
    return destinationIsAFile;
  }

  /** @param destinationIsAFile The destinationIsAFile to set */
  public void setDestinationIsAFile(boolean destinationIsAFile) {
    this.destinationIsAFile = destinationIsAFile;
  }

  /**
   * Gets createDestinationFolder
   *
   * @return value of createDestinationFolder
   */
  public boolean isCreateDestinationFolder() {
    return createDestinationFolder;
  }

  /** @param createDestinationFolder The createDestinationFolder to set */
  public void setCreateDestinationFolder(boolean createDestinationFolder) {
    this.createDestinationFolder = createDestinationFolder;
  }

  /**
   * Gets sourceFileFolder
   *
   * @return value of sourceFileFolder
   */
  public String[] getSourceFileFolder() {
    return sourceFileFolder;
  }

  /** @param sourceFileFolder The sourceFileFolder to set */
  public void setSourceFileFolder(String[] sourceFileFolder) {
    this.sourceFileFolder = sourceFileFolder;
  }

  /**
   * Gets destinationFileFolder
   *
   * @return value of destinationFileFolder
   */
  public String[] getDestinationFileFolder() {
    return destinationFileFolder;
  }

  /** @param destinationFileFolder The destinationFileFolder to set */
  public void setDestinationFileFolder(String[] destinationFileFolder) {
    this.destinationFileFolder = destinationFileFolder;
  }

  /**
   * Gets wildcard
   *
   * @return value of wildcard
   */
  public String[] getWildcard() {
    return wildcard;
  }

  /** @param wildcard The wildcard to set */
  public void setWildcard(String[] wildcard) {
    this.wildcard = wildcard;
  }

  /**
   * Gets listFilesRemove
   *
   * @return value of listFilesRemove
   */
  public HashSet<String> getListFilesRemove() {
    return listFilesRemove;
  }

  /** @param listFilesRemove The listFilesRemove to set */
  public void setListFilesRemove(HashSet<String> listFilesRemove) {
    this.listFilesRemove = listFilesRemove;
  }

  /**
   * Gets listAddResult
   *
   * @return value of listAddResult
   */
  public HashSet<String> getListAddResult() {
    return listAddResult;
  }

  /** @param listAddResult The listAddResult to set */
  public void setListAddResult(HashSet<String> listAddResult) {
    this.listAddResult = listAddResult;
  }

  /**
   * Gets nbrFail
   *
   * @return value of nbrFail
   */
  public int getNbrFail() {
    return nbrFail;
  }

  /** @param nbrFail The nbrFail to set */
  public void setNbrFail(int nbrFail) {
    this.nbrFail = nbrFail;
  }

  /**
   * Gets configurationMappings
   *
   * @return value of configurationMappings
   */
  public Map<String, String> getConfigurationMappings() {
    return configurationMappings;
  }
}
