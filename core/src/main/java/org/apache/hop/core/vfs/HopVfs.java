/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.vfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.apache.hop.i18n.BaseMessages;

public class HopVfs {
  private static final Class<?> PKG = HopVfs.class;

  public static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

  /** The one and only file system manager. */
  private static DefaultFileSystemManager fsm;

  /**
   * The variables used to bootstrap the metadata driven providers (the named VFS connections).
   * They're only needed while those providers are being registered, not to resolve individual
   * files.
   */
  private static IVariables bootstrapVariables;

  /** Set once the metadata driven providers have been registered on {@link #fsm}. */
  private static boolean namedProvidersRegistered;

  /**
   * Guards against re-entrant registration: reading the VFS connection metadata resolves files
   * through this very class.
   */
  private static boolean registeringNamedProviders;

  /**
   * Set the variables to look up the metadata driven VFS providers (the named VFS connections)
   * with. Call this when a project is loaded: its variables are what point at the metadata holding
   * those connections. The named connections of that project are registered the next time the file
   * system manager is used.
   *
   * @param variables the variables to bootstrap the VFS providers with
   */
  public static synchronized void setBootstrapVariables(IVariables variables) {
    if (variables == bootstrapVariables) {
      return;
    }
    bootstrapVariables = variables;

    // Connections of a previous project are registered on the manager and there's no unregistering
    // them, so start over. Nothing registered yet means we can keep the manager as it is: phase 2
    // simply runs with these variables the next time around.
    //
    if (namedProvidersRegistered) {
      reset();
    }
  }

  /**
   * Get the file system manager. There is only one: it knows both the fixed schemes (file, zip, s3,
   * ...) and the schemes of the named VFS connections in the metadata.
   *
   * @return the file system manager
   */
  public static synchronized DefaultFileSystemManager getFileSystemManager() {
    if (fsm == null) {
      try {
        // Phase 1 : the standard schemes and the fixed schemes of the VFS plugins.
        //
        DefaultFileSystemManager manager = createFileSystemManager();
        manager.init();

        // Publish before phase 2 : registering the metadata driven providers below reads the
        // metadata, and reading metadata resolves files through this very manager.
        //
        fsm = manager;
      } catch (Exception e) {
        throw new HopRuntimeException("Error initializing file system manager : ", e);
      }
    }

    // Phase 2 : the providers of the named VFS connections in the metadata. Only once a project
    // handed us its variables: they're what points us at the metadata holding those connections,
    // and what the providers resolve their credentials with. Everything resolving files before
    // that (reading the configuration, loading the images of the GUI) wants a local file and is
    // served by phase 1 alone.
    //
    if (bootstrapVariables != null && !namedProvidersRegistered && !registeringNamedProviders) {
      // Set before anything else: registering reads metadata, which resolves files, which lands
      // right back here.
      //
      registeringNamedProviders = true;
      try {
        registerNamedProviders(fsm, bootstrapVariables);
      } finally {
        registeringNamedProviders = false;
        namedProvidersRegistered = true;
      }
    }

    return fsm;
  }

  /**
   * @param variables the variables to bootstrap the metadata driven providers with, in case nothing
   *     did that yet
   * @return the one and only file system manager
   * @see #getFileSystemManager()
   */
  public static synchronized DefaultFileSystemManager getFileSystemManager(IVariables variables) {
    bootstrapWith(variables);
    return getFileSystemManager();
  }

  /**
   * Remember the variables to bootstrap the metadata driven providers with, for as long as nothing
   * bootstrapped them yet. These are the first variables we get to see, so the named connections
   * are looked up in the metadata they point at, the next time the manager is used. An explicit
   * {@link #setBootstrapVariables(IVariables)} always wins.
   */
  private static synchronized void bootstrapWith(IVariables variables) {
    if (variables != null && bootstrapVariables == null) {
      bootstrapVariables = variables;
    }
  }

  /**
   * Register a provider for every named VFS connection in the metadata. A connection which can't be
   * registered is skipped: a single bad connection should never take the whole file system manager
   * down with it.
   */
  private static void registerNamedProviders(
      DefaultFileSystemManager manager, IVariables variables) {
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin plugin : registry.getPlugins(VfsPluginType.class)) {
      try {
        IVfs iVfs = registry.loadClass(plugin, IVfs.class);
        Map<String, FileProvider> fileProviderMap = iVfs.getProviders(variables);
        if (fileProviderMap == null) {
          continue;
        }
        for (Map.Entry<String, FileProvider> entry : fileProviderMap.entrySet()) {
          String scheme = entry.getKey();
          if (manager.hasProvider(scheme)) {
            LogChannel.GENERAL.logError(
                "The VFS connection '"
                    + scheme
                    + "' of plugin "
                    + plugin.getIds()[0]
                    + " is ignored: a provider is already registered for that scheme.");
            continue;
          }
          manager.addProvider(scheme, entry.getValue());
        }
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error registering provider for VFS plugin "
                + plugin.getIds()[0]
                + " : "
                + plugin.getName(),
            e);
      }
    }
  }

  /**
   * Make sure to close when done using!
   *
   * @return A new standard file system manager
   * @throws HopException
   */
  @SuppressWarnings("java:S2095") // the file system manager is a process-wide singleton
  private static DefaultFileSystemManager createFileSystemManager() throws HopException {
    try {
      DefaultFileSystemManager fsm = new DefaultFileSystemManager();
      fsm.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
      fsm.addProvider(
          "file", new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
      fsm.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
      fsm.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
      fsm.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
      fsm.addProvider("jar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("http", new org.apache.commons.vfs2.provider.http5.Http5FileProvider());
      fsm.addProvider("https", new org.apache.commons.vfs2.provider.http5s.Http5sFileProvider());
      fsm.addProvider("ftp", new org.apache.commons.vfs2.provider.ftp.FtpFileProvider());
      fsm.addProvider("ftps", new org.apache.commons.vfs2.provider.ftps.FtpsFileProvider());
      fsm.addProvider("sftp", new org.apache.commons.vfs2.provider.sftp.SftpFileProvider());
      fsm.addProvider("war", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("par", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("ear", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("sar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("ejb3", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("tmp", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
      fsm.addProvider("tar", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      fsm.addProvider("tbz2", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      fsm.addProvider("tgz", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      fsm.addProvider("bz2", new org.apache.commons.vfs2.provider.bzip2.Bzip2FileProvider());
      fsm.addProvider(
          "files-cache", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
      fsm.addExtensionMap("jar", "jar");
      fsm.addExtensionMap("zip", "zip");
      fsm.addExtensionMap("gz", "gz");
      fsm.addExtensionMap("tar", "tar");
      fsm.addExtensionMap("tbz2", "tar");
      fsm.addExtensionMap("tgz", "tar");
      fsm.addExtensionMap("bz2", "bz2");
      fsm.addMimeTypeMap("application/x-tar", "tar");
      fsm.addMimeTypeMap("application/x-gzip", "gz");
      fsm.addMimeTypeMap("application/zip", "zip");
      fsm.setFileContentInfoFactory(new FileContentInfoFilenameFactory());

      DefaultFileReplicator replicator = new DefaultFileReplicator();
      fsm.setReplicator(replicator);
      fsm.setTemporaryFileStore(replicator);

      fsm.setFilesCache(new SoftRefFilesCache());
      fsm.setCacheStrategy(CacheStrategy.ON_RESOLVE);

      // Here are extra VFS plugins to register
      //
      PluginRegistry registry = PluginRegistry.getInstance();
      List<IPlugin> plugins = registry.getPlugins(VfsPluginType.class);
      for (IPlugin plugin : plugins) {
        IVfs iVfs = registry.loadClass(plugin, IVfs.class);
        try {
          String[] urlSchemes = iVfs.getUrlSchemes();
          FileProvider provider = iVfs.getProvider();

          // Skip plugins with no fixed scheme (Minio, Databricks): a provider the manager has no
          // scheme for is never reached, and never closed. Phase 2 registers those by name.
          if (urlSchemes == null || urlSchemes.length == 0 || provider == null) {
            continue;
          }
          fsm.addProvider(urlSchemes, provider);
        } catch (Exception e) {
          throw new HopException(
              "Error registering provider for VFS plugin "
                  + plugin.getIds()[0]
                  + " : "
                  + plugin.getName()
                  + " : ",
              e);
        }
      }
      return fsm;
    } catch (Exception e) {
      throw new HopException("Error creating file system manager", e);
    }
  }

  /**
   * @param vfsFilename the name of the file to resolve
   * @param variables the variables to bootstrap the metadata driven providers with, in case nothing
   *     did that yet. They play no role in resolving the file itself.
   * @return the file object
   * @see #getFileObject(String)
   */
  public static FileObject getFileObject(String vfsFilename, IVariables variables)
      throws HopFileException {
    bootstrapWith(variables);
    return getFileObject(vfsFilename);
  }

  public static synchronized FileObject getFileObject(String vfsFilename) throws HopFileException {
    DefaultFileSystemManager fsManager = getFileSystemManager();

    try {
      // We have one problem with VFS: if the file is in a subdirectory of the current one:
      // somedir/somefile
      // In that case, VFS doesn't parse the file correctly.
      // We need to put file: in front of it to make it work.
      // However, how are we going to verify this?
      //
      // We are going to see if the filename starts with one of the known protocols like file:
      // zip: ram: smb: jar: etc.
      // If not, we are going to assume it's a file.
      //
      boolean relativeFilename = true;
      String[] initialSchemes = fsManager.getSchemes();

      relativeFilename = checkForScheme(initialSchemes, relativeFilename, vfsFilename);

      String filename;
      if (vfsFilename.startsWith("\\\\")) {
        File file = new File(vfsFilename);
        filename = file.toURI().toString();
      } else {
        if (relativeFilename) {
          File file = new File(vfsFilename);
          filename = file.getAbsolutePath();
        } else {
          filename = vfsFilename;
        }
      }

      return fsManager.resolveFile(filename);
    } catch (Exception e) {
      throw new HopFileException(
          "Unable to get VFS File object for filename '"
              + cleanseFilename(vfsFilename)
              + "' : "
              + e.getMessage(),
          e);
    }
  }

  protected static boolean checkForScheme(
      String[] initialSchemes, boolean relativeFilename, String vfsFilename) {
    if (vfsFilename == null) {
      return false;
    }
    for (int i = 0; i < initialSchemes.length && relativeFilename; i++) {
      if (vfsFilename.startsWith(initialSchemes[i] + ":")) {
        relativeFilename = false;
      }
    }
    return relativeFilename;
  }

  /**
   * Private method for stripping password from filename when a FileObject can not be obtained.
   * getFriendlyURI(FileObject) or getFriendlyURI(String) are the public methods.
   */
  private static String cleanseFilename(String vfsFilename) {
    return vfsFilename.replaceAll(":[^:@/]+@", ":<password>@");
  }

  /**
   * Read a text file (like an XML document). WARNING DO NOT USE FOR DATA FILES.
   *
   * @param vfsFilename the filename or URL to read from
   * @param charset the character set of the string (UTF-8, ISO8859-1, etc.)
   * @return The content of the file as a String
   * @throws org.apache.hop.core.exception.HopFileException ex
   */
  public static String getTextFileContent(String vfsFilename, Charset charset)
      throws HopFileException {
    try {
      InputStream inputStream = getInputStream(vfsFilename);
      InputStreamReader reader = new InputStreamReader(inputStream, charset);
      int c;
      StringBuilder aBuffer = new StringBuilder();
      while ((c = reader.read()) != -1) {
        aBuffer.append((char) c);
      }
      reader.close();
      inputStream.close();

      return aBuffer.toString();
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  public static boolean fileExists(String vfsFilename) throws HopFileException {
    FileObject fileObject = null;
    try {
      fileObject = getFileObject(vfsFilename);
      return fileObject.exists();
    } catch (IOException e) {
      throw new HopFileException(e);
    } finally {
      if (fileObject != null) {
        try {
          fileObject.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }
  }

  /**
   * @see #fileExists(String)
   */
  public static boolean fileExists(String vfsFilename, IVariables variables)
      throws HopFileException {
    bootstrapWith(variables);
    return fileExists(vfsFilename);
  }

  public static boolean isLocalFileSystem(String path) {
    try {
      FileObject fileObject = getFileObject(path);
      return fileObject instanceof LocalFile;
    } catch (HopFileException e) {
      return false;
    }
  }

  public static InputStream getInputStream(FileObject fileObject) throws FileSystemException {
    FileContent content = fileObject.getContent();
    return content.getInputStream();
  }

  public static InputStream getInputStream(String vfsFilename) throws HopFileException {
    try {
      FileObject fileObject = getFileObject(vfsFilename);

      return getInputStream(fileObject);
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  /**
   * @see #getInputStream(String)
   */
  public static InputStream getInputStream(String vfsFilename, IVariables variables)
      throws HopFileException {
    bootstrapWith(variables);
    return getInputStream(vfsFilename);
  }

  public static OutputStream getOutputStream(FileObject fileObject, boolean append)
      throws IOException {
    FileObject parent = fileObject.getParent();
    if (parent != null && !parent.exists()) {
      throw new IOException(
          BaseMessages.getString(
              PKG, "HopVFS.Exception.ParentDirectoryDoesNotExist", getFriendlyURI(parent)));
    }
    try {
      // Temporary work-around for VFS-807 bug (can be removed after 2.9.0)
      //
      if (fileObject.exists() && !append) {
        // Content was not removed at the trailing end of the file
        // Se we're going to delete the file first in this scenario
        //
        fileObject.delete();
      }
      fileObject.createFile();
      FileContent content = fileObject.getContent();
      return content.getOutputStream(append);
    } catch (FileSystemException e) {
      // Perhaps if it's a local file, we can retry using the standard
      // File object. This is because on Windows there is a bug in VFS.
      //
      if (fileObject instanceof LocalFile) {
        try {
          String filename = getFilename(fileObject);
          return new FileOutputStream(new File(filename), append);
        } catch (Exception e2) {
          throw e; // throw the original exception: hide the retry.
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * @see #getOutputStream(String, boolean)
   */
  public static OutputStream getOutputStream(
      String vfsFilename, boolean append, IVariables variables) throws HopFileException {
    bootstrapWith(variables);
    return getOutputStream(vfsFilename, append);
  }

  public static OutputStream getOutputStream(String vfsFilename, boolean append)
      throws HopFileException {
    try {
      FileObject fileObject = getFileObject(vfsFilename);
      return getOutputStream(fileObject, append);
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  /**
   * Utility to normalize file name depending on OS.
   *
   * <p>On Window clean some situation where {@code c:/project/\workflow.hwf} is normalized to
   * {@code c:\project\workflow.hwf}
   */
  public static String normalize(String filename) throws HopFileException {
    return getFilename(getFileObject(filename));
  }

  public static String getFilename(FileObject fileObject) {
    FileName fileName = fileObject.getName();
    String root = fileName.getRootURI();
    if (!root.startsWith("file:")) {
      return fileName.getURI(); // nothing we can do about non-normal files.
    }
    if (root.startsWith("file:////")) {
      return fileName.getURI(); // we'll see 4 forward slashes for a windows/smb network share
    }
    if (root.endsWith(":/")) { // Windows
      root = root.substring(8, 10);
    } else { // *nix & OSX
      root = "";
    }
    String fileString = root + fileName.getPath();
    if (!"/".equals(Const.FILE_SEPARATOR)) {
      fileString = Const.replace(fileString, "/", Const.FILE_SEPARATOR);
    }
    return fileString;
  }

  public static String getFriendlyURI(String filename) {
    if (filename == null) {
      return null;
    }
    String friendlyName;
    try {
      friendlyName = getFriendlyURI(HopVfs.getFileObject(filename));
    } catch (Exception e) {
      // unable to get a friendly name from VFS object.
      // Cleanse name of pwd before returning
      friendlyName = cleanseFilename(filename);
    }
    return friendlyName;
  }

  /**
   * @see #getFriendlyURI(String)
   */
  public static String getFriendlyURI(String filename, IVariables variables) {
    bootstrapWith(variables);
    return getFriendlyURI(filename);
  }

  public static String getFriendlyURI(FileObject fileObject) {
    return fileObject.getName().getFriendlyURI();
  }

  /**
   * Creates a file using "java.io.tmpdir" directory
   *
   * @param prefix - file name
   * @param suffix - file extension
   * @return FileObject
   * @throws HopFileException
   */
  public static FileObject createTempFile(String prefix, Suffix suffix) throws HopFileException {
    return createTempFile(prefix, suffix.ext, TEMP_DIR);
  }

  /**
   * @param prefix - file name
   * @param suffix - file extension
   * @param directory - directory where file will be created
   * @return FileObject
   * @throws HopFileException
   */
  public static synchronized FileObject createTempFile(
      String prefix, String suffix, String directory) throws HopFileException {
    try {
      FileObject fileObject;
      do {
        // Temporary files are always stored locally.
        // No other schemes besides file:// make sense
        //
        String baseUrl;
        if (directory.contains("://")) {
          baseUrl = directory;
        } else {
          File directoryFile = new File(directory);
          baseUrl = "file://" + directoryFile.getAbsolutePath();
        }

        // Build temporary file name using UUID to ensure uniqueness. Old mechanism would fail using
        // Sort Rows (for example)
        // when there multiple nodes with multiple JVMs on each node. In this case, the temp file
        // names would end up being
        // duplicated which would cause the sort to fail.
        //
        String filename = baseUrl + "/" + prefix + "_" + UUID.randomUUID() + suffix;

        fileObject = getFileObject(filename);
      } while (fileObject.exists());
      return fileObject;
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  /**
   * @param prefix - file name
   * @param suffix - file extension
   * @param directory - directory where file will be created
   * @param variables the variables to bootstrap the metadata driven providers with, in case nothing
   *     did that yet
   * @return FileObject
   * @throws HopFileException
   * @see #createTempFile(String, String, String)
   */
  public static FileObject createTempFile(
      String prefix, String suffix, String directory, IVariables variables)
      throws HopFileException {
    bootstrapWith(variables);
    return createTempFile(prefix, suffix, directory);
  }

  public static Comparator<FileObject> getComparator() {
    return (o1, o2) -> {
      String filename1 = getFilename(o1);
      String filename2 = getFilename(o2);
      return filename1.compareTo(filename2);
    };
  }

  /**
   * Check if filename starts with one of the known protocols like file: zip: ram: smb: jar: etc. If
   * yes, return true otherwise return false
   *
   * @param vfsFileName
   * @return boolean
   */
  public static boolean startsWithScheme(String vfsFileName, IVariables variables) {
    bootstrapWith(variables);
    return startsWithScheme(vfsFileName);
  }

  /**
   * Check if filename starts with one of the known protocols like file: zip: ram: smb: jar: etc. If
   * yes, return true otherwise return false
   *
   * @param vfsFileName
   * @return boolean
   */
  public static boolean startsWithScheme(String vfsFileName) {
    DefaultFileSystemManager fsManager = getFileSystemManager();

    boolean found = false;
    String[] schemes = fsManager.getSchemes();
    for (String scheme : schemes) {
      if (vfsFileName.startsWith(scheme + ":")) {
        found = true;
        break;
      }
    }

    return found;
  }

  /**
   * Check if a filename is an absolute path and therefore should not have a base/relative folder
   * prepended. This recognises:
   *
   * <ul>
   *   <li>VFS URIs with a scheme, e.g. {@code file:///...}, {@code s3://...}, {@code hdfs://...}
   *   <li>POSIX absolute paths ({@code /...})
   *   <li>Windows UNC paths ({@code \\host\share})
   *   <li>Windows drive-letter paths ({@code C:\...} / {@code C:/...})
   * </ul>
   *
   * @param filename the (unresolved) filename to check
   * @return true if the filename is an absolute path
   */
  public static boolean isAbsolutePath(String filename) {
    if (filename == null || filename.isEmpty()) {
      return false;
    }
    // A VFS URI with a scheme, e.g. file:///, s3://, hdfs://, ...
    if (filename.contains("://")) {
      return true;
    }
    // POSIX absolute path or Windows UNC path (\\host\share)
    if (filename.startsWith("/") || filename.startsWith("\\")) {
      return true;
    }
    // Windows drive-letter absolute path: C:\... or C:/...
    return filename.length() >= 3
        && Character.isLetter(filename.charAt(0))
        && filename.charAt(1) == ':'
        && (filename.charAt(2) == '/' || filename.charAt(2) == '\\');
  }

  /**
   * Find files with a specific extension in the specified folder and optionally
   *
   * @param folder The folder to search in
   * @param extension The extension of null if you want all files
   * @param includeSubFolders True if you want to search sub-folders as well.
   * @return The list of files found
   * @throws Exception
   */
  public static final List<FileObject> findFiles(
      FileObject folder, String extension, boolean includeSubFolders) throws Exception {
    List<FileObject> files = new ArrayList<>();

    for (FileObject child : folder.getChildren()) {
      if (child.isFolder() && includeSubFolders) {
        files.addAll(findFiles(child, extension, true));
      } else {
        if (StringUtils.isEmpty(extension)
            || extension.equalsIgnoreCase(child.getName().getExtension())) {
          files.add(child);
        }
      }
    }

    return files;
  }

  /**
   * @see StandardFileSystemManager#freeUnusedResources()
   */
  public static synchronized void freeUnusedResources() {
    if (fsm != null) {
      fsm.freeUnusedResources();
    }
  }

  /**
   * Drop the file system manager so it's rebuilt, providers of the named VFS connections included,
   * the next time it's used. The bootstrap variables are kept: use {@link
   * #setBootstrapVariables(IVariables)} to change those.
   */
  public static synchronized void reset() {
    if (fsm != null) {
      fsm.freeUnusedResources();
      fsm.close();
      fsm = null;
    }
    namedProvidersRegistered = false;
    registeringNamedProviders = false;
  }

  public enum Suffix {
    ZIP(".zip"),
    TMP(".tmp"),
    JAR(".jar");

    private String ext;

    Suffix(String ext) {
      this.ext = ext;
    }
  }
}
