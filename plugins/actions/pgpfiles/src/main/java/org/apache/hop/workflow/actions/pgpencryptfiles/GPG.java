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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

/** This defines a GnuPG wrapper class. */
public class GPG {

  private static final Class<?> PKG = ActionPGPEncryptFiles.class;
  public static final String CONST_BATCH_YES = "--batch --yes";

  private ILogChannel log;

  /** Options prepended when GnuPG reads the data to process from stdin instead of a file. */
  private static final List<String> GNU_PG_COMMAND = List.of("--batch", "--armor");

  /** Options prepended to the file based operations. */
  private static final List<String> BATCH_YES = List.of("--batch", "--yes");

  /** gpg program location */
  private String gpgexe = "/usr/local/bin/gpg";

  /** temporary file create when running command */
  private File tmpFile;

  /** Reads an output stream from an external process. Implemented as a thread. */
  class ProcessStreamReader extends Thread {
    StringBuilder stream;
    InputStreamReader in;

    static final int BUFFER_SIZE = 1024;

    /**
     * Creates new ProcessStreamReader object.
     *
     * @param in
     */
    ProcessStreamReader(InputStream in) {
      super();

      this.in = new InputStreamReader(in);

      this.stream = new StringBuilder();
    }

    @Override
    public void run() {
      try {
        int read;
        char[] c = new char[BUFFER_SIZE];

        while ((read = in.read(c, 0, BUFFER_SIZE - 1)) > 0) {
          stream.append(c, 0, read);
          if (read < BUFFER_SIZE - 1) {
            break;
          }
        }
      } catch (IOException io) {
        // Ignore read errors
      }
    }

    String getString() {
      return stream.toString();
    }
  }

  /**
   * Constructs a new GnuPG
   *
   * @param gpgFilename gpg program location
   * @param logInterface ILogChannel
   * @throws HopException
   */
  public GPG(String gpgFilename, ILogChannel logInterface, IVariables variables)
      throws HopException {
    this.log = logInterface;
    this.gpgexe = gpgFilename;
    // Let's check GPG filename
    if (Utils.isEmpty(getGpgExeFile())) {
      // No filename specified
      throw new HopException(BaseMessages.getString(PKG, "GPG.GPGFilenameMissing"));
    }
    // We have a filename, we need to check
    FileObject file = null;
    try {
      file = HopVfs.getFileObject(getGpgExeFile(), variables);

      if (!file.exists()) {
        throw new HopException(BaseMessages.getString(PKG, "GPG.GPGFilenameNotFound"));
      }
      // The file exists
      if (!file.getType().equals(FileType.FILE)) {
        throw new HopException(BaseMessages.getString(PKG, "GPG.GPGNotAFile", getGpgExeFile()));
      }

      // Ok we have a real file
      // Get the local filename
      this.gpgexe = HopVfs.getFilename(file);

    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "GPG.ErrorCheckingGPGFile", getGpgExeFile()), e);
    } finally {
      try {
        if (file != null) {
          file.close();
        }
      } catch (Exception e) {
        // Ignore close errors
      }
    }
  }

  /**
   * Returns GPG program location
   *
   * @return GPG filename
   */
  public String getGpgExeFile() {
    return this.gpgexe;
  }

  /**
   * Runs GnuPG external program.
   *
   * <p>The arguments are handed to the process as a list, never as a single command line: they are
   * passed to GnuPG verbatim and are never interpreted by a shell. Filenames reach this method from
   * directory scans, so any character a filesystem accepts has to survive unchanged.
   *
   * @param args command line arguments
   * @param inputStr data written to the standard input of the process, or null
   * @param fileMode true when GnuPG reads the data to process from a file rather than stdin
   * @return result
   * @throws HopException
   */
  private String execGnuPG(List<String> args, String inputStr, boolean fileMode)
      throws HopException {
    Process p;
    List<String> command = new ArrayList<>();
    command.add(getGpgExeFile());
    if (!fileMode) {
      command.addAll(GNU_PG_COMMAND);
    }
    command.addAll(args);

    if (log.isDebug()) {
      // Secrets are handed to GnuPG over stdin, so the argument list is safe to log.
      log.logDebug(BaseMessages.getString(PKG, "GPG.RunningCommand", String.join(" ", command)));
    }
    String retval;

    try {
      p = new ProcessBuilder(command).start();
    } catch (IOException io) {
      throw new HopException(BaseMessages.getString(PKG, "GPG.IOException"), io);
    }

    ProcessStreamReader psrStdOut = new ProcessStreamReader(p.getInputStream());
    ProcessStreamReader psrStdErr = new ProcessStreamReader(p.getErrorStream());
    psrStdOut.start();
    psrStdErr.start();
    if (inputStr != null) {
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()));
      try {
        out.write(inputStr);
      } catch (IOException io) {
        throw new HopException(BaseMessages.getString(PKG, "GPG.ExceptionWrite"), io);
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (Exception e) {
            // Ignore
          }
        }
      }
    }

    try {
      p.waitFor();

      psrStdOut.join();
      psrStdErr.join();
    } catch (InterruptedException i) {
      throw new HopException(BaseMessages.getString(PKG, "GPG.ExceptionWait"), i);
    }

    try {
      if (p.exitValue() != 0) {
        throw new HopException(
            BaseMessages.getString(PKG, "GPG.Exception.ExistStatus", psrStdErr.getString()));
      }
    } catch (IllegalThreadStateException itse) {
      throw new HopException(
          BaseMessages.getString(PKG, "GPG.ExceptionillegalThreadStateException"), itse);
    } finally {
      p.destroy();
    }

    retval = psrStdOut.getString();

    return retval;
  }

  /**
   * Asks GnuPG to read the passphrase from standard input.
   *
   * <p>On the command line a passphrase is readable by every other user on the machine through the
   * process table, so it travels over stdin instead. GnuPG 2.1 and later ignore a passphrase given
   * this way unless the loopback pinentry is requested as well: without it the agent tries to
   * prompt and the operation fails with "Inappropriate ioctl for device".
   *
   * @param args argument list to append to
   */
  private static void addPassPhraseFromStdin(List<String> args) {
    args.add("--pinentry-mode");
    args.add("loopback");
    args.add("--passphrase-fd");
    args.add("0");
  }

  /** Arguments for signing the given file with a passphrase supplied over stdin. */
  private static List<String> signArgs(String filename) {
    List<String> args = new ArrayList<>();
    addPassPhraseFromStdin(args);
    args.add("--sign");
    args.add(filename);
    return args;
  }

  /** Arguments for decrypting the given file with a passphrase supplied over stdin. */
  private static List<String> decryptArgs(String filename) {
    List<String> args = new ArrayList<>();
    addPassPhraseFromStdin(args);
    args.add("--decrypt");
    args.add(filename);
    return args;
  }

  /**
   * Decrypt a file
   *
   * @param cryptedFilename crypted filename
   * @param passPhrase passphrase for the personal private key to sign with
   * @param decryptedFilename decrypted filename
   * @throws HopException
   */
  public void decryptFile(
      FileObject cryptedFilename, String passPhrase, FileObject decryptedFilename)
      throws HopException {

    decryptFile(
        HopVfs.getFilename(cryptedFilename), passPhrase, HopVfs.getFilename(decryptedFilename));
  }

  /**
   * Decrypt a file
   *
   * @param cryptedFilename crypted filename
   * @param passPhrase passphrase for the personal private key to sign with
   * @param decryptedFilename decrypted filename
   * @throws HopException
   */
  public void decryptFile(String cryptedFilename, String passPhrase, String decryptedFilename)
      throws HopException {

    try {
      List<String> args = new ArrayList<>(BATCH_YES);
      boolean withPassPhrase = !Utils.isEmpty(passPhrase);
      if (withPassPhrase) {
        addPassPhraseFromStdin(args);
      }
      args.add("--output");
      args.add(decryptedFilename);
      args.add("--decrypt");
      args.add(cryptedFilename);

      execGnuPG(args, withPassPhrase ? passPhrase : null, true);

    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Encrypt a file
   *
   * @param filename file to encrypt
   * @param userID specific user id key
   * @param cryptedFilename crypted filename
   * @param asciiMode output ASCII file
   * @throws HopException
   */
  public void encryptFile(
      FileObject filename, String userID, FileObject cryptedFilename, boolean asciiMode)
      throws HopException {
    encryptFile(
        HopVfs.getFilename(filename), userID, HopVfs.getFilename(cryptedFilename), asciiMode);
  }

  /**
   * Encrypt a file
   *
   * @param filename file to encrypt
   * @param userID specific user id key
   * @param cryptedFilename crypted filename
   * @param asciiMode output ASCII file
   * @throws HopException
   */
  public void encryptFile(String filename, String userID, String cryptedFilename, boolean asciiMode)
      throws HopException {
    try {
      List<String> args = new ArrayList<>(BATCH_YES);
      if (asciiMode) {
        args.add("-a");
      }
      if (!Utils.isEmpty(userID)) {
        args.add("-r");
        args.add(userID);
      }
      args.add("--output");
      args.add(cryptedFilename);
      args.add("--encrypt");
      args.add(filename);

      execGnuPG(args, null, true);

    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Sign and encrypt a file
   *
   * @param file file to encrypt
   * @param userID specific user id key
   * @param cryptedFile crypted filename
   * @param asciiMode output ASCII file
   * @throws HopException
   */
  public void signAndEncryptFile(
      FileObject file, String userID, FileObject cryptedFile, boolean asciiMode)
      throws HopException {
    signAndEncryptFile(
        HopVfs.getFilename(file), userID, HopVfs.getFilename(cryptedFile), asciiMode);
  }

  /**
   * Sign and encrypt a file
   *
   * @param filename file to encrypt
   * @param userID specific user id key
   * @param cryptedFilename crypted filename
   * @param asciiMode output ASCII file
   * @throws HopException
   */
  public void signAndEncryptFile(
      String filename, String userID, String cryptedFilename, boolean asciiMode)
      throws HopException {

    try {
      List<String> args = new ArrayList<>(BATCH_YES);
      if (asciiMode) {
        args.add("-a");
      }
      if (!Utils.isEmpty(userID)) {
        args.add("-r");
        args.add(userID);
      }
      args.add("--output");
      args.add(cryptedFilename);
      args.add("--encrypt");
      args.add("--sign");
      args.add(filename);

      execGnuPG(args, null, true);
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Sign a file
   *
   * @param filename file to encrypt
   * @param userID specific user id key
   * @param signedFilename crypted filename
   * @param asciiMode output ASCII file
   * @throws HopException
   */
  public void signFile(String filename, String userID, String signedFilename, boolean asciiMode)
      throws HopException {
    try {
      List<String> args = new ArrayList<>(BATCH_YES);
      if (asciiMode) {
        args.add("-a");
      }
      if (!Utils.isEmpty(userID)) {
        args.add("-r");
        args.add(userID);
      }
      args.add("--output");
      args.add(signedFilename);
      args.add(asciiMode ? "--clearsign" : "--sign");
      args.add(filename);

      execGnuPG(args, null, true);

    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Sign a file
   *
   * @param file file to encrypt
   * @param userID specific user id key
   * @param signedFile crypted filename
   * @param asciiMode output ASCII file
   * @throws HopException
   */
  public void signFile(FileObject file, String userID, FileObject signedFile, boolean asciiMode)
      throws HopException {
    try {
      signFile(HopVfs.getFilename(file), userID, HopVfs.getFilename(signedFile), asciiMode);

    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Verify a signature
   *
   * @param filename filename
   * @throws HopException
   */
  public void verifySignature(FileObject filename) throws HopException {
    verifySignature(HopVfs.getFilename(filename));
  }

  /**
   * Verify a signature
   *
   * @param filename filename
   * @throws HopException
   */
  public void verifySignature(String filename) throws HopException {

    execGnuPG(List.of("--batch", "--verify", filename), null, true);
  }

  /**
   * Verify a signature for detached file
   *
   * @param signatureFilename filename
   * @param originalFilename this value in case of detached signature
   * @throws HopException
   */
  public void verifyDetachedSignature(String signatureFilename, String originalFilename)
      throws HopException {
    execGnuPG(List.of("--batch", "--verify", signatureFilename, originalFilename), null, true);
  }

  /**
   * Verify a signature for detached file
   *
   * @param signatureFile filename
   * @param originalFile fill this value in case of detached signature
   * @throws HopException
   */
  public void verifyDetachedSignature(FileObject signatureFile, FileObject originalFile)
      throws HopException {
    verifyDetachedSignature(HopVfs.getFilename(signatureFile), HopVfs.getFilename(originalFile));
  }

  /**
   * Encrypt a string
   *
   * @param plainText input string to encrypt
   * @param keyID key ID of the key in GnuPG's key database to encrypt with
   * @return encrypted string
   * @throws HopException
   */
  public String encrypt(String plainText, String keyID) throws HopException {
    List<String> args = new ArrayList<>();
    if (!Utils.isEmpty(keyID)) {
      args.add("-r");
      args.add(keyID);
    }
    args.add("--encrypt");

    return execGnuPG(args, plainText, false);
  }

  /**
   * Signs and encrypts a string
   *
   * @param plainText input string to encrypt
   * @param userID key ID of the key in GnuPG's key database to encrypt with
   * @param passPhrase passphrase for the personal private key to sign with
   * @return encrypted string
   * @throws HopException
   */
  public String signAndEncrypt(String plainText, String userID, String passPhrase)
      throws HopException {
    try {
      createTempFile(plainText);

      List<String> args = new ArrayList<>();
      if (!Utils.isEmpty(userID)) {
        args.add("-r");
        args.add(userID);
      }
      addPassPhraseFromStdin(args);
      args.add("-se");
      args.add(getTempFileName());

      return execGnuPG(args, passPhrase, false);
    } finally {

      deleteTempFile();
    }
  }

  /**
   * Sign
   *
   * @param stringToSign input string to sign
   * @param passPhrase passphrase for the personal private key to sign with
   * @throws HopException
   */
  public String sign(String stringToSign, String passPhrase) throws HopException {
    String retval;
    try {

      createTempFile(stringToSign);

      retval = execGnuPG(signArgs(getTempFileName()), passPhrase, false);

    } finally {
      deleteTempFile();
    }
    return retval;
  }

  /**
   * Decrypt a string
   *
   * @param cryptedText input string to decrypt
   * @param passPhrase passphrase for the personal private key to sign with
   * @return plain text
   * @throws HopException
   */
  public String decrypt(String cryptedText, String passPhrase) throws HopException {
    try {
      createTempFile(cryptedText);

      return execGnuPG(decryptArgs(getTempFileName()), passPhrase, false);

    } finally {
      deleteTempFile();
    }
  }

  /**
   * Create a unique temporary file when needed by one of the main methods. The file handle is store
   * in tmpFile object var.
   *
   * @param content data to write into the file
   * @throws HopException
   */
  private void createTempFile(String content) throws HopException {
    this.tmpFile = null;

    try {
      this.tmpFile = File.createTempFile("GnuPG", null);
      if (log.isDebug()) {
        log.logDebug(BaseMessages.getString(PKG, "GPG.TempFileCreated", getTempFileName()));
      }
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "GPG.ErrorCreatingTempFile"), e);
    }

    try (FileWriter fw = new FileWriter(this.tmpFile)) {
      fw.write(content);
      fw.flush();
    } catch (Exception e) {
      // delete our file:
      deleteTempFile();

      throw new HopException(BaseMessages.getString(PKG, "GPG.ErrorWritingTempFile"), e);
    }
  }

  /**
   * Delete temporary file.
   *
   * @throws HopException
   */
  private void deleteTempFile() {
    if (this.tmpFile != null) {
      if (log.isDebug()) {
        log.logDebug(BaseMessages.getString(PKG, "GPG.DeletingTempFile", getTempFileName()));
      }
      this.tmpFile.delete();
    }
  }

  /**
   * Returns temporary filename.
   *
   * @return temporary filename
   */
  private String getTempFileName() {
    return this.tmpFile.getAbsolutePath();
  }

  public String toString() {
    return "GPG";
  }
}
