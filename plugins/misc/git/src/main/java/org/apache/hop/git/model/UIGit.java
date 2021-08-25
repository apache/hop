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

package org.apache.hop.git.model;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FilenameUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.git.model.revision.GitObjectRevision;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.jgit.api.*;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.MergeResult.MergeStatus;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.diff.RenameDetector;
import org.eclipse.jgit.dircache.DirCacheIterator;
import org.eclipse.jgit.errors.*;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.merge.MergeStrategy;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.*;
import org.eclipse.jgit.transport.http.apache.HttpClientConnectionFactory;
import org.eclipse.jgit.treewalk.*;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.treewalk.filter.TreeFilter;
import org.eclipse.jgit.util.FileUtils;
import org.eclipse.jgit.util.RawParseUtils;
import org.eclipse.jgit.util.SystemReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

public class UIGit extends VCS {
  protected static final Class<?> PKG = UIGit.class; // For Translator

  static {
    /**
     * Use Apache HTTP Client instead of Sun HTTP client. This resolves the issue that Git commands
     * (e.g., push, clone) via http(s) do not work in EE. This issue is caused by the fact that weka
     * plugins (namely, knowledge-flow, weka-forecasting, and weka-scoring) calls
     * java.net.Authenticator.setDefault(). See here
     * https://bugs.eclipse.org/bugs/show_bug.cgi?id=296201 for more details.
     */
    HttpTransport.setConnectionFactory(new HttpClientConnectionFactory());
  }

  private Git git;
  private CredentialsProvider credentialsProvider;

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getDirectory()
   */
  public String getDirectory() {
    return directory;
  }

  @VisibleForTesting
  void setDirectory(String directory) {
    this.directory = directory;
  }

  @VisibleForTesting
  void setGit(Git git) {
    this.git = git;
  }

  public String getAuthorName(String commitId) {
    if (commitId.equals(VCS.WORKINGTREE)) {
      Config config = git.getRepository().getConfig();
      return config.get(UserConfig.KEY).getAuthorName()
          + " <"
          + config.get(UserConfig.KEY).getAuthorEmail()
          + ">";
    } else {
      RevCommit commit = resolve(commitId);
      PersonIdent author = commit.getAuthorIdent();
      final StringBuilder r = new StringBuilder();
      r.append(author.getName());
      r.append(" <"); //
      r.append(author.getEmailAddress());
      r.append(">"); //
      return r.toString();
    }
  }

  public String getCommitMessage(String commitId) {
    if (commitId.equals(VCS.WORKINGTREE)) {
      try {
        String mergeMsg = git.getRepository().readMergeCommitMsg();
        return mergeMsg == null ? "" : mergeMsg;
      } catch (Exception e) {
        return e.getMessage();
      }
    } else {
      RevCommit commit = resolve(commitId);
      return commit.getFullMessage();
    }
  }

  public String getCommitId(String revstr) {
    ObjectId id = null;
    try {
      id = git.getRepository().resolve(revstr);
    } catch (RevisionSyntaxException | AmbiguousObjectException | IncorrectObjectTypeException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (id == null) {
      return null;
    } else {
      return id.getName();
    }
  }

  public String getParentCommitId(String revstr) {
    return getCommitId(revstr + "~");
  }

  public String getBranch() {
    try {
      Ref head = git.getRepository().exactRef(Constants.HEAD);
      String branch = git.getRepository().getBranch();
      if (head.getLeaf().getName().equals(Constants.HEAD)) { // if detached
        return Constants.HEAD + " detached at " + branch.substring(0, 7);
      } else {
        return branch;
      }
    } catch (Exception e) {
      return "";
    }
  }

  public List<String> getLocalBranches() {
    return getBranches(null);
  }

  public List<String> getBranches() {
    return getBranches(ListMode.ALL);
  }

  /**
   * Get a list of branches based on mode
   *
   * @param mode
   * @return
   */
  private List<String> getBranches(ListMode mode) {
    try {
      return git.branchList().setListMode(mode).call().stream()
          .filter(ref -> !ref.getName().endsWith(Constants.HEAD))
          .map(ref -> Repository.shortenRefName(ref.getName()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public String getRemote() {
    try {
      StoredConfig config = git.getRepository().getConfig();
      RemoteConfig remoteConfig = new RemoteConfig(config, Constants.DEFAULT_REMOTE_NAME);
      return remoteConfig.getURIs().iterator().next().toString();
    } catch (Exception e) {
      return "";
    }
  }

  public void addRemote(String value) {
    // Make sure you have only one URI for push
    removeRemote();

    try {
      URIish uri = new URIish(value);
      RemoteAddCommand cmd = git.remoteAdd();
      cmd.setName(Constants.DEFAULT_REMOTE_NAME);
      cmd.setUri(uri);
      cmd.call();
    } catch (URISyntaxException e) {
      if (value.equals("")) {
        removeRemote();
      } else {
        showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      }
    } catch (GitAPIException e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  public void removeRemote() {
    RemoteRemoveCommand cmd = git.remoteRemove();
    cmd.setName(Constants.DEFAULT_REMOTE_NAME);
    try {
      cmd.call();
    } catch (GitAPIException e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  public boolean hasRemote() {
    StoredConfig config = git.getRepository().getConfig();
    Set<String> remotes = config.getSubsections(ConfigConstants.CONFIG_REMOTE_SECTION);
    return remotes.contains(Constants.DEFAULT_REMOTE_NAME);
  }

  public boolean commit(String authorName, String message) throws HopException {
    PersonIdent author = RawParseUtils.parsePersonIdent(authorName);
    // Set the local time
    PersonIdent author2 =
        new PersonIdent(
            author.getName(),
            author.getEmailAddress(),
            SystemReader.getInstance().getCurrentTime(),
            SystemReader.getInstance().getTimezone(SystemReader.getInstance().getCurrentTime()));
    try {
      git.commit().setAuthor(author2).setMessage(message).call();
      return true;
    } catch (Exception e) {
      throw new HopException("Error in git commit", e);
    }
  }

  public List<ObjectRevision> getRevisions() {
    return getRevisions(null);
  }

  public List<ObjectRevision> getRevisions(String path) {
    List<ObjectRevision> revisions = new ArrayList<>();
    try {
      if (!isClean()
          || git.getRepository().getRepositoryState() == RepositoryState.MERGING_RESOLVED) {
        GitObjectRevision rev =
            new GitObjectRevision(WORKINGTREE, "*", new Date(), " // " + VCS.WORKINGTREE);
        revisions.add(rev);
      }
      LogCommand logCommand = git.log();
      if (path != null && !".".equals(path)) {
        logCommand = logCommand.addPath(path);
      }
      Iterable<RevCommit> iterable = logCommand.call();
      for (RevCommit commit : iterable) {
        GitObjectRevision rev =
            new GitObjectRevision(
                commit.getName(),
                commit.getAuthorIdent().getName(),
                commit.getAuthorIdent().getWhen(),
                commit.getShortMessage());
        revisions.add(rev);
      }
    } catch (Exception e) {
      // Do nothing
    }
    return revisions;
  }

  public List<UIFile> getUnstagedFiles() {
    return getUnstagedFiles(null);
  }

  public List<UIFile> getUnstagedFiles(String path) {
    List<UIFile> files = new ArrayList<>();
    Status status = null;
    try {
      StatusCommand statusCommand = git.status();
      if (path != null && !".".equals(path)) {
        statusCommand = statusCommand.addPath(path);
      }

      status = statusCommand.call();
    } catch (Exception e) {
      e.printStackTrace();
      return files;
    }
    status.getUntracked().forEach(name -> files.add(new UIFile(name, ChangeType.ADD, false)));
    status.getModified().forEach(name -> files.add(new UIFile(name, ChangeType.MODIFY, false)));
    status.getConflicting().forEach(name -> files.add(new UIFile(name, ChangeType.MODIFY, false)));
    status.getMissing().forEach(name -> files.add(new UIFile(name, ChangeType.DELETE, false)));
    return files;
  }

  public List<UIFile> getStagedFiles() {
    List<UIFile> files = new ArrayList<>();
    Status status = null;
    try {
      status = git.status().call();
    } catch (Exception e) {
      e.printStackTrace();
      return files;
    }
    status.getAdded().forEach(name -> files.add(new UIFile(name, ChangeType.ADD, true)));
    status.getChanged().forEach(name -> files.add(new UIFile(name, ChangeType.MODIFY, true)));
    status.getRemoved().forEach(name -> files.add(new UIFile(name, ChangeType.DELETE, true)));
    return files;
  }

  public List<UIFile> getStagedFiles(String oldCommitId, String newCommitId) {
    List<UIFile> files = new ArrayList<>();
    try {
      List<DiffEntry> diffs =
          getDiffCommand(oldCommitId, newCommitId).setShowNameAndStatusOnly(true).call();
      RenameDetector rd = new RenameDetector(git.getRepository());
      rd.addAll(diffs);
      diffs = rd.compute();
      diffs.forEach(
          diff ->
              files.add(
                  new UIFile(
                      diff.getChangeType() == ChangeType.DELETE
                          ? diff.getOldPath()
                          : diff.getNewPath(),
                      diff.getChangeType(),
                      false)));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return files;
  }

  public boolean hasStagedFiles() {
    if (git.getRepository().getRepositoryState() == RepositoryState.SAFE) {
      return !getStagedFiles().isEmpty();
    } else {
      return git.getRepository().getRepositoryState().canCommit();
    }
  }

  public void initRepo(String baseDirectory) throws Exception {
    git = Git.init().setDirectory(new File(baseDirectory)).call();
    directory = baseDirectory;
  }

  public void openRepo(String baseDirectory) throws Exception {
    git = Git.open(new File(baseDirectory));
    directory = baseDirectory;
  }

  public void closeRepo() {
    git.close();
    git = null;
  }

  public void add(String filePattern) throws HopException {
    try {
      if (filePattern.endsWith(".ours") || filePattern.endsWith(".theirs")) {
        FileUtils.rename(
            new File(directory, filePattern),
            new File(directory, FilenameUtils.removeExtension(filePattern)),
            StandardCopyOption.REPLACE_EXISTING);
        filePattern = FilenameUtils.removeExtension(filePattern);
        org.apache.commons.io.FileUtils.deleteQuietly(new File(directory, filePattern + ".ours"));
        org.apache.commons.io.FileUtils.deleteQuietly(new File(directory, filePattern + ".theirs"));
      }
      git.add().addFilepattern(filePattern).call();
    } catch (Exception e) {
      throw new HopException("Error adding '" + filePattern + "'to git", e);
    }
  }

  public void rm(String filepattern) {
    try {
      git.rm().addFilepattern(filepattern).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  /** Reset to a commit (mixed) */
  public void reset(String name) {
    try {
      git.reset().setRef(name).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  /** Reset a file to HEAD (mixed) */
  public void resetPath(String path) {
    try {
      git.reset().addPath(path).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  @VisibleForTesting
  void resetHard() throws Exception {
    git.reset().setMode(ResetType.HARD).call();
  }

  public boolean rollback(String name) {
    if (hasUncommittedChanges()) {
      showMessageBox(
          BaseMessages.getString(PKG, "Dialog.Error"),
          BaseMessages.getString(PKG, "Git.Dialog.UncommittedChanges.Message"));
      return false;
    }
    String commit = resolve(Constants.HEAD).getName();
    RevertCommand cmd = git.revert();
    for (int i = 0; i < getRevisions().size(); i++) {
      String commitId = getRevisions().get(i).getRevisionId();
      /*
       * Revert commits from HEAD to the specified commit in reverse order.
       */
      cmd.include(resolve(commitId));
      if (commitId.equals(name)) {
        break;
      }
    }
    try {
      cmd.call();
      git.reset().setRef(commit).call();
      return true;
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
    return false;
  }

  public boolean pull() throws HopException {
    if (hasUncommittedChanges()) {
      throw new HopException(
          "You have uncommitted changes. Please commit work before pulling changes.");
    }
    if (!hasRemote()) {
      throw new HopException("There is no remote set up to pull from. Please set this up first.");
    }

    try {
      // Pull = Fetch + Merge
      git.fetch().setCredentialsProvider(credentialsProvider).call();
      return mergeBranch(
          Constants.DEFAULT_REMOTE_NAME + "/" + getBranch(), MergeStrategy.RECURSIVE.getName());
    } catch (TransportException e) {
      if (e.getMessage()
              .contains("Authentication is required but no CredentialsProvider has been registered")
          || e.getMessage()
              .contains("not authorized")) { // when the cached credential does not work
        if (promptUsernamePassword()) {
          return pull();
        }
      } else {
        throw new HopException("There was an error doing a git pull", e);
      }
    } catch (Exception e) {
      throw new HopException("There was an error doing a git pull", e);
    }
    return false;
  }

  public boolean push() throws HopException {
    return push("default");
  }

  public boolean push(String type) throws HopException {
    if (!hasRemote()) {
      throw new HopException("There is no remote set up to push to. Please set this up.");
    }
    String name = null;
    List<String> names;
    EnterSelectionDialog esd;
    switch (type) {
      case VCS.TYPE_BRANCH:
        names = getLocalBranches();
        esd =
            getEnterSelectionDialog(
                names.toArray(new String[names.size()]),
                "Select Branch",
                "Select the branch to push...");
        name = esd.open();
        if (name == null) {
          return false;
        }
        break;
      case VCS.TYPE_TAG:
        names = getTags();
        esd =
            getEnterSelectionDialog(
                names.toArray(new String[names.size()]), "Select Tag", "Select the tag to push...");
        name = esd.open();
        if (name == null) {
          return false;
        }
        break;
    }
    try {
      name = name == null ? null : getExpandedName(name, type);

      PushCommand cmd = git.push();
      cmd.setCredentialsProvider(credentialsProvider);
      if (name != null) {
        cmd.setRefSpecs(new RefSpec(name));
      }
      Iterable<PushResult> resultIterable = cmd.call();
      processPushResult(resultIterable);
      return true;
    } catch (TransportException e) {
      if (e.getMessage()
              .contains("Authentication is required but no CredentialsProvider has been registered")
          || e.getMessage()
              .contains("not authorized")) { // when the cached credential does not work
        if (promptUsernamePassword()) {
          return push(type);
        }
      } else {
        throw new HopException("There was an error doing a git push", e);
      }
    } catch (Exception e) {
      throw new HopException("There was an error doing a git push", e);
    }
    return false;
  }

  private void processPushResult(Iterable<PushResult> resultIterable) throws Exception {
    resultIterable.forEach(
        result -> { // for each (push)url
          StringBuilder sb = new StringBuilder();
          result.getRemoteUpdates().stream()
              .filter(update -> update.getStatus() != RemoteRefUpdate.Status.OK)
              .filter(update -> update.getStatus() != RemoteRefUpdate.Status.UP_TO_DATE)
              .forEach(
                  update -> // for each failed refspec
                  sb.append(
                          result.getURI().toString()
                              + "\n"
                              + update.getSrcRef()
                              + "\n"
                              + update.getStatus().toString()
                              + (update.getMessage() == null ? "" : "\n" + update.getMessage())
                              + "\n\n"));
          if (sb.length() == 0) {
            showMessageBox(
                BaseMessages.getString(PKG, "Dialog.Success"),
                BaseMessages.getString(PKG, "Dialog.Success"));
          } else {
            showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), sb.toString());
          }
        });
  }

  public String diff(String oldCommitId, String newCommitId) throws Exception {
    return diff(oldCommitId, newCommitId, null);
  }

  public String diff(String oldCommitId, String newCommitId, String file) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      getDiffCommand(oldCommitId, newCommitId)
          .setOutputStream(out)
          .setPathFilter(file == null ? TreeFilter.ALL : PathFilter.create(file))
          .call();
      return out.toString("UTF-8");
    } catch (Exception e) {
      return e.getMessage();
    }
  }

  public InputStream open(String file, String commitId) throws HopException {
    if (commitId.equals(WORKINGTREE)) {
      String baseDirectory = getDirectory();
      String filePath = baseDirectory + Const.FILE_SEPARATOR + file;
      try {
        return HopVfs.getInputStream(filePath);
      } catch (HopFileException e) {
        throw new HopException("Unable to find working tree file '" + filePath + "'", e);
      }
    }
    RevCommit commit = resolve(commitId);
    RevTree tree = commit.getTree();
    try (TreeWalk tw = new TreeWalk(git.getRepository())) {
      tw.addTree(tree);
      tw.setFilter(PathFilter.create(file));
      tw.setRecursive(true);
      tw.next();
      ObjectLoader loader = git.getRepository().open(tw.getObjectId(0));
      return loader.openStream();
    } catch (MissingObjectException e) {
      throw new HopException(
          "Unable to find file '" + file + "' for commit ID '" + commitId + "", e);
    } catch (IncorrectObjectTypeException e) {
      throw new HopException(
          "Incorrect object type error for file '" + file + "' for commit ID '" + commitId + "", e);
    } catch (CorruptObjectException e) {
      throw new HopException(
          "Corrupt object error for file '" + file + "' for commit ID '" + commitId + "", e);
    } catch (IOException e) {
      throw new HopException(
          "Error reading git file '" + file + "' for commit ID '" + commitId + "", e);
    }
  }

  public boolean cloneRepo(String directory, String uri) {
    CloneCommand cmd = Git.cloneRepository();
    cmd.setDirectory(new File(directory));
    cmd.setURI(uri);
    cmd.setCredentialsProvider(credentialsProvider);
    try {
      Git gitClone = cmd.call();
      gitClone.close();
      return true;
    } catch (Exception e) {
      if ((e instanceof TransportException)
          && (e.getMessage()
                  .contains(
                      "Authentication is required but no CredentialsProvider has been registered")
              || e.getMessage().contains("not authorized"))) {
        if (promptUsernamePassword()) {
          return cloneRepo(directory, uri);
        }
      } else {
        showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      }
    }
    return false;
  }

  public void checkout(String name) {
    try {
      git.checkout().setName(name).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  public void checkoutBranch(String name) {
    checkout(name);
  }

  public void checkoutTag(String name) {
    checkout(name);
  }

  public void revertPath(String path) throws HopException {
    try {
      // Revert files to HEAD state
      Status status = git.status().addPath(path).call();
      if (status.getUntracked().size() != 0 || status.getAdded().size() != 0) {
        resetPath(path);
        org.apache.commons.io.FileUtils.deleteQuietly(new File(directory, path));
      }

      /*
       * This is a work-around to discard changes of conflicting files
       * Git CLI `git checkout -- conflicted.txt` discards the changes, but jgit does not
       */
      git.add().addFilepattern(path).call();

      git.checkout().setStartPoint(Constants.HEAD).addPath(path).call();
      org.apache.commons.io.FileUtils.deleteQuietly(new File(directory, path + ".ours"));
      org.apache.commons.io.FileUtils.deleteQuietly(new File(directory, path + ".theirs"));
    } catch (Exception e) {
      throw new HopException("Git: error reverting path '" + path + "'", e);
    }
  }

  /**
   * Get the list of files which will be reverted.
   *
   * @param path The path to revert
   * @return The list of affected files
   */
  public List<String> getRevertPathFiles(String path) throws HopException {
    try {
      Set<String> files = new HashSet<>();
      StatusCommand statusCommand = git.status();
      if (path != null && !".".equals(path)) {
        statusCommand = statusCommand.addPath(path);
      }

      // Get files to be reverted to HEAD state
      //
      Status status = statusCommand.call();
      files.addAll(status.getUntracked());
      files.addAll(status.getAdded());
      files.addAll(status.getMissing());
      files.addAll(status.getChanged());
      files.addAll(status.getUncommittedChanges());

      return new ArrayList<>(files);
    } catch (Exception e) {
      throw new HopException("Git: error reverting path files for '" + path + "'", e);
    }
  }

  public boolean createBranch(String value) {
    try {
      git.branchCreate().setName(value).call();
      checkoutBranch(getExpandedName(value, VCS.TYPE_BRANCH));
      return true;
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  public boolean deleteBranch(String name, boolean force) {
    try {
      git.branchDelete()
          .setBranchNames(getExpandedName(name, VCS.TYPE_BRANCH))
          .setForce(force)
          .call();
      return true;
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  private boolean mergeBranch(String value, String mergeStrategy) throws HopException {
    try {
      ObjectId obj = git.getRepository().resolve(value);
      MergeResult result =
          git.merge().include(obj).setStrategy(MergeStrategy.get(mergeStrategy)).call();
      if (result.getMergeStatus().isSuccessful()) {
        return true;
      } else {
        // TODO: get rid of message box
        //
        showMessageBox(
            BaseMessages.getString(PKG, "Dialog.Error"), result.getMergeStatus().toString());
        if (result.getMergeStatus() == MergeStatus.CONFLICTING) {
          Map<String, int[][]> conflicts = result.getConflicts();
          for (String path : conflicts.keySet()) {
            checkout(path, Constants.HEAD, ".ours");
            checkout(path, getExpandedName(value, VCS.TYPE_BRANCH), ".theirs");
          }
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      throw new HopException(
          "Error merging branch '" + value + "' with strategy '" + mergeStrategy + "'", e);
    }
  }

  private boolean hasUncommittedChanges() {
    try {
      return git.status().call().hasUncommittedChanges();
    } catch (NoWorkTreeException | GitAPIException e) {
      e.printStackTrace();
      return false;
    }
  }

  private void checkout(String path, String commitId, String postfix) throws HopException {
    InputStream stream = open(path, commitId);
    File file = new File(directory + Const.FILE_SEPARATOR + path + postfix);
    try {
      org.apache.commons.io.FileUtils.copyInputStreamToFile(stream, file);
      stream.close();
    } catch (IOException e) {
      throw new HopException(
          "Error checking out file '"
              + path
              + "' for commit ID '"
              + commitId
              + "' and postfix "
              + postfix,
          e);
    }
  }

  private DiffCommand getDiffCommand(String oldCommitId, String newCommitId) throws Exception {
    return git.diff()
        .setOldTree(getTreeIterator(oldCommitId))
        .setNewTree(getTreeIterator(newCommitId));
  }

  private AbstractTreeIterator getTreeIterator(String commitId) throws Exception {
    if (commitId == null) {
      return new EmptyTreeIterator();
    }
    if (commitId.equals(WORKINGTREE)) {
      return new FileTreeIterator(git.getRepository());
    } else if (commitId.equals(INDEX)) {
      return new DirCacheIterator(git.getRepository().readDirCache());
    } else {
      ObjectId id = git.getRepository().resolve(commitId);
      if (id == null) { // commitId does not exist
        return new EmptyTreeIterator();
      } else {
        CanonicalTreeParser treeIterator = new CanonicalTreeParser();
        try (RevWalk rw = new RevWalk(git.getRepository())) {
          RevTree tree = rw.parseTree(id);
          try (ObjectReader reader = git.getRepository().newObjectReader()) {
            treeIterator.reset(reader, tree.getId());
          }
        }
        return treeIterator;
      }
    }
  }

  public String getShortenedName(String name, String type) {
    if (name.length() == Constants.OBJECT_ID_STRING_LENGTH) {
      return name.substring(0, 7);
    } else {
      return Repository.shortenRefName(name);
    }
  }

  public boolean isClean() {
    try {
      return git.status().call().isClean();
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public List<String> getTags() {
    try {
      return git.tagList().call().stream()
          .map(ref -> Repository.shortenRefName(ref.getName()))
          .collect(Collectors.toList());
    } catch (GitAPIException e) {
      e.printStackTrace();
    }
    return null;
  }

  public boolean createTag(String name) {
    try {
      git.tag().setName(name).call();
      return true;
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  public boolean deleteTag(String name) {
    try {
      git.tagDelete().setTags(getExpandedName(name, VCS.TYPE_TAG)).call();
      return true;
    } catch (GitAPIException e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  public String getExpandedName(String name, String type) {
    switch (type) {
      case TYPE_TAG:
        return Constants.R_TAGS + name;
      case TYPE_BRANCH:
        try {
          return git.getRepository().findRef(Constants.R_HEADS + name).getName();
        } catch (Exception e) {
          try {
            return git.getRepository().findRef(Constants.R_REMOTES + name).getName();
          } catch (Exception e1) {
            showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
          }
        }
      default:
        return getCommitId(name);
    }
  }

  @Override
  public void setCredential(String username, String password) {
    credentialsProvider = new UsernamePasswordCredentialsProvider(username, password);
  }

  public RevCommit resolve(String commitId) {
    ObjectId id = null;
    try {
      id = git.getRepository().resolve(commitId);
    } catch (RevisionSyntaxException | AmbiguousObjectException | IncorrectObjectTypeException e1) {
      e1.printStackTrace();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    try (RevWalk rw = new RevWalk(git.getRepository())) {
      RevObject obj = rw.parseAny(id);
      RevCommit commit = (RevCommit) obj;
      return commit;
    } catch (MissingObjectException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @VisibleForTesting
  EnterSelectionDialog getEnterSelectionDialog(String[] choices, String shellText, String message) {
    return new EnterSelectionDialog(HopGui.getInstance().getShell(), choices, shellText, message);
  }

  public Set<String> getIgnored(String path) {
    try {
      StatusCommand statusCommand = git.status();
      if (path != null && !".".equals(path)) {
        statusCommand = statusCommand.addPath(path);
      }
      Status status = statusCommand.call();
      return status.getIgnoredNotInIndex();
    } catch (GitAPIException e) {
      LogChannel.UI.logError("Error getting list of files ignored by git", e);
      return new HashSet<>();
    }
  }

  public Git getGit() {
    return git;
  }
}
