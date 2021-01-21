/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hop.git.dialog.MergeBranchDialog;
import org.apache.hop.git.model.revision.GitObjectRevision;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.eclipse.jface.window.Window;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.DiffCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.api.MergeResult.MergeStatus;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.RemoteAddCommand;
import org.eclipse.jgit.api.RemoteRemoveCommand;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.RevertCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.diff.RenameDetector;
import org.eclipse.jgit.dircache.DirCacheIterator;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.Config;
import org.eclipse.jgit.lib.ConfigConstants;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryState;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.lib.UserConfig;
import org.eclipse.jgit.merge.MergeStrategy;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.HttpTransport;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.transport.http.apache.HttpClientConnectionFactory;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.treewalk.FileTreeIterator;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.treewalk.filter.TreeFilter;
import org.eclipse.jgit.util.FileUtils;
import org.eclipse.jgit.util.RawParseUtils;
import org.eclipse.jgit.util.SystemReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UIGit extends VCS implements IVCS {

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

  @Override
  public String getType() {
    return IVCS.GIT;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getDirectory()
   */
  @Override
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getAuthorName(java.lang.String)
   */
  @Override
  public String getAuthorName(String commitId) {
    if (commitId.equals(IVCS.WORKINGTREE)) {
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getCommitMessage(java.lang.String)
   */
  @Override
  public String getCommitMessage(String commitId) {
    if (commitId.equals(IVCS.WORKINGTREE)) {
      try {
        String merge_msg = git.getRepository().readMergeCommitMsg();
        return merge_msg == null ? "" : merge_msg;
      } catch (Exception e) {
        return e.getMessage();
      }
    } else {
      RevCommit commit = resolve(commitId);
      return commit.getFullMessage();
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getCommitId(java.lang.String)
   */
  @Override
  public String getCommitId(String revstr) {
    ObjectId id = null;
    try {
      id = git.getRepository().resolve(revstr);
    } catch (RevisionSyntaxException e) {
      e.printStackTrace();
    } catch (AmbiguousObjectException e) {
      e.printStackTrace();
    } catch (IncorrectObjectTypeException e) {
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

  @Override
  public String getParentCommitId(String revstr) {
    return getCommitId(revstr + "~");
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getBranch()
   */
  @Override
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getLocalBranches()
   */
  @Override
  public List<String> getLocalBranches() {
    return getBranches(null);
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getBranches()
   */
  @Override
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getRemote()
   */
  @Override
  public String getRemote() {
    try {
      StoredConfig config = git.getRepository().getConfig();
      RemoteConfig remoteConfig = new RemoteConfig(config, Constants.DEFAULT_REMOTE_NAME);
      return remoteConfig.getURIs().iterator().next().toString();
    } catch (Exception e) {
      return "";
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#addRemote(java.lang.String)
   */
  @Override
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#removeRemote()
   */
  @Override
  public void removeRemote() {
    RemoteRemoveCommand cmd = git.remoteRemove();
    cmd.setName(Constants.DEFAULT_REMOTE_NAME);
    try {
      cmd.call();
    } catch (GitAPIException e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#hasRemote()
   */
  @Override
  public boolean hasRemote() {
    StoredConfig config = git.getRepository().getConfig();
    Set<String> remotes = config.getSubsections(ConfigConstants.CONFIG_REMOTE_SECTION);
    return remotes.contains(Constants.DEFAULT_REMOTE_NAME);
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#commit(java.lang.String, java.lang.String)
   */
  @Override
  public boolean commit(String authorName, String message) {
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
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getRevisions()
   */
  @Override
  public List<ObjectRevision> getRevisions() {
    List<ObjectRevision> revisions = new ArrayList<>();
    try {
      if (!isClean()
          || git.getRepository().getRepositoryState() == RepositoryState.MERGING_RESOLVED) {
        GitObjectRevision rev =
            new GitObjectRevision(WORKINGTREE, "*", new Date(), " // " + IVCS.WORKINGTREE);
        revisions.add(rev);
      }
      Iterable<RevCommit> iterable = git.log().call();
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getUnstagedFiles()
   */
  @Override
  public List<UIFile> getUnstagedFiles() {
    List<UIFile> files = new ArrayList<>();
    Status status = null;
    try {
      status = git.status().call();
    } catch (Exception e) {
      e.printStackTrace();
      return files;
    }
    status
        .getUntracked()
        .forEach(
            name -> {
              files.add(new UIFile(name, ChangeType.ADD, false));
            });
    status
        .getModified()
        .forEach(
            name -> {
              files.add(new UIFile(name, ChangeType.MODIFY, false));
            });
    status
        .getConflicting()
        .forEach(
            name -> {
              files.add(new UIFile(name, ChangeType.MODIFY, false));
            });
    status
        .getMissing()
        .forEach(
            name -> {
              files.add(new UIFile(name, ChangeType.DELETE, false));
            });
    return files;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getStagedFiles()
   */
  @Override
  public List<UIFile> getStagedFiles() {
    List<UIFile> files = new ArrayList<>();
    Status status = null;
    try {
      status = git.status().call();
    } catch (Exception e) {
      e.printStackTrace();
      return files;
    }
    status
        .getAdded()
        .forEach(
            name -> {
              files.add(new UIFile(name, ChangeType.ADD, true));
            });
    status
        .getChanged()
        .forEach(
            name -> {
              files.add(new UIFile(name, ChangeType.MODIFY, true));
            });
    status
        .getRemoved()
        .forEach(
            name -> {
              files.add(new UIFile(name, ChangeType.DELETE, true));
            });
    return files;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#getStagedFiles(java.lang.String, java.lang.String)
   */
  @Override
  public List<UIFile> getStagedFiles(String oldCommitId, String newCommitId) {
    List<UIFile> files = new ArrayList<>();
    try {
      List<DiffEntry> diffs =
          getDiffCommand(oldCommitId, newCommitId).setShowNameAndStatusOnly(true).call();
      RenameDetector rd = new RenameDetector(git.getRepository());
      rd.addAll(diffs);
      diffs = rd.compute();
      diffs.forEach(
          diff -> {
            files.add(
                new UIFile(
                    diff.getChangeType() == ChangeType.DELETE
                        ? diff.getOldPath()
                        : diff.getNewPath(),
                    diff.getChangeType(),
                    false));
          });
    } catch (Exception e) {
      e.printStackTrace();
    }
    return files;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#hasStagedFiles()
   */
  @Override
  public boolean hasStagedFiles() {
    if (git.getRepository().getRepositoryState() == RepositoryState.SAFE) {
      return !getStagedFiles().isEmpty();
    } else {
      return git.getRepository().getRepositoryState().canCommit();
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#initRepo(java.lang.String)
   */
  @Override
  public void initRepo(String baseDirectory) throws Exception {
    git = Git.init().setDirectory(new File(baseDirectory)).call();
    directory = baseDirectory;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#openRepo(java.lang.String)
   */
  @Override
  public void openRepo(String baseDirectory) throws Exception {
    git = Git.open(new File(baseDirectory));
    directory = baseDirectory;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#closeRepo()
   */
  @Override
  public void closeRepo() {
    git.close();
    git = null;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#add(java.lang.String)
   */
  @Override
  public void add(String filepattern) {
    try {
      if (filepattern.endsWith(".ours") || filepattern.endsWith(".theirs")) {
        FileUtils.rename(
            new File(directory, filepattern),
            new File(directory, FilenameUtils.removeExtension(filepattern)),
            StandardCopyOption.REPLACE_EXISTING);
        filepattern = FilenameUtils.removeExtension(filepattern);
        org.apache.commons.io.FileUtils.deleteQuietly(new File(directory, filepattern + ".ours"));
        org.apache.commons.io.FileUtils.deleteQuietly(new File(directory, filepattern + ".theirs"));
      }
      git.add().addFilepattern(filepattern).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#rm(java.lang.String)
   */
  @Override
  public void rm(String filepattern) {
    try {
      git.rm().addFilepattern(filepattern).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  /** Reset to a commit (mixed) */
  @Override
  public void reset(String name) {
    try {
      git.reset().setRef(name).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  /** Reset a file to HEAD (mixed) */
  @Override
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

  @Override
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#pull()
   */
  @Override
  public boolean pull() {
    if (hasUncommittedChanges()) {
      showMessageBox(
          BaseMessages.getString(PKG, "Dialog.Error"),
          BaseMessages.getString(PKG, "Git.Dialog.UncommittedChanges.Message"));
      return false;
    }
    if (!hasRemote()) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), "Please setup a remote");
      return false;
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
        showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      }
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#push()
   */
  @Override
  public boolean push() {
    return push("default");
  }

  @Override
  public boolean push(String type) {
    if (!hasRemote()) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), "Please setup a remote");
      return false;
    }
    String name = null;
    List<String> names;
    EnterSelectionDialog esd;
    switch (type) {
      case IVCS.TYPE_BRANCH:
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
      case IVCS.TYPE_TAG:
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
        showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      }
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
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
                  update -> { // for each failed refspec
                    sb.append(
                        result.getURI().toString()
                            + "\n"
                            + update.getSrcRef().toString()
                            + "\n"
                            + update.getStatus().toString()
                            + (update.getMessage() == null ? "" : "\n" + update.getMessage())
                            + "\n\n");
                  });
          if (sb.length() == 0) {
            showMessageBox(
                BaseMessages.getString(PKG, "Dialog.Success"),
                BaseMessages.getString(PKG, "Dialog.Success"));
          } else {
            showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), sb.toString());
          }
        });
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#diff(java.lang.String, java.lang.String)
   */
  @Override
  public String diff(String oldCommitId, String newCommitId) throws Exception {
    return diff(oldCommitId, newCommitId, null);
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#diff(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
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

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.VCS#open(java.lang.String, java.lang.String)
   */
  @Override
  public InputStream open(String file, String commitId) {
    if (commitId.equals(WORKINGTREE)) {
      String baseDirectory = getDirectory();
      String filePath = baseDirectory + Const.FILE_SEPARATOR + file;
      try {
        return new FileInputStream(new File(filePath));
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      return null;
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
      e.printStackTrace();
    } catch (IncorrectObjectTypeException e) {
      e.printStackTrace();
    } catch (CorruptObjectException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public boolean cloneRepo(String directory, String uri) {
    CloneCommand cmd = Git.cloneRepository();
    cmd.setDirectory(new File(directory));
    cmd.setURI(uri);
    cmd.setCredentialsProvider(credentialsProvider);
    try {
      Git git = cmd.call();
      git.close();
      return true;
    } catch (Exception e) {
      if ((e instanceof TransportException)
          && ((e.getMessage()
                  .contains(
                      "Authentication is required but no CredentialsProvider has been registered")
              || e.getMessage().contains("not authorized")))) {
        if (promptUsernamePassword()) {
          return cloneRepo(directory, uri);
        }
      } else {
        showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      }
    }
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.IVCS#checkout(java.lang.String)
   */
  @Override
  public void checkout(String name) {
    try {
      git.checkout().setName(name).call();
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  @Override
  public void checkoutBranch(String name) {
    checkout(name);
  }

  @Override
  public void checkoutTag(String name) {
    checkout(name);
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.IVCS#revertFile(java.lang.String)
   */
  @Override
  public void revertPath(String path) {
    try {
      // Delete added files
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
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.IVCS#createBranch(java.lang.String)
   */
  @Override
  public boolean createBranch(String value) {
    try {
      git.branchCreate().setName(value).call();
      checkoutBranch(getExpandedName(value, IVCS.TYPE_BRANCH));
      return true;
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hop.git.spoon.model.IVCS#deleteBranch(java.lang.String, boolean)
   */
  @Override
  public boolean deleteBranch(String name, boolean force) {
    try {
      git.branchDelete()
          .setBranchNames(getExpandedName(name, IVCS.TYPE_BRANCH))
          .setForce(force)
          .call();
      return true;
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  private boolean mergeBranch(String value, String mergeStrategy) {
    try {
      ObjectId obj = git.getRepository().resolve(value);
      MergeResult result =
          git.merge().include(obj).setStrategy(MergeStrategy.get(mergeStrategy)).call();
      if (result.getMergeStatus().isSuccessful()) {
        showMessageBox(
            BaseMessages.getString(PKG, "Dialog.Success"),
            BaseMessages.getString(PKG, "Dialog.Success"));
        return true;
      } else {
        showMessageBox(
            BaseMessages.getString(PKG, "Dialog.Error"), result.getMergeStatus().toString());
        if (result.getMergeStatus() == MergeStatus.CONFLICTING) {
          result
              .getConflicts()
              .keySet()
              .forEach(
                  path -> {
                    checkout(path, Constants.HEAD, ".ours");
                    checkout(path, getExpandedName(value, IVCS.TYPE_BRANCH), ".theirs");
                  });
          return true;
        }
      }
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
    return false;
  }

  @Override
  public boolean merge() {
    if (hasUncommittedChanges()) {
      showMessageBox(
          BaseMessages.getString(PKG, "Dialog.Error"),
          BaseMessages.getString(PKG, "Git.Dialog.UncommittedChanges.Message"));
      return false;
    }
    MergeBranchDialog dialog = new MergeBranchDialog(shell);
    List<String> branches = getBranches();
    branches.remove(getBranch());
    dialog.setBranches(branches);
    if (dialog.open() == Window.OK) {
      String branch = dialog.getSelectedBranch();
      String mergeStrategy = dialog.getSelectedMergeStrategy();
      return mergeBranch(branch, mergeStrategy);
    }
    return false;
  }

  private boolean hasUncommittedChanges() {
    try {
      return git.status().call().hasUncommittedChanges();
    } catch (NoWorkTreeException | GitAPIException e) {
      e.printStackTrace();
      return false;
    }
  }

  private void checkout(String path, String commitId, String postfix) {
    InputStream stream = open(path, commitId);
    File file = new File(directory + Const.FILE_SEPARATOR + path + postfix);
    try {
      org.apache.commons.io.FileUtils.copyInputStreamToFile(stream, file);
      stream.close();
    } catch (IOException e) {
      e.printStackTrace();
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

  @Override
  public String getShortenedName(String name, String type) {
    if (name.length() == Constants.OBJECT_ID_STRING_LENGTH) {
      return name.substring(0, 7);
    } else {
      return Repository.shortenRefName(name);
    }
  }

  @Override
  public boolean isClean() {
    try {
      return git.status().call().isClean();
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
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

  @Override
  public boolean createTag(String name) {
    try {
      git.tag().setName(name).call();
      return true;
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  @Override
  public boolean deleteTag(String name) {
    try {
      git.tagDelete().setTags(getExpandedName(name, IVCS.TYPE_TAG)).call();
      return true;
    } catch (GitAPIException e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
      return false;
    }
  }

  @Override
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

  private RevCommit resolve(String commitId) {
    ObjectId id = null;
    try {
      id = git.getRepository().resolve(commitId);
    } catch (RevisionSyntaxException e1) {
      e1.printStackTrace();
    } catch (AmbiguousObjectException e1) {
      e1.printStackTrace();
    } catch (IncorrectObjectTypeException e1) {
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
    return new EnterSelectionDialog(shell, choices, shellText, message);
  }
}
