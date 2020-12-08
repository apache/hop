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

import org.apache.hop.git.model.revision.ObjectRevision;
import org.eclipse.swt.widgets.Shell;

import java.io.InputStream;
import java.util.List;

public interface IVCS {

  String WORKINGTREE = "WORKINGTREE";
  String INDEX = "INDEX";
  String GIT = "Git";
  String SVN = "SVN";
  String TYPE_TAG = "tag";
  String TYPE_BRANCH = "branch";
  String TYPE_REMOTE = "remote";
  String TYPE_COMMIT = "commit";

  /**
   * Get the name of implementation (e.g., Git, SVN)
   *
   * @return
   */
  String getType();

  String getDirectory();

  /**
   * If the repository is clean and not dirty.
   *
   * @return
   */
  boolean isClean();

  /**
   * Get the author name for a commit
   *
   * @param commitId
   * @return
   * @throws Exception
   */
  String getAuthorName(String commitId);

  String getCommitMessage(String commitId);

  /**
   * Get SHA-1 commit Id
   *
   * @param revstr: (e.g., HEAD, SHA-1)
   * @return
   * @throws Exception
   */
  String getCommitId(String revstr);

  /**
   * Get SHA-1 commit Id
   *
   * @param revstr: (e.g., HEAD, SHA-1)
   * @return
   * @throws Exception
   */
  String getParentCommitId(String revstr);

  /**
   * Get an expanded name from shortened name (e.g., master -> refs/heads/master)
   *
   * @param name (e.g., master)
   * @param type
   * @return
   * @throws Exception
   */
  String getExpandedName(String name, String type);

  String getShortenedName(String name, String type);

  /**
   * Get the current branch
   *
   * @return Current branch
   */
  String getBranch();

  /**
   * Get a list of local branches
   *
   * @return
   */
  List<String> getLocalBranches();

  /**
   * Get a list of all (local + remote) branches
   *
   * @return
   */
  List<String> getBranches();

  String getRemote();

  void addRemote(String s);

  void removeRemote();

  boolean hasRemote();

  boolean commit(String authorName, String message);

  List<ObjectRevision> getRevisions();

  void setCredential(String username, String password);

  /**
   * Get the list of unstaged files
   *
   * @return
   */
  List<UIFile> getUnstagedFiles();

  /**
   * Get the list of staged files
   *
   * @return
   */
  List<UIFile> getStagedFiles();

  /**
   * Get the list of changed files between two commits
   *
   * @param oldCommitId
   * @param newCommitId
   * @return
   */
  List<UIFile> getStagedFiles(String oldCommitId, String newCommitId);

  // TODO should be renamed as canCommit()
  boolean hasStagedFiles();

  void initRepo(String baseDirectory) throws Exception;

  void openRepo(String baseDirectory) throws Exception;

  void closeRepo();

  void add(String filepattern);

  void rm(String filepattern);

  void reset(String name);

  /**
   * Reset a file to HEAD
   *
   * @param path of the file
   * @throws Exception
   */
  void resetPath(String path);

  /**
   * Equivalent of <tt>git fetch; git merge --ff</tt>
   *
   * @return true on success
   * @throws Exception
   * @see <a href="http://www.kernel.org/pub/software/scm/git/docs/git-pull.html">Git documentation
   *     about Pull</a>
   */
  boolean pull();

  boolean push();

  boolean push(String type);

  String diff(String oldCommitId, String newCommitId) throws Exception;

  String diff(String oldCommitId, String newCommitId, String file);

  InputStream open(String file, String commitId);

  /**
   * Checkout a commit
   *
   * @param name
   */
  void checkout(String name);

  void checkoutBranch(String name);

  void checkoutTag(String name);

  /**
   * Revert a file to the last commited state
   *
   * @param path
   */
  void revertPath(String path);

  boolean createBranch(String value);

  boolean deleteBranch(String name, boolean force);

  List<String> getTags();

  boolean createTag(String name);

  boolean deleteTag(String name);

  void setShell(Shell shell);

  boolean merge();

  boolean cloneRepo(String directory, String url);

  boolean rollback(String name);
}
