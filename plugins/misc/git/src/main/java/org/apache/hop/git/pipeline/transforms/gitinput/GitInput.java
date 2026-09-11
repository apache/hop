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

package org.apache.hop.git.pipeline.transforms.gitinput;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.git.provider.GitListOptions;
import org.apache.hop.git.provider.GitResourceClient;
import org.apache.hop.git.provider.GitResourceRecord;
import org.apache.hop.git.provider.GitResourceType;
import org.apache.hop.git.provider.LocalGitResourceClient;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class GitInput extends BaseTransform<GitInputMeta, GitInputData> {

  private static final Class<?> PKG = GitInputMeta.class;

  public GitInput(
      TransformMeta transformMeta,
      GitInputMeta meta,
      GitInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    GitInputSource source;
    try {
      source = GitInputSource.fromStored(resolve(meta.getSource()));
    } catch (IllegalArgumentException e) {
      logError(BaseMessages.getString(PKG, "GitInput.Error.UnknownSource", meta.getSource()));
      return false;
    }

    if (source == GitInputSource.LOCAL) {
      if (StringUtils.isEmpty(meta.getLocalRepositoryPath())) {
        logError(BaseMessages.getString(PKG, "GitInput.Error.LocalPathRequired"));
        return false;
      }
      return super.init();
    }

    if (StringUtils.isEmpty(meta.getConnectionName())) {
      logError(BaseMessages.getString(PKG, "GitInput.Error.ConnectionRequired"));
      return false;
    }

    String connectionName = resolve(meta.getConnectionName());
    try {
      IHopMetadataSerializer<GitConnection> serializer =
          metadataProvider.getSerializer(GitConnection.class);
      if (!serializer.exists(connectionName)) {
        throw new HopException(
            BaseMessages.getString(PKG, "GitInput.Error.ConnectionNotFound", connectionName));
      }
      data.gitConnection = serializer.load(connectionName);
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "GitInput.Error.ConnectionLoad", connectionName), e);
      return false;
    }

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {

    if (first) {
      first = false;
      openReader();
    }

    if (data.reader != null && data.reader.hasNext()) {
      GitResourceRecord record = data.reader.next();
      Object[] outputRow = record.toRow(meta.isIncludeRawJson());
      incrementLinesInput();
      putRow(data.outputRowMeta, outputRow);
      return true;
    }

    logSummaryIfNeeded();
    setOutputDone();
    return false;
  }

  private void openReader() throws HopException {
    data.outputRowMeta = new RowMeta();
    meta.getFields(
        data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);

    data.source = GitInputSource.fromStored(resolve(meta.getSource()));
    try {
      data.resourceType = GitResourceType.fromStored(resolve(meta.getResourceType()));
    } catch (IllegalArgumentException e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "GitInputMeta.CheckResult.UnknownResourceType", meta.getResourceType()),
          e);
    }
    data.pageSize = Const.toInt(resolve(meta.getPageSize()), GitListOptions.DEFAULT_PAGE_SIZE);
    data.maxPages = Const.toInt(resolve(meta.getMaxPages()), GitListOptions.DEFAULT_MAX_PAGES);

    GitListOptions options =
        new GitListOptions(
            resolve(meta.getState()),
            resolve(meta.getSince()),
            resolve(meta.getBranch()),
            data.pageSize,
            data.maxPages);

    if (data.source == GitInputSource.LOCAL) {
      openLocalReader(options);
    } else {
      openRemoteReader(options);
    }
  }

  private void openLocalReader(GitListOptions options) throws HopException {
    if (data.resourceType != GitResourceType.COMMITS
        && data.resourceType != GitResourceType.COMMIT_FILES) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "GitInput.Error.LocalResourceTypeUnsupported", data.resourceType.name()));
    }
    String repositoryPath = resolve(meta.getLocalRepositoryPath());
    LocalGitResourceClient.LocalRepositoryInfo info =
        LocalGitResourceClient.openRepositoryInfo(repositoryPath);
    data.owner = info.getOwner();
    data.repository = info.getRepositoryName();
    data.reader = LocalGitResourceClient.openReader(repositoryPath, options, data.resourceType);
  }

  private void openRemoteReader(GitListOptions options) throws HopException {
    if (data.resourceType == GitResourceType.COMMIT_FILES) {
      throw new HopException(BaseMessages.getString(PKG, "GitInput.Error.CommitFilesRemote"));
    }
    data.owner = resolve(meta.getOwner());
    data.repository = resolve(meta.getRepository());
    if (Utils.isEmpty(data.owner) || Utils.isEmpty(data.repository)) {
      throw new HopException(BaseMessages.getString(PKG, "GitInput.Error.RepositoryRequired"));
    }
    GitResourceClient client = GitResourceClient.forConnection(data.gitConnection, this);
    data.reader = client.openReader(data.resourceType, data.owner, data.repository, options);
  }

  @Override
  public void dispose() {
    // The reader may still hold provider resources - a JGit RevWalk keeps pack files open - and
    // processRow() is not guaranteed to run to completion when the pipeline stops early or a
    // downstream transform fails.
    if (data.reader != null) {
      data.reader.close();
      data.reader = null;
    }
    super.dispose();
  }

  private void logSummaryIfNeeded() {
    if (data.summaryLogged || data.reader == null) {
      return;
    }
    data.summaryLogged = true;

    int loaded = data.reader.count();
    String entity = data.resourceType.getEntityType();
    if (data.source == GitInputSource.LOCAL) {
      logBasic(
          BaseMessages.getString(
              PKG,
              "GitInput.Log.LocalSummary",
              String.valueOf(loaded),
              entity,
              resolve(meta.getLocalRepositoryPath()),
              String.valueOf(data.pageSize)));
    } else {
      logBasic(
          data.maxPages == GitListOptions.UNLIMITED_MAX_PAGES
              ? BaseMessages.getString(
                  PKG,
                  "GitInput.Log.RemoteSummaryAllPages",
                  String.valueOf(loaded),
                  entity,
                  data.owner + "/" + data.repository,
                  String.valueOf(data.pageSize))
              : BaseMessages.getString(
                  PKG,
                  "GitInput.Log.RemoteSummary",
                  String.valueOf(loaded),
                  entity,
                  data.owner + "/" + data.repository,
                  String.valueOf(data.pageSize),
                  String.valueOf(data.maxPages)));
    }

    String truncationNote = data.reader.getTruncationNote();
    if (truncationNote != null) {
      logBasic(BaseMessages.getString(PKG, "GitInput.Log.Truncated", truncationNote));
    }
  }
}
