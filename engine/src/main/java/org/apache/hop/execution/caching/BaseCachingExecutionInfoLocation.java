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
 *
 */

package org.apache.hop.execution.caching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.IExecutionMatcher;
import org.apache.hop.execution.IExecutionSelector;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

@Getter
@Setter
public abstract class BaseCachingExecutionInfoLocation implements IExecutionInfoLocation {
  @GuiWidgetElement(
      id = "persistenceDelay",
      order = "900",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::CachingFileExecutionInfoLocation.PersistenceDelay.Tooltip",
      label = "i18n::CachingFileExecutionInfoLocation.PersistenceDelay.Label")
  @HopMetadataProperty
  protected String persistenceDelay = "5000";

  @GuiWidgetElement(
      id = "maxCacheSize",
      order = "905",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::CachingFileExecutionInfoLocation.MaxCacheSize.Tooltip",
      label = "i18n::CachingFileExecutionInfoLocation.MaxCacheSize.Label")
  @HopMetadataProperty
  protected String maxCacheSize = "50";

  /**
   * Locations saved before {@code maxCacheAge} existed omit the property. The metadata loader
   * leaves this initializer in place, so those locations keep the original one-day age.
   */
  public static final String LEGACY_MAX_CACHE_AGE = "86400000";

  /** Age applied when a caching location is created in the GUI, not when an old file is loaded. */
  public static final String NEW_LOCATION_MAX_CACHE_AGE = "600000";

  /**
   * Logging text kept on one cache entry. Matches the execution viewer's default display limit and
   * keeps the newest lines, so a long run cannot retain the whole log buffer here as well.
   */
  public static final int MAX_CACHED_LOGGING_TEXT_CHARS = 2_000_000;

  @GuiWidgetElement(
      id = "maxCacheAge",
      order = "910",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::CachingFileExecutionInfoLocation.MaxCacheAge.Tooltip",
      label = "i18n::CachingFileExecutionInfoLocation.MaxCacheAge.Label")
  @HopMetadataProperty
  protected String maxCacheAge = LEGACY_MAX_CACHE_AGE;

  protected IVariables variables;
  protected IHopMetadataProvider metadataProvider;

  // This is the main cache
  protected Map<String, CacheEntry> cache;

  protected Timer cacheTimer;

  /**
   * Set when {@link #close()} has cancelled the timer. A timer task that already passed {@code
   * schedule} must not persist after that, and {@link #initialize} starts a fresh timer.
   */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private volatile boolean cacheClosed = true;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private boolean loggedManageCacheError;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private String lastManageCacheError;

  protected final AtomicBoolean locked;

  protected int delay;
  protected int maxAge;
  protected int maxSize;

  protected BaseCachingExecutionInfoLocation() {
    cache = new LinkedHashMap<>(16, 0.75f, true);
    this.cacheTimer = null;
    this.locked = new AtomicBoolean(false);
  }

  protected BaseCachingExecutionInfoLocation(BaseCachingExecutionInfoLocation location) {
    this();
    this.maxCacheSize = location.maxCacheSize;
    this.maxCacheAge = location.maxCacheAge;
    this.persistenceDelay = location.persistenceDelay;
    this.variables = location.variables;
    this.metadataProvider = location.metadataProvider;
    this.delay = location.delay;
    this.maxAge = location.maxAge;
    this.maxSize = location.maxSize;
  }

  public abstract BaseCachingExecutionInfoLocation clone();

  protected abstract void persistCacheEntry(CacheEntry cacheEntry) throws HopException;

  protected abstract CacheEntry loadCacheEntry(String executionId) throws HopException;

  protected abstract void deleteCacheEntry(CacheEntry cacheEntry) throws HopException;

  protected abstract void retrieveIds(
      boolean includeChildren, Set<DatedId> ids, int limit, IExecutionSelector selector)
      throws HopException;

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    this.variables = variables;
    this.metadataProvider = metadataProvider;

    // The default persistence delay is 1 minute
    //
    delay = Const.toInt(variables.resolve(persistenceDelay), 60000);

    // The default maximum cache size is 50
    //
    maxSize = Const.toInt(variables.resolve(maxCacheSize), 50);
    if (maxSize <= 0) {
      maxSize = 50;
    }

    // A missing age is the pre-existing 1 day. New GUI locations save 10 minutes explicitly.
    //
    maxAge = Const.toInt(variables.resolve(maxCacheAge), Integer.parseInt(LEGACY_MAX_CACHE_AGE));

    // Let's start a timer to manage the cache every second or so.
    // Cancel any previous timer first: a second initialize() used to leave the old one running
    // after close() disconnected the only connection that timer still wrote through.
    //
    synchronized (this) {
      cacheClosed = false;
      loggedManageCacheError = false;
      lastManageCacheError = null;
      Timer previousTimer = cacheTimer;
      cacheTimer = null;
      ExecutorUtil.cleanup(previousTimer);
      cacheTimer = new Timer("Caching execution location timer", true);
      TimerTask cacheManageTask =
          new TimerTask() {
            @Override
            public void run() {
              manageCache();
            }
          };
      cacheTimer.schedule(cacheManageTask, 1000L, 1000L);
    }
  }

  @Override
  public synchronized void unBuffer(String executionId) {
    cache.remove(executionId);
  }

  protected synchronized void manageCache() {
    if (cacheClosed) {
      return;
    }
    try {
      // Let's make sure we never run this method in parallel
      //
      if (locked.get()) {
        return;
      }
      locked.set(true);

      // See which dirty cache entries haven't been saved in a while.
      //
      for (CacheEntry cacheEntry : cache.values()) {
        if (cacheEntry.needsWriting(delay)) {
          try {
            persistCacheEntry(cacheEntry);
          } catch (Exception e) {
            LogChannel.GENERAL.logError(
                "Error persisting cache entry for " + cacheEntry.getId(), e);
          }
        }
      }

      // Perhaps there are cache entries which are getting too old?
      //
      List<String> tooOld = new ArrayList<>();
      for (CacheEntry cacheEntry : cache.values()) {
        if (cacheEntry.isTooOld(maxAge)) {
          tooOld.add(cacheEntry.getId());
        }
      }
      // Remove these entries.
      //
      tooOld.forEach(id -> cache.remove(id));

      enforceMaxCacheSize();

      loggedManageCacheError = false;
      lastManageCacheError = null;
    } catch (Exception e) {
      // A dead JDBC connection used to log a full stack trace from this timer once a second.
      String message = e.getMessage();
      if (!loggedManageCacheError || !StringUtils.equals(message, lastManageCacheError)) {
        LogChannel.GENERAL.logError("Error managing execution information location cache", e);
        loggedManageCacheError = true;
        lastManageCacheError = message;
      }
    } finally {
      locked.set(false);
    }
  }

  protected synchronized void enforceMaxCacheSize() {
    int max = maxSize > 0 ? maxSize : 50;
    if (cache.size() <= max) {
      return;
    }
    var iterator = cache.entrySet().iterator();
    while (iterator.hasNext() && cache.size() > max) {
      Map.Entry<String, CacheEntry> entry = iterator.next();
      CacheEntry cacheEntry = entry.getValue();
      if (cacheEntry != null && cacheEntry.isDirty()) {
        try {
          persistCacheEntry(cacheEntry);
        } catch (Exception e) {
          // A failed write must stay in memory. Dropping it here loses the only copy.
          LogChannel.GENERAL.logError(
              "Error persisting cache entry during eviction: " + cacheEntry.getId(), e);
          continue;
        }
      }
      iterator.remove();
    }
  }

  @Override
  public synchronized void close() throws HopException {
    // Stop the timer before the final flush. Tasks that are already inside manageCache hold this
    // lock and finish first; tasks still queued see cacheClosed and return.
    cacheClosed = true;
    Timer timer = cacheTimer;
    cacheTimer = null;
    ExecutorUtil.cleanup(timer);
    HopException failure = null;
    for (Map.Entry<String, CacheEntry> mapEntry : new ArrayList<>(cache.entrySet())) {
      CacheEntry cacheEntry = mapEntry.getValue();
      if (cacheEntry != null && cacheEntry.isDirty()) {
        try {
          persistCacheEntry(cacheEntry);
        } catch (Exception e) {
          // Leave this entry. A later close() can retry it. Clearing it drops unsaved state.
          if (failure == null) {
            failure =
                new HopException("Error persisting caching execution information location", e);
          }
          continue;
        }
      }
      cache.remove(mapEntry.getKey());
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public synchronized void clearCaches() {
    cache.clear();
  }

  @Override
  public synchronized void registerExecution(Execution execution) throws HopException {
    /*
     We're going to collect execution information of actions along with the parent workflow.
     Similarly, we're doing to add transform execution information under its parent pipeline.
     This way, we'll always just have one simple file to deal with.
    */
    ExecutionType type = execution.getExecutionType();
    if (type == ExecutionType.Pipeline || type == ExecutionType.Workflow) {
      addExecutionToCache(execution);
      // Persist immediately so distributed engines (Spark/Beam executors) can load the parent
      // CacheEntry from disk before calling registerData / register child executions.
      CacheEntry entry = cache.get(execution.getId());
      if (entry != null) {
        persistCacheEntry(entry);
      }
    } else {
      addChildExecutionToCache(execution);
    }
  }

  @Override
  public List<String> getExecutionIds(boolean includeChildren, int limit) throws HopException {
    Set<DatedId> ids = new HashSet<>();

    // The data in the cache is the most recent, so we start with that.
    //
    getExecutionIdsFromCache(ids, includeChildren);

    // Get all the IDs from disk if we don't have it in the cache.
    //
    retrieveIds(includeChildren, ids, limit, IExecutionSelector.ALL);

    // Reverse sort the IDs by date
    //
    List<DatedId> datedIds = new ArrayList<>(ids);
    datedIds.sort(Comparator.comparing(DatedId::getDate).reversed());

    // Take only the first from the list
    //
    int iLimit;
    if (limit > 0) {
      iLimit = Math.min(limit, datedIds.size());
    } else {
      iLimit = datedIds.size();
    }

    List<String> list = new ArrayList<>();
    for (int i = 0; i < iLimit; i++) {
      list.add(datedIds.get(i).getId());
    }
    return list;
  }

  @Override
  public List<String> findExecutionIDs(IExecutionSelector selector) throws HopException {
    final IExecutionSelector activeSelector = selector == null ? IExecutionSelector.ALL : selector;
    Set<DatedId> dateIds = new HashSet<>();

    if (activeSelector.isSelectingByUuid()) {
      // We try the cache and simply loading the file itself by ID.
      //
      CacheEntry cacheEntry = loadCacheEntry(activeSelector.filterText());
      if (cacheEntry != null) {
        return List.of(cacheEntry.getId());
      }
    }

    // The data in the cache is the most recent, so we start with that.
    //
    getExecutionIdsFromCache(dateIds, activeSelector);

    // Get all the IDs from disk if we don't have it in the cache.
    //
    retrieveIds(!activeSelector.isSelectingParents(), dateIds, 50, activeSelector);

    // Reverse sort the IDs by date
    //
    List<DatedId> datedIds = new ArrayList<>(dateIds);
    // Newest first
    datedIds.sort(Comparator.comparing(DatedId::getDate).reversed());

    // Take only the first from the list
    //
    List<String> list = new ArrayList<>();
    for (DatedId datedId : datedIds) {
      list.add(datedId.getId());
    }
    return list;
  }

  /**
   * Add the execution to the cache as a top level object.
   *
   * @param execution The execution to add to the cache
   */
  protected synchronized void addExecutionToCache(Execution execution) {
    // Check if the execution is already in the cache...
    CacheEntry entry = cache.get(execution.getId());
    if (entry == null) {
      entry = new CacheEntry();
      entry.setId(execution.getId());
    }
    entry.setExecution(execution);
    entry.setName(execution.getName());
    entry.setDirty(true);
    entry.setLastWritten(null);

    cache.put(execution.getId(), entry);
    enforceMaxCacheSize();
  }

  protected synchronized void addChildExecutionToCache(Execution execution) throws HopException {
    // Find the parent in the cache (or load from disk for multi-process engines).
    //
    CacheEntry entry = findCacheEntry(execution.getParentId());
    if (entry != null) {
      entry.addChildExecution(execution);
    } else {
      LogChannel.GENERAL.logError(
          "Unable to register child execution '"
              + execution.getId()
              + "': parent execution '"
              + execution.getParentId()
              + "' not found in cache or on disk");
    }
  }

  @Override
  public synchronized void updateExecutionState(ExecutionState executionState) throws HopException {
    ExecutionType type = executionState.getExecutionType();
    if (type == ExecutionType.Pipeline || type == ExecutionType.Workflow) {
      addStateToCache(executionState);
    } else {
      addChildStateToCache(executionState);
    }
  }

  /**
   * Pipeline and workflow updates carry only the lines written since {@code lastLogLineNr}. Append
   * that delta and keep the newest characters. A full snapshot ({@code lastLogLineNr == null}) is
   * capped the same way so the cache does not keep a second copy of the central log buffer.
   */
  static void appendLoggingDelta(ExecutionState previous, ExecutionState update) {
    if (update == null) {
      return;
    }
    String delta = capLoggingText(update.getLoggingText());
    if (update.getLastLogLineNr() != null && previous != null) {
      String oldText = previous.getLoggingText();
      if (StringUtils.isNotEmpty(oldText) && StringUtils.isNotEmpty(delta)) {
        delta = capLoggingText(oldText + delta);
      } else if (StringUtils.isNotEmpty(oldText)) {
        delta = capLoggingText(oldText);
      }
    }
    update.setLoggingText(delta);
  }

  private static String capLoggingText(String text) {
    if (text == null || text.length() <= MAX_CACHED_LOGGING_TEXT_CHARS) {
      return text;
    }
    return text.substring(text.length() - MAX_CACHED_LOGGING_TEXT_CHARS);
  }

  protected synchronized void addStateToCache(ExecutionState executionState) throws HopException {
    CacheEntry entry = cache.get(executionState.getId());
    if (entry == null) {
      // Load from disk (separate process / cache eviction) — same pattern as registerData
      entry = findCacheEntry(executionState.getId());
    }
    if (entry == null) {
      // Lookup by parent (happens when a pipeline is executed by a transform)
      entry = findCacheEntryWithParent(executionState.getParentId());
    }
    if (entry != null) {
      // setExecutionState flags dirty so close()/timer flush include the state
      appendLoggingDelta(entry.getExecutionState(), executionState);
      entry.setExecutionState(executionState);
    } else {
      LogChannel.GENERAL.logError(
          "Unable to update execution state for '"
              + executionState.getId()
              + "': parent entry not found in cache or on disk");
    }
  }

  protected synchronized CacheEntry findCacheEntryWithParent(String parentId) {
    if (StringUtils.isEmpty(parentId)) {
      return null;
    }
    CacheEntry found = null;
    Collection<CacheEntry> values = cache.values();
    for (CacheEntry cacheEntry : values) {
      if (cacheEntry.getExecution() == null) {
        continue;
      }
      String execParent = cacheEntry.getExecution().getParentId();
      if (parentId.equals(execParent)) {
        found = cacheEntry;
        break;
      }
      if (cacheEntry.getExecutionState() == null) {
        continue;
      }
      if (parentId.equals(cacheEntry.getExecutionState().getId())) {
        found = cacheEntry;
        break;
      }
    }
    if (found != null) {
      cache.get(found.getId());
      return found;
    }
    return null;
  }

  protected synchronized void addChildStateToCache(ExecutionState executionState)
      throws HopException {
    CacheEntry entry = cache.get(executionState.getParentId());
    if (entry == null) {
      // Load parent from disk (Spark/Beam executors have a separate empty in-memory cache)
      entry = findCacheEntry(executionState.getParentId());
    }
    if (entry == null) {
      // Lookup by parent (happens when a pipeline is executed by a transform)
      entry = findCacheEntryWithParent(executionState.getParentId());
    }
    if (entry != null) {
      // This parent entry should always exist
      entry.addChildExecutionState(executionState);
    }
  }

  @Override
  public synchronized boolean deleteExecution(String executionId) throws HopException {
    CacheEntry removed = cache.remove(executionId);
    deleteCacheEntry(removed);
    return true;
  }

  @Override
  public synchronized ExecutionState getExecutionState(String executionId) throws HopException {
    CacheEntry entry = findCacheEntry(executionId);
    if (entry == null) {
      return null;
    }
    if (entry.getId().equals(executionId)) {
      return entry.getExecutionState();
    }
    return entry.getChildExecutionState(executionId);
  }

  protected synchronized CacheEntry findCacheEntry(String executionId) throws HopException {
    // Check the cache first...
    CacheEntry found = null;
    for (CacheEntry cacheEntry : cache.values()) {
      // See if this is a parent in the cache.
      //
      if (cacheEntry.getId().equals(executionId)) {
        found = cacheEntry;
        break;
      }
      // Sometimes the ID of the execution state is different from the execution
      //
      if (cacheEntry.getExecutionState() != null
          && cacheEntry.getExecutionState().getId().equals(executionId)) {
        found = cacheEntry;
        break;
      }

      // Is it perhaps one of the children?
      //
      Execution childExecution = cacheEntry.getChildExecution(executionId);
      if (childExecution != null) {
        found = cacheEntry;
        break;
      }
    }

    if (found != null) {
      // Iteration does not update access order. A key lookup moves this entry to the newest end.
      cache.get(found.getId());
      return found;
    }

    // We still haven't found anything in the cache.
    // Let's load this from disk.
    //
    CacheEntry entry = loadCacheEntry(executionId);
    if (entry != null) {
      entry.setLastRead(new Date());
      entry.setLastWritten(new Date());
      entry.setDirty(false);

      cache.put(entry.getId(), entry);
      enforceMaxCacheSize();

      return entry;
    }
    return null;
  }

  @Override
  public synchronized ExecutionState getExecutionState(String executionId, boolean includeLogging)
      throws HopException {
    // This is the same as the other method ignoring the logging size, for now.
    //
    return getExecutionState(executionId);
  }

  @Override
  public synchronized String getExecutionStateLoggingText(String executionId, int sizeLimit)
      throws HopException {
    ExecutionState state = getExecutionState(executionId);
    if (state == null) {
      return null;
    }
    String log = state.getLoggingText();
    if (StringUtils.isEmpty(log)) {
      return null;
    }
    if (log.length() < sizeLimit) {
      return log;
    }
    return log.substring(0, sizeLimit);
  }

  /**
   * We need to add execution data to a child execution state.
   *
   * @param data The data to add to a child execution state.
   * @throws HopException In case we couldn't find or load the cache entry to register with
   */
  @Override
  public synchronized void registerData(ExecutionData data) throws HopException {
    // The ownerId in the data refers to the execution ID of the transform or action.
    // Parent may only exist on disk (driver registered it; this process is an executor).
    //
    CacheEntry entry = findCacheEntry(data.getParentId());
    if (entry != null) {
      entry.addExecutionData(data);
      // Flush promptly so other processes can merge samples when they persist parent state
      persistCacheEntry(entry);
    } else {
      LogChannel.GENERAL.logError(
          "Unable to register execution data for owner '"
              + data.getOwnerId()
              + "': parent execution '"
              + data.getParentId()
              + "' not found in cache or on disk");
    }
  }

  protected static void addChildIds(CacheEntry entry, Set<DatedId> ids) {
    for (String childId : entry.getChildIds()) {
      Execution childExecution = entry.getChildExecution(childId);
      // getChildIds() also includes owners that only contributed sample data or state
      // (e.g. local engine "all-transforms") without a full child Execution object.
      if (childExecution == null) {
        continue;
      }
      // We're only interested to know about pipelines and workflows here.
      //
      if (childExecution.getExecutionType() == ExecutionType.Pipeline
          || childExecution.getExecutionType() == ExecutionType.Workflow) {
        ids.add(new DatedId(childExecution.getId(), childExecution.getRegistrationDate()));
      }
    }
  }

  protected static void addChildIds(
      CacheEntry entry, Set<DatedId> ids, IExecutionSelector selector) {
    for (String childId : entry.getChildIds()) {
      Execution childExecution = entry.getChildExecution(childId);
      // Data-only owners (no Execution) are not selectable as top-level children.
      if (childExecution == null) {
        continue;
      }
      if (!selector.isSelected(childExecution)) {
        continue;
      }
      ExecutionState childExecutionState = entry.getChildExecutionState(childId);
      if (!selector.isSelected(childExecutionState)) {
        continue;
      }
      ids.add(new DatedId(childExecution.getId(), childExecution.getRegistrationDate()));
    }
  }

  protected synchronized void getExecutionIdsFromCache(Set<DatedId> ids, boolean includeChildren) {
    for (CacheEntry cacheEntry : cache.values()) {
      ids.add(new DatedId(cacheEntry.getId(), cacheEntry.getExecution().getRegistrationDate()));
      if (includeChildren) {
        addChildIds(cacheEntry, ids);
      }
    }
  }

  protected synchronized void getExecutionIdsFromCache(
      Set<DatedId> ids, IExecutionSelector selector) {
    for (CacheEntry cacheEntry : cache.values()) {
      if (selector.isSelected(cacheEntry.getExecution())
          && selector.isSelected(cacheEntry.getExecutionState())) {
        ids.add(new DatedId(cacheEntry.getId(), cacheEntry.getExecution().getRegistrationDate()));
      }
      if (!selector.isSelectingParents()) {
        addChildIds(cacheEntry, ids, selector);
      }
    }
  }

  @Override
  public Execution getExecution(String executionId) throws HopException {
    CacheEntry entry = findCacheEntry(executionId);
    if (entry == null) {
      return null;
    }
    return entry.getExecution();
  }

  @Override
  public List<Execution> findExecutions(String parentExecutionId) throws HopException {
    try {
      Set<Execution> executions = new HashSet<>();

      for (String id : getExecutionIds(true, 10000)) {
        Execution execution = getExecution(id);
        if (execution != null && parentExecutionId.equals(execution.getParentId())) {
          executions.add(execution);
        }
      }
      return executions.stream().toList();
    } catch (Exception e) {
      throw new HopException(
          "Error finding child executions for parent ID " + parentExecutionId, e);
    }
  }

  @Override
  public Execution findPreviousSuccessfulExecution(ExecutionType executionType, String name)
      throws HopException {
    try {
      List<Execution> executions =
          findExecutions(e -> e.getExecutionType() == executionType && name.equals(e.getName()));
      for (Execution execution : executions) {
        ExecutionState executionState = getExecutionState(execution.getId());
        if (executionState != null && !executionState.isFailed()) {
          return execution;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException("Error finding previous successful execution", e);
    }
  }

  @Override
  public List<Execution> findExecutions(IExecutionMatcher matcher) throws HopException {
    try {
      List<Execution> executions = new ArrayList<>();

      for (String id : getExecutionIds(true, 0)) {
        Execution execution = getExecution(id);
        if (execution != null && matcher.matches(execution)) {
          executions.add(execution);
        }
      }
      return executions;
    } catch (Exception e) {
      throw new HopException("Error finding executions with a matcher", e);
    }
  }

  @Override
  public ExecutionData getExecutionData(String parentExecutionId, String executionId)
      throws HopException {
    try {
      CacheEntry cacheEntry = findCacheEntry(parentExecutionId);
      if (cacheEntry == null) {
        return null;
      }

      // Local engine: all transform samples under a single "all-transforms" owner
      if (executionId == null) {
        ExecutionData allTransforms = cacheEntry.getExecutionData("all-transforms");
        if (allTransforms != null) {
          return allTransforms;
        }
        // Beam/Spark: per-transform (or per-copy) ExecutionData under the parent CacheEntry.
        // Aggregate so the GUI can resolve samples without a separate findChildIds round-trip.
        return aggregateChildExecutionData(cacheEntry, parentExecutionId);
      }

      ExecutionData data = cacheEntry.getExecutionData(executionId);
      if (data == null) {
        // Retry for the exception for transforms: "all-transforms" stored together.
        data = cacheEntry.getExecutionData("all-transforms");
      }
      return data;
    } catch (Exception e) {
      throw new HopException(
          "Error finding execution data for parent execution ID " + executionId, e);
    }
  }

  /**
   * Merge every {@link ExecutionData} stored under a parent cache entry (Beam/Spark style) into one
   * builder payload the GUI can filter by transform name.
   */
  private static ExecutionData aggregateChildExecutionData(
      CacheEntry cacheEntry, String parentExecutionId) {
    Map<String, ExecutionData> byOwner = cacheEntry.getChildExecutionData();
    if (byOwner == null || byOwner.isEmpty()) {
      return null;
    }
    ExecutionDataBuilder builder =
        ExecutionDataBuilder.of()
            .withParentId(parentExecutionId)
            .withOwnerId("all-transforms")
            .withExecutionType(ExecutionType.Transform)
            .withFinished(true)
            .withCollectionDate(new Date());
    boolean any = false;
    for (ExecutionData child : byOwner.values()) {
      if (child == null) {
        continue;
      }
      if (child.getDataSets() != null && !child.getDataSets().isEmpty()) {
        builder.addDataSets(child.getDataSets());
        any = true;
      }
      if (child.getSetMetaData() != null && !child.getSetMetaData().isEmpty()) {
        builder.addSetMeta(child.getSetMetaData());
        any = true;
      }
    }
    return any ? builder.build() : null;
  }

  @Override
  public Execution findLastExecution(ExecutionType executionType, String name) throws HopException {
    try {
      List<String> ids = getExecutionIds(true, 100);
      for (String id : ids) {
        Execution execution = getExecution(id);
        if (execution != null
            && execution.getExecutionType() == executionType
            && name.equals(execution.getName())) {
          return execution;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(
          "Error looking up the last execution of type " + executionType + " and name " + name, e);
    }
  }

  @Override
  public List<String> findChildIds(ExecutionType parentExecutionType, String parentExecutionId)
      throws HopException {
    CacheEntry cacheEntry = findCacheEntry(parentExecutionId);
    if (cacheEntry == null) {
      return Collections.emptyList();
    }
    return cacheEntry.getChildIds();
  }

  @Override
  public String findParentId(String childId) throws HopException {
    CacheEntry cacheEntry = findCacheEntry(childId);
    if (cacheEntry == null) {
      return null;
    }
    return cacheEntry.getId();
  }
}
