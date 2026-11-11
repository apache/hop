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

package org.apache.hop.lineage.datahub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage.RunEvent;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.lineage.datahub.DataHubClient.ReadException;
import org.apache.hop.lineage.datahub.DataHubSinkConfig.SchemaPolicy;
import org.apache.hop.lineage.datahub.DataHubTranslator.LifecycleChange;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.datahub.DataHubTranslator.SchemaCandidate;
import org.apache.hop.lineage.datahub.DataHubTranslator.StaleCheck;
import org.apache.hop.lineage.datahub.DataHubTranslator.Translation;
import org.apache.hop.lineage.openlineage.OpenLineageEmitter;

/**
 * Delivers mapped lineage events to DataHub: each event is translated into metadata change
 * proposals ({@link DataHubTranslator}) and written in one {@code ingestProposalBatch} request.
 *
 * <p>DataHub is written to directly rather than through its OpenLineage endpoint. That endpoint
 * needs server-wide settings that also change how every other OpenLineage producer is ingested,
 * ingests asynchronously, and gives every transform a pipeline of its own; writing the proposals
 * ourselves leaves the server as it is and makes Hop the only writer of the entities it describes.
 *
 * <p>Events can arrive out of order: one that could not be delivered waits in the spool while newer
 * ones go through, and a later process replays what an earlier one left behind. What an event says
 * about a pipeline, workflow or task (its description, its datasets, which tasks it has) is only
 * written while nothing newer is known, so a late event cannot undo a newer run.
 *
 * <p>The emitter runs on the dispatcher's worker thread, one batch at a time, and is the same for a
 * live event and one replayed from the spool.
 */
class DataHubEmitter implements OpenLineageEmitter {

  /** Proposals per request; a large pipeline's terminal event can carry many datasets. */
  static final int MAX_BATCH = 200;

  /**
   * Requests a refused batch may cost while the proposals DataHub refuses are singled out: enough
   * to find a few bad aspects among {@link #MAX_BATCH}, few enough not to hold up the queue.
   */
  static final int MAX_ISOLATION_REQUESTS = 40;

  /** The longest pause between two attempts of one request. */
  static final long MAX_BACKOFF_MS = 30_000L;

  /** How long what DataHub said about a dataset's schema is trusted before it is read again. */
  static final long SCHEMA_STATE_TTL_MS = 10 * 60_000L;

  /**
   * How long a proposal this process wrote is not written again unchanged. Most of what a run
   * writes is the same every time — the pipeline, its transforms, their datasets, often the job's
   * inputs and outputs — and every aspect costs DataHub a database write; a Pipeline Executor
   * running a pipeline per row would otherwise rewrite all of it for every row. The window is
   * short, so an entity deleted in DataHub in the meantime comes back soon enough.
   */
  static final long WRITTEN_TTL_MS = 10 * 60_000L;

  private static final int MAX_SCHEMA_STATES = 4_096;
  private static final int MAX_WRITTEN = 16_384;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  static final String FLOW_JOBS_QUERY =
      "query($urns: [String!]!) { entities(urns: $urns) { urn ... on DataFlow {"
          + " jobs: relationships(input: {types: [\"IsPartOf\"], direction: INCOMING, start: 0,"
          + " count: 1000}) { relationships { entity { urn ... on DataJob { status { removed }"
          + " properties { customProperties { key value } } } } } } } } }";
  static final String DATASETS_QUERY =
      "query($urns: [String!]!) { entities(urns: $urns) { urn ... on Dataset { exists"
          + " schemaMetadata { name } deprecation { deprecated note } } } }";

  /** The newest run of each task, to tell whether an event is older than what DataHub has. */
  static final String JOB_RUNS_QUERY =
      "query($urns: [String!]!) { entities(urns: $urns) { urn ... on DataJob {"
          + " runs(start: 0, count: 1) { runs { properties { created { time } } } } } } }";

  /** A task's inputs and outputs as stored, edges included (GraphQL's own type leaves them out). */
  static final String JOB_IO_QUERY =
      "query($urns: [String!]!) { entities(urns: $urns) { urn ... on DataJob {"
          + " aspects(input: {aspectNames: [\"dataJobInputOutput\"]}) { aspectName payload } } } }";

  private static final String JOB_IO = "dataJobInputOutput";

  private final DataHubTranslator translator;
  private final DataHubClient client;
  private final SchemaPolicy schemaPolicy;
  private final String actor;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final ILogChannel log;

  /** Events older than this were left in the spool by an earlier process. */
  private final long replayedBeforeMs;

  private final Map<String, SchemaState> schemaStates = lru(MAX_SCHEMA_STATES);
  private final Map<String, Long> written = lru(MAX_WRITTEN);

  /**
   * For each aspect of a pipeline, workflow or task, the time of the newest event this process
   * delivered it from: an older event's version of it is not written.
   */
  private final Map<String, Long> describedAt = lru(MAX_WRITTEN);

  private boolean readFailureReported;

  /** Whether Hop may write a dataset's schema, as last read from DataHub. */
  private record SchemaState(boolean writable, long readAtMs) {}

  /** What DataHub holds for a dataset, as far as the sink's decisions go. */
  private record DatasetState(boolean hasForeignSchema, boolean deprecated, String note) {}

  /**
   * A proposal of the batch: the event it is written for ({@code -1} for one the batch shares) and
   * when that event happened. {@code sendAs} is what goes out, when it differs from {@code mcp}.
   */
  private record Owned(Mcp mcp, int owner, long timeMs, Mcp sendAs) {
    Owned(Mcp mcp, int owner, long timeMs) {
      this(mcp, owner, timeMs, mcp);
    }

    Owned sending(Mcp other) {
      return new Owned(mcp, owner, timeMs, other);
    }
  }

  /** Something an event asks DataHub about, with that event. */
  private record Ask<T>(T item, int owner) {}

  DataHubEmitter(
      DataHubTranslator translator,
      DataHubClient client,
      SchemaPolicy schemaPolicy,
      String actor,
      int maxRetries,
      long retryBackoffMs,
      ILogChannel log) {
    this(
        translator,
        client,
        schemaPolicy,
        actor,
        maxRetries,
        retryBackoffMs,
        log,
        // Every event of this process is younger than the JVM it runs in.
        ManagementFactory.getRuntimeMXBean().getStartTime());
  }

  DataHubEmitter(
      DataHubTranslator translator,
      DataHubClient client,
      SchemaPolicy schemaPolicy,
      String actor,
      int maxRetries,
      long retryBackoffMs,
      ILogChannel log,
      long replayedBeforeMs) {
    this.translator = translator;
    this.client = client;
    this.schemaPolicy = schemaPolicy;
    this.actor = actor;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.log = log;
    this.replayedBeforeMs = replayedBeforeMs;
  }

  private static <V> Map<String, V> lru(int maxEntries) {
    return new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
        return size() > maxEntries;
      }
    };
  }

  @Override
  public Result emit(RunEvent event) {
    return emitAll(List.of(event)).get(0);
  }

  /**
   * Writes the events in one go: their proposals, with one read of DataHub per kind of decision
   * they need, in as few requests as {@link #MAX_BATCH} allows. Each event gets its own outcome: it
   * is only sent again when a proposal of its own did not get through, or when a decision it needs
   * could not be made.
   */
  @Override
  public List<Result> emitAll(List<RunEvent> events) {
    int count = events.size();
    Result[] results = new Result[count];
    Translation[] translations = new Translation[count];
    boolean[] retry = new boolean[count];
    for (int i = 0; i < count; i++) {
      try {
        translations[i] = translator.translate(events.get(i));
      } catch (RuntimeException e) {
        log.logError("Unable to translate a lineage event for DataHub; it is skipped", e);
        results[i] = Result.REJECTED;
      }
    }
    try {
      deliver(translations, retry, results);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      for (int i = 0; i < count; i++) {
        if (results[i] == null) {
          results[i] = Result.RETRY;
        }
      }
    }
    return Arrays.asList(results);
  }

  private void deliver(Translation[] translations, boolean[] retry, Result[] results)
      throws InterruptedException {
    Set<Integer> staleReplays = staleReplays(translations, retry);
    List<List<Mcp>> decided = decide(translations, retry);
    Set<Mcp> retirements = new HashSet<>();
    List<List<Mcp>> retired = retire(translations, staleReplays, retry, retirements);

    // Event by event, so a failed request only holds up the events from there on.
    List<Owned> proposals = new ArrayList<>();
    proposals.add(new Owned(DataHubTranslator.hopPlatform(), -1, 0));
    for (int i = 0; i < translations.length; i++) {
      Translation translation = translations[i];
      if (translation == null) {
        continue;
      }
      long time = translation.timeMs();
      for (Mcp mcp : translation.proposals()) {
        if (!describes(mcp) || !(staleReplays.contains(i) || newerDescribed(mcp, time))) {
          proposals.add(new Owned(mcp, i, time));
        }
      }
      for (Mcp mcp : decided.get(i)) {
        proposals.add(new Owned(mcp, i, time));
      }
      for (Mcp mcp : retired.get(i)) {
        if (!newerDescribed(mcp, time)) {
          proposals.add(new Owned(mcp, i, time));
        }
      }
    }

    long now = System.currentTimeMillis();
    List<Owned> toSend = new ArrayList<>();
    for (Owned owned : proposals) {
      if (!writtenRecently(owned.mcp(), now)) {
        toSend.add(owned);
      }
    }
    toSend = keepParentEdges(toSend, retry);
    Result[] outcomes = send(toSend.stream().map(Owned::sendAs).toList());

    for (int k = 0; k < toSend.size(); k++) {
      Mcp mcp = toSend.get(k).mcp();
      if (outcomes[k] == Result.SENT && retirements.contains(mcp)) {
        // What this process wrote for a retired task no longer holds: when the task comes back,
        // its next start has to write it again.
        String prefix = mcp.entityUrn() + '\u0000';
        written.keySet().removeIf(key -> key.startsWith(prefix));
      }
    }
    boolean[] failed = new boolean[results.length];
    int[] own = new int[results.length];
    int[] refused = new int[results.length];
    for (int k = 0; k < toSend.size(); k++) {
      Owned owned = toSend.get(k);
      if (outcomes[k] == Result.SENT) {
        written.put(key(owned.mcp()), now);
        if ("schemaMetadata".equals(owned.mcp().aspectName())) {
          remember(owned.mcp().entityUrn(), true);
        }
      }
      int owner = owned.owner();
      if (owner >= 0) {
        own[owner]++;
        failed[owner] |= outcomes[k] == Result.RETRY;
        refused[owner] += outcomes[k] == Result.REJECTED ? 1 : 0;
      }
    }
    for (int i = 0; i < results.length; i++) {
      if (results[i] != null) {
        continue;
      }
      if (retry[i] || failed[i]) {
        results[i] = Result.RETRY;
      } else if (own[i] > 0 && refused[i] == own[i]) {
        results[i] = Result.REJECTED;
      } else {
        results[i] = Result.SENT;
      }
    }
    // What a delivered event said, sent now or written unchanged before, is the newest known.
    for (Owned owned : proposals) {
      if (owned.owner() >= 0
          && results[owned.owner()] == Result.SENT
          && describes(owned.mcp())
          && "UPSERT".equals(owned.mcp().changeType())) {
        describedAt.merge(aspectKey(owned.mcp()), owned.timeMs(), Math::max);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Events older than what DataHub has
  // ---------------------------------------------------------------------------------------------

  /** Whether a proposal describes a pipeline, workflow or task (rather than a run or dataset). */
  private static boolean describes(Mcp mcp) {
    return "dataFlow".equals(mcp.entityType()) || "dataJob".equals(mcp.entityType());
  }

  /** Whether this process delivered the same aspect from a newer event. */
  private boolean newerDescribed(Mcp mcp, long timeMs) {
    if (!"UPSERT".equals(mcp.changeType())) {
      return false; // a patch only adds to what is there
    }
    Long newest = describedAt.get(aspectKey(mcp));
    return newest != null && newest > timeMs;
  }

  /**
   * The task events an earlier process left in the spool that are older than the task's newest run
   * in DataHub: a newer version of the pipeline or workflow has run since, and what they say about
   * the task no longer holds. Within one process {@link #describedAt} knows that already.
   */
  private Set<Integer> staleReplays(Translation[] translations, boolean[] retry)
      throws InterruptedException {
    Map<String, List<Integer>> byJob = new LinkedHashMap<>();
    for (int i = 0; i < translations.length; i++) {
      Translation translation = translations[i];
      if (translation != null
          && translation.jobUrn() != null
          && translation.timeMs() < replayedBeforeMs) {
        byJob.computeIfAbsent(translation.jobUrn(), urn -> new ArrayList<>()).add(i);
      }
    }
    if (byJob.isEmpty()) {
      return Set.of();
    }
    Set<Integer> stale = new HashSet<>();
    Map<String, Long> newestRuns;
    try {
      newestRuns = newestRuns(byJob.keySet());
    } catch (ReadException e) {
      reportReadFailure(e);
      if (e.worthRetrying()) {
        // Not known to be current: written later rather than possibly over something newer.
        for (List<Integer> owners : byJob.values()) {
          for (int i : owners) {
            stale.add(i);
            retry[i] = true;
          }
        }
      }
      return stale;
    }
    for (Map.Entry<String, List<Integer>> job : byJob.entrySet()) {
      Long newest = newestRuns.get(job.getKey());
      for (int i : job.getValue()) {
        if (newest != null && newest > translations[i].timeMs()) {
          stale.add(i);
        }
      }
    }
    return stale;
  }

  /** When each task last started a run, as far as DataHub knows. */
  private Map<String, Long> newestRuns(Collection<String> jobUrns)
      throws ReadException, InterruptedException {
    JsonNode data = read(JOB_RUNS_QUERY, jobUrns);
    Map<String, Long> newest = new HashMap<>();
    for (JsonNode job : data.path("entities")) {
      JsonNode created = job.path("runs").path("runs").path(0).path("properties").path("created");
      if (created.has("time")) {
        newest.put(job.path("urn").asText(), created.get("time").asLong());
      }
    }
    return newest;
  }

  // ---------------------------------------------------------------------------------------------
  // Retired tasks
  // ---------------------------------------------------------------------------------------------

  /**
   * Soft-deletes the tasks of each flow that its run no longer has: a transform or action removed
   * from the pipeline or workflow, or renamed. Only tasks written for the same Hop file are
   * candidates, and only those that have not run since: a task that ran after the run being checked
   * belongs to a newer version of the file, and this run's event is merely late. DataHub keeps a
   * soft-deleted task, and the task's next start restores it.
   *
   * @param all receives every retirement, to tell them apart once sent
   * @return the retirements per event
   */
  private List<List<Mcp>> retire(
      Translation[] translations, Set<Integer> staleReplays, boolean[] retry, Set<Mcp> all)
      throws InterruptedException {
    List<List<Mcp>> retirements = perEvent(translations.length);
    Map<String, Ask<StaleCheck>> byFlow = new LinkedHashMap<>();
    List<Integer> owners = new ArrayList<>();
    for (int i = 0; i < translations.length; i++) {
      if (translations[i] == null || staleReplays.contains(i)) {
        continue;
      }
      for (StaleCheck check : translations[i].staleChecks()) {
        // The last run of a flow in the batch decides for the flow.
        byFlow.put(check.flowUrn(), new Ask<>(check, i));
        owners.add(i);
      }
    }
    if (byFlow.isEmpty()) {
      return retirements;
    }
    try {
      Map<String, Ask<StaleCheck>> candidates = new LinkedHashMap<>();
      JsonNode data = read(FLOW_JOBS_QUERY, byFlow.keySet());
      for (JsonNode flow : data.path("entities")) {
        Ask<StaleCheck> ask = byFlow.get(flow.path("urn").asText());
        if (ask == null) {
          continue;
        }
        StaleCheck check = ask.item();
        for (JsonNode relationship : flow.path("jobs").path("relationships")) {
          JsonNode job = relationship.path("entity");
          String urn = job.path("urn").asText();
          // A soft-deleted task still "exists" for DataHub; its status says it was retired already.
          if (job.path("status").path("removed").asBoolean(false)
              || check.currentJobUrns().contains(urn)
              || !check
                  .filename()
                  .equals(customProperty(job, DataHubTranslator.FILENAME_PROPERTY))) {
            continue;
          }
          // Started in this process after the run being checked: DataHub may not list that run yet.
          Long started = describedAt.get(urn + '\u0000' + "dataJobInfo");
          if (started == null || started <= check.timeMs()) {
            candidates.put(urn, ask);
          }
        }
      }
      Map<String, Long> newestRuns =
          candidates.isEmpty() ? Map.of() : newestRuns(candidates.keySet());
      for (Map.Entry<String, Ask<StaleCheck>> candidate : candidates.entrySet()) {
        Long newest = newestRuns.get(candidate.getKey());
        if (newest != null && newest > candidate.getValue().item().timeMs()) {
          continue;
        }
        ObjectNode status = MAPPER.createObjectNode();
        status.put("removed", true);
        Mcp retirement = Mcp.upsert("dataJob", candidate.getKey(), "status", status);
        retirements.get(candidate.getValue().owner()).add(retirement);
        all.add(retirement);
        log.logBasic(
            "Retiring DataHub task "
                + candidate.getKey()
                + ": its pipeline or workflow no longer has it");
      }
    } catch (ReadException e) {
      reportReadFailure(e);
      if (e.worthRetrying()) {
        owners.forEach(i -> retry[i] = true);
      }
    }
    return retirements;
  }

  private static String customProperty(JsonNode entity, String key) {
    for (JsonNode property : entity.path("properties").path("customProperties")) {
      if (key.equals(property.path("key").asText())) {
        return property.path("value").asText(null);
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------------------------
  // Parent tasks
  // ---------------------------------------------------------------------------------------------

  /**
   * Keeps the parent tasks DataHub has for a first task of a pipeline or workflow. Its inputs and
   * outputs are written whole, which would otherwise drop the actions and transforms of other
   * workflows and pipelines that ran it (each one is added by a patch when it runs it, see {@code
   * DataHubTranslator.linkToParentJob}): only the hop graph of its own flow is Hop's to replace. A
   * task that could not be read is written later, rather than without its parents.
   */
  private List<Owned> keepParentEdges(List<Owned> toSend, boolean[] retry)
      throws InterruptedException {
    Map<String, Integer> roots = new LinkedHashMap<>();
    for (int k = 0; k < toSend.size(); k++) {
      Mcp mcp = toSend.get(k).mcp();
      if ("dataJob".equals(mcp.entityType())
          && JOB_IO.equals(mcp.aspectName())
          && "UPSERT".equals(mcp.changeType())
          && mcp.value().path("inputDatajobEdges").isEmpty()) {
        roots.put(mcp.entityUrn(), k);
      }
    }
    if (roots.isEmpty()) {
      return toSend;
    }
    Map<String, Set<String>> parents = new HashMap<>();
    try {
      JsonNode data = read(JOB_IO_QUERY, roots.keySet());
      for (JsonNode job : data.path("entities")) {
        for (JsonNode aspect : job.path("aspects")) {
          if (!JOB_IO.equals(aspect.path("aspectName").asText())) {
            continue;
          }
          JsonNode stored = MAPPER.readTree(aspect.path("payload").asText("{}"));
          for (JsonNode edge : stored.path("inputDatajobEdges")) {
            parents
                .computeIfAbsent(job.path("urn").asText(), urn -> new LinkedHashSet<>())
                .add(edge.path("destinationUrn").asText());
          }
        }
      }
    } catch (ReadException e) {
      reportReadFailure(e);
      if (!e.worthRetrying()) {
        return toSend;
      }
      Set<Integer> held = new HashSet<>(roots.values());
      List<Owned> kept = new ArrayList<>();
      for (int k = 0; k < toSend.size(); k++) {
        Owned owned = toSend.get(k);
        if (!held.contains(k)) {
          kept.add(owned);
        } else if (owned.owner() >= 0) {
          retry[owned.owner()] = true;
        }
      }
      return kept;
    } catch (IOException e) {
      log.logError("Unreadable task inputs and outputs from DataHub: " + e.getMessage());
      return toSend;
    }

    List<Owned> result = new ArrayList<>(toSend);
    for (Map.Entry<String, Integer> root : roots.entrySet()) {
      String jobUrn = root.getKey();
      Set<String> keep = new LinkedHashSet<>(parents.getOrDefault(jobUrn, Set.of()));
      // A parent linked earlier in this batch is not in DataHub yet.
      for (int k = 0; k < root.getValue(); k++) {
        Mcp earlier = toSend.get(k).mcp();
        if (earlier.entityUrn().equals(jobUrn)
            && JOB_IO.equals(earlier.aspectName())
            && "PATCH".equals(earlier.changeType())) {
          for (JsonNode op : earlier.value()) {
            if (op.path("path").asText().startsWith("/inputDatajobEdges/")) {
              keep.add(op.path("value").path("destinationUrn").asText());
            }
          }
        }
      }
      // Tasks of the task's own flow are the hop graph, which this aspect replaces.
      String flowUrn = DataHubUrns.flowOfJob(jobUrn);
      keep.removeIf(urn -> urn.isEmpty() || Objects.equals(flowUrn, DataHubUrns.flowOfJob(urn)));
      if (keep.isEmpty()) {
        continue;
      }
      Owned owned = toSend.get(root.getValue());
      ObjectNode value = owned.mcp().value().deepCopy();
      ArrayNode edges = array(value, "inputDatajobEdges");
      ArrayNode jobs = array(value, "inputDatajobs");
      for (String parent : keep) {
        edges.addObject().put("destinationUrn", parent);
        jobs.add(parent);
      }
      result.set(root.getValue(), owned.sending(Mcp.upsert("dataJob", jobUrn, JOB_IO, value)));
    }
    return result;
  }

  private static ArrayNode array(ObjectNode node, String field) {
    return node.get(field) instanceof ArrayNode existing ? existing : node.putArray(field);
  }

  // ---------------------------------------------------------------------------------------------
  // Decisions that depend on what DataHub holds
  // ---------------------------------------------------------------------------------------------

  /**
   * The schema and lifecycle proposals that DataHub's current state allows, per event. A dataset
   * another source describes (DataHub's own database ingestion, say) keeps that source's schema; a
   * deprecation a person wrote is never lifted. When DataHub cannot be read, the events that need
   * it wait for a later attempt; their lineage itself is written now.
   */
  private List<List<Mcp>> decide(Translation[] translations, boolean[] retry)
      throws InterruptedException {
    List<List<Mcp>> decided = perEvent(translations.length);
    List<Ask<SchemaCandidate>> schemas = new ArrayList<>();
    List<Ask<LifecycleChange>> lifecycle = new ArrayList<>();
    for (int i = 0; i < translations.length; i++) {
      if (translations[i] == null) {
        continue;
      }
      if (schemaPolicy != SchemaPolicy.NEVER) {
        for (SchemaCandidate schema : translations[i].schemas()) {
          schemas.add(new Ask<>(schema, i));
        }
      }
      for (LifecycleChange change : translations[i].lifecycle()) {
        lifecycle.add(new Ask<>(change, i));
      }
    }
    if (schemas.isEmpty() && lifecycle.isEmpty()) {
      return decided;
    }
    Set<String> toRead = new LinkedHashSet<>();
    long now = System.currentTimeMillis();
    if (schemaPolicy == SchemaPolicy.MISSING) {
      for (Ask<SchemaCandidate> schema : schemas) {
        SchemaState known = schemaStates.get(schema.item().datasetUrn());
        if (known == null || now - known.readAtMs() > SCHEMA_STATE_TTL_MS) {
          toRead.add(schema.item().datasetUrn());
        }
      }
    }
    for (Ask<LifecycleChange> change : lifecycle) {
      toRead.add(change.item().datasetUrn());
    }
    Map<String, DatasetState> states;
    try {
      states = toRead.isEmpty() ? Map.of() : readDatasets(toRead);
    } catch (ReadException e) {
      reportReadFailure(e);
      if (e.worthRetrying()) {
        for (Ask<SchemaCandidate> schema : schemas) {
          if (toRead.contains(schema.item().datasetUrn())) {
            retry[schema.owner()] = true;
          }
        }
        lifecycle.forEach(change -> retry[change.owner()] = true);
      }
      return decided;
    }

    for (Ask<SchemaCandidate> schema : schemas) {
      String urn = schema.item().datasetUrn();
      DatasetState state = states.get(urn);
      if (state != null) {
        remember(urn, !state.hasForeignSchema());
      }
      SchemaState known = schemaStates.get(urn);
      if (schemaPolicy == SchemaPolicy.ALWAYS || (known != null && known.writable())) {
        decided.get(schema.owner()).add(schema.item().schemaMetadata());
      }
    }
    // In event order, each change seeing the ones before it: a run that writes a file and then
    // moves it away lifts Hop's deprecation and then sets a new one, in that order.
    Map<String, DatasetState> current = new HashMap<>(states);
    for (Ask<LifecycleChange> ask : lifecycle) {
      LifecycleChange change = ask.item();
      DatasetState state = current.get(change.datasetUrn());
      Mcp deprecation = deprecation(change, state);
      if (deprecation != null) {
        decided.get(ask.owner()).add(deprecation);
        current.put(
            change.datasetUrn(),
            new DatasetState(
                state != null && state.hasForeignSchema(),
                deprecation.value().path("deprecated").asBoolean(),
                change.note()));
      }
    }
    return decided;
  }

  private Mcp deprecation(LifecycleChange change, DatasetState state) {
    boolean deprecated = state != null && state.deprecated();
    ObjectNode aspect = MAPPER.createObjectNode();
    if (change.kind() == LifecycleChange.Kind.DEPRECATE) {
      if (deprecated) {
        return null; // already retired, by Hop or by a person: keep the note that is there
      }
      aspect.put("deprecated", true);
      aspect.put("decommissionTime", change.timeMs());
      if (change.replacementUrn() != null) {
        aspect.put("replacement", change.replacementUrn());
      }
    } else {
      boolean byHop =
          state != null
              && state.note() != null
              && state.note().startsWith(DataHubTranslator.HOP_NOTE_PREFIX);
      if (!deprecated || !byHop) {
        return null; // not deprecated, or deprecated by a person: a write does not undo that
      }
      aspect.put("deprecated", false);
    }
    aspect.put("note", change.note());
    aspect.put("actor", actor);
    return Mcp.upsert("dataset", change.datasetUrn(), "deprecation", aspect);
  }

  private void remember(String datasetUrn, boolean writable) {
    schemaStates.put(datasetUrn, new SchemaState(writable, System.currentTimeMillis()));
  }

  /** What DataHub holds for the datasets. */
  private Map<String, DatasetState> readDatasets(Collection<String> urns)
      throws ReadException, InterruptedException {
    JsonNode data = read(DATASETS_QUERY, urns);
    Map<String, DatasetState> states = new HashMap<>();
    for (JsonNode entity : data.path("entities")) {
      String schemaName = entity.path("schemaMetadata").path("name").asText(null);
      boolean hasSchema =
          !entity.path("schemaMetadata").isMissingNode() && !entity.path("schemaMetadata").isNull();
      JsonNode deprecation = entity.path("deprecation");
      states.put(
          entity.path("urn").asText(),
          new DatasetState(
              hasSchema && !DataHubTranslator.HOP_SCHEMA_NAME.equals(schemaName),
              deprecation.path("deprecated").asBoolean(false),
              deprecation.path("note").asText(null)));
    }
    return states;
  }

  // ---------------------------------------------------------------------------------------------
  // Reading DataHub
  // ---------------------------------------------------------------------------------------------

  private JsonNode read(String query, Collection<String> urns)
      throws ReadException, InterruptedException {
    ObjectNode variables = MAPPER.createObjectNode();
    urns.forEach(variables.putArray("urns")::add);
    JsonNode data = client.graphql(query, variables);
    readFailureReported = false;
    return data;
  }

  /** Reported once until a read succeeds again. */
  private void reportReadFailure(ReadException e) {
    if (!readFailureReported) {
      readFailureReported = true;
      log.logError(
          "Unable to read from DataHub"
              + (e.worthRetrying()
                  ? "; what depends on it (dataset schemas, deprecations, retired and parent tasks,"
                      + " replayed events) is kept and tried again later: "
                  : ", so dataset schemas, deprecations, retired tasks and the parent tasks of"
                      + " first tasks are left alone; check that the token may read entities: ")
              + e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Written recently
  // ---------------------------------------------------------------------------------------------

  private boolean writtenRecently(Mcp mcp, long now) {
    Long at = written.get(key(mcp));
    return at != null && now - at < WRITTEN_TTL_MS;
  }

  private static String aspectKey(Mcp mcp) {
    return mcp.entityUrn() + '\u0000' + mcp.aspectName();
  }

  /** The proposal, its value as a digest: a changed value is never mistaken for the old one. */
  private static String key(Mcp mcp) {
    return aspectKey(mcp) + '\u0000' + mcp.changeType() + '\u0000' + sha256(mcp.value().toString());
  }

  private static String sha256(String text) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is part of every Java runtime", e);
    }
  }

  private static List<List<Mcp>> perEvent(int count) {
    List<List<Mcp>> lists = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      lists.add(new ArrayList<>());
    }
    return lists;
  }

  // ---------------------------------------------------------------------------------------------
  // Delivery
  // ---------------------------------------------------------------------------------------------

  /**
   * Writes the proposals in batches, in order, and says what became of each. A batch DataHub
   * refuses as a whole (one invalid proposal fails the request, as does a request too large) is
   * split in halves until the refused proposals are singled out, within {@link
   * #MAX_ISOLATION_REQUESTS}, so a bad aspect costs that aspect and not the run's lineage. Once
   * DataHub cannot be reached, the rest waits for a later attempt.
   */
  private Result[] send(List<Mcp> proposals) {
    Result[] outcomes = new Result[proposals.size()];
    try {
      for (int start = 0; start < proposals.size(); start += MAX_BATCH) {
        int[] budget = {MAX_ISOLATION_REQUESTS};
        int end = Math.min(proposals.size(), start + MAX_BATCH);
        if (!sendRange(proposals, start, end, outcomes, budget)) {
          break;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    for (int k = 0; k < outcomes.length; k++) {
      if (outcomes[k] == null) {
        outcomes[k] = Result.RETRY;
      }
    }
    return outcomes;
  }

  /** Sends {@code [from, to)}; {@code false} when delivery has to stop for now. */
  private boolean sendRange(List<Mcp> proposals, int from, int to, Result[] outcomes, int[] budget)
      throws InterruptedException {
    DataHubClient.Response response = ingestWithRetries(proposals.subList(from, to));
    if (response.ok()) {
      Arrays.fill(outcomes, from, to, Result.SENT);
      return true;
    }
    if (OpenLineageEmitter.failureOf(response.status()) == Result.RETRY) {
      logFailure(response, proposals.size() - from);
      return false;
    }
    if (to - from == 1) {
      Mcp mcp = proposals.get(from);
      outcomes[from] = Result.REJECTED;
      log.logError(
          "DataHub refused "
              + mcp.aspectName()
              + " of "
              + mcp.entityUrn()
              + " (HTTP "
              + response.status()
              + "): "
              + abbreviate(response.body()));
      return true;
    }
    if (budget[0] < 2) {
      Arrays.fill(outcomes, from, to, Result.REJECTED);
      log.logError(
          "DataHub refused "
              + (to - from)
              + " proposal(s) together (HTTP "
              + response.status()
              + ") and they were not singled out further: "
              + abbreviate(response.body()));
      return true;
    }
    budget[0] -= 2;
    int middle = (from + to) >>> 1;
    return sendRange(proposals, from, middle, outcomes, budget)
        && sendRange(proposals, middle, to, outcomes, budget);
  }

  private DataHubClient.Response ingestWithRetries(List<Mcp> batch) throws InterruptedException {
    DataHubClient.Response response = null;
    int attempts = Math.max(1, maxRetries + 1);
    for (int attempt = 0; attempt < attempts; attempt++) {
      response = client.ingest(batch);
      if (response.ok() || !OpenLineageEmitter.isTransient(response.status())) {
        return response;
      }
      if (attempt < attempts - 1) {
        Thread.sleep(backoff(attempt));
      }
    }
    return response;
  }

  /** {@code retryBackoffMs * 2^attempt}, at most {@link #MAX_BACKOFF_MS}. */
  long backoff(int attempt) {
    long delay = Math.max(0, retryBackoffMs);
    for (int i = 0; i < attempt && delay < MAX_BACKOFF_MS; i++) {
      delay *= 2;
    }
    return Math.min(delay, MAX_BACKOFF_MS);
  }

  private void logFailure(DataHubClient.Response response, int proposals) {
    log.logError(
        "Unable to write "
            + proposals
            + " proposal(s) to DataHub"
            + (response.status() > 0 ? " (HTTP " + response.status() + ")" : "")
            + "; their events are kept and sent again later: "
            + abbreviate(response.body()));
  }

  private static String abbreviate(String text) {
    if (text == null) {
      return "";
    }
    return text.length() > 500 ? text.substring(0, 500) + "..." : text;
  }

  @Override
  public void close() {
    // The JDK HTTP client holds no resources that outlive its last request.
  }
}
