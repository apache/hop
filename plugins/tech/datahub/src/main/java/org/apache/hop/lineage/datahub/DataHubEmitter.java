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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage.RunEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.logging.ILogChannel;
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
 * <p>The emitter runs on the dispatcher's worker thread, one event at a time, and is the same for a
 * live event and one replayed from the spool.
 */
class DataHubEmitter implements OpenLineageEmitter {

  /** Proposals per request; a large pipeline's terminal event can carry many datasets. */
  static final int MAX_BATCH = 200;

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

  private final DataHubTranslator translator;
  private final DataHubClient client;
  private final SchemaPolicy schemaPolicy;
  private final String actor;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final ILogChannel log;
  private final Map<String, SchemaState> schemaStates =
      new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, SchemaState> eldest) {
          return size() > MAX_SCHEMA_STATES;
        }
      };
  private final Map<String, Long> written =
      new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
          return size() > MAX_WRITTEN;
        }
      };
  private boolean readFailureReported;

  /** Whether Hop may write a dataset's schema, as last read from DataHub. */
  private record SchemaState(boolean writable, long readAtMs) {}

  /** What DataHub holds for a dataset, as far as the sink's decisions go. */
  private record DatasetState(boolean hasForeignSchema, boolean deprecated, String note) {}

  DataHubEmitter(
      DataHubTranslator translator,
      DataHubClient client,
      SchemaPolicy schemaPolicy,
      String actor,
      int maxRetries,
      long retryBackoffMs,
      ILogChannel log) {
    this.translator = translator;
    this.client = client;
    this.schemaPolicy = schemaPolicy;
    this.actor = actor;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.log = log;
  }

  @Override
  public Result emit(RunEvent event) {
    return emitAll(List.of(event)).get(0);
  }

  /**
   * Writes the events in one go: their proposals, with one read of DataHub for the decisions they
   * need, in as few requests as {@link #MAX_BATCH} allows. They succeed or wait together; an event
   * that cannot be translated is refused on its own.
   */
  @Override
  public List<Result> emitAll(List<RunEvent> events) {
    Result[] results = new Result[events.size()];
    List<Mcp> proposals = new ArrayList<>();
    List<SchemaCandidate> schemas = new ArrayList<>();
    List<LifecycleChange> lifecycle = new ArrayList<>();
    List<StaleCheck> staleChecks = new ArrayList<>();
    proposals.add(DataHubTranslator.hopPlatform());
    for (int i = 0; i < events.size(); i++) {
      try {
        Translation translation = translator.translate(events.get(i));
        proposals.addAll(translation.proposals());
        schemas.addAll(translation.schemas());
        lifecycle.addAll(translation.lifecycle());
        staleChecks.addAll(translation.staleChecks());
      } catch (RuntimeException e) {
        log.logError("Unable to translate a lineage event for DataHub; it is skipped", e);
        results[i] = Result.REJECTED;
      }
    }
    Result outcome;
    try {
      proposals.addAll(decide(schemas, lifecycle));
      List<Mcp> retirements = retire(staleChecks);
      proposals.addAll(retirements);
      long now = System.currentTimeMillis();
      proposals.removeIf(mcp -> writtenRecently(mcp, now));
      outcome = proposals.isEmpty() ? Result.SENT : send(proposals);
      if (outcome == Result.SENT) {
        for (Mcp mcp : retirements) {
          // What this process wrote for a retired task no longer holds: when the task comes back,
          // its next start has to write it again.
          String prefix = mcp.entityUrn() + '\u0000';
          written.keySet().removeIf(key -> key.startsWith(prefix));
        }
        for (Mcp mcp : proposals) {
          written.put(key(mcp), now);
        }
        for (SchemaCandidate schema : schemas) {
          if (proposals.contains(schema.schemaMetadata())) {
            remember(schema.datasetUrn(), true);
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      outcome = Result.RETRY;
    }
    for (int i = 0; i < results.length; i++) {
      if (results[i] == null) {
        results[i] = outcome;
      }
    }
    return Arrays.asList(results);
  }

  /**
   * Soft-deletes the tasks of each flow that its run no longer has: a transform or action removed
   * from the pipeline or workflow, or renamed. Only tasks written for the same Hop file are
   * candidates, so a pipeline of the same name in another project cannot retire this one's tasks.
   * DataHub keeps a soft-deleted task, and the task's next start restores it.
   */
  private List<Mcp> retire(List<StaleCheck> checks) throws InterruptedException {
    if (checks.isEmpty()) {
      return List.of();
    }
    Map<String, StaleCheck> byFlow = new LinkedHashMap<>();
    checks.forEach(check -> byFlow.put(check.flowUrn(), check));
    ObjectNode variables = MAPPER.createObjectNode();
    byFlow.keySet().forEach(variables.putArray("urns")::add);
    JsonNode data;
    try {
      data = client.graphql(FLOW_JOBS_QUERY, variables);
    } catch (IOException e) {
      reportReadFailure(e);
      return List.of();
    }
    List<Mcp> retirements = new ArrayList<>();
    for (JsonNode flow : data.path("entities")) {
      StaleCheck check = byFlow.get(flow.path("urn").asText());
      if (check == null) {
        continue;
      }
      for (JsonNode relationship : flow.path("jobs").path("relationships")) {
        JsonNode job = relationship.path("entity");
        String urn = job.path("urn").asText();
        // A soft-deleted task still "exists" for DataHub; its status says it was retired already.
        if (job.path("status").path("removed").asBoolean(false)
            || check.currentJobUrns().contains(urn)
            || !check.filename().equals(customProperty(job, DataHubTranslator.FILENAME_PROPERTY))) {
          continue;
        }
        ObjectNode status = MAPPER.createObjectNode();
        status.put("removed", true);
        retirements.add(Mcp.upsert("dataJob", urn, "status", status));
        log.logBasic(
            "Retiring DataHub task " + urn + ": its pipeline or workflow no longer has it");
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

  private void reportReadFailure(IOException e) {
    if (!readFailureReported) {
      readFailureReported = true;
      log.logError(
          "Unable to read from DataHub, so dataset schemas, deprecations and retired tasks are"
              + " left alone (reported once): "
              + e.getMessage());
    }
  }

  private boolean writtenRecently(Mcp mcp, long now) {
    Long at = written.get(key(mcp));
    return at != null && now - at < WRITTEN_TTL_MS;
  }

  private static String key(Mcp mcp) {
    return mcp.entityUrn()
        + '\u0000'
        + mcp.aspectName()
        + '\u0000'
        + mcp.changeType()
        + '\u0000'
        + Integer.toHexString(mcp.value().toString().hashCode());
  }

  // ---------------------------------------------------------------------------------------------
  // Decisions that depend on what DataHub holds
  // ---------------------------------------------------------------------------------------------

  /**
   * The schema and lifecycle proposals that DataHub's current state allows. A dataset another
   * source describes (DataHub's own database ingestion, say) keeps that source's schema; a
   * deprecation a person wrote is never lifted. When DataHub cannot be read, both are left out: the
   * lineage itself does not wait for them.
   */
  private List<Mcp> decide(List<SchemaCandidate> candidates, List<LifecycleChange> lifecycle)
      throws InterruptedException {
    List<SchemaCandidate> schemas = schemaPolicy == SchemaPolicy.NEVER ? List.of() : candidates;
    if (schemas.isEmpty() && lifecycle.isEmpty()) {
      return List.of();
    }
    Set<String> toRead = new LinkedHashSet<>();
    long now = System.currentTimeMillis();
    if (schemaPolicy == SchemaPolicy.MISSING) {
      for (SchemaCandidate schema : schemas) {
        SchemaState known = schemaStates.get(schema.datasetUrn());
        if (known == null || now - known.readAtMs() > SCHEMA_STATE_TTL_MS) {
          toRead.add(schema.datasetUrn());
        }
      }
    }
    for (LifecycleChange change : lifecycle) {
      toRead.add(change.datasetUrn());
    }
    Map<String, DatasetState> states = toRead.isEmpty() ? Map.of() : read(toRead);
    if (states == null) {
      return List.of();
    }

    List<Mcp> proposals = new ArrayList<>();
    for (SchemaCandidate schema : schemas) {
      DatasetState state = states.get(schema.datasetUrn());
      if (state != null) {
        remember(schema.datasetUrn(), !state.hasForeignSchema());
      }
      SchemaState known = schemaStates.get(schema.datasetUrn());
      if (schemaPolicy == SchemaPolicy.ALWAYS || (known != null && known.writable())) {
        proposals.add(schema.schemaMetadata());
      }
    }
    // In event order, each change seeing the ones before it: a run that writes a file and then
    // moves it away lifts Hop's deprecation and then sets a new one, in that order.
    Map<String, DatasetState> current = new HashMap<>(states);
    for (LifecycleChange change : lifecycle) {
      DatasetState state = current.get(change.datasetUrn());
      Mcp deprecation = deprecation(change, state);
      if (deprecation != null) {
        proposals.add(deprecation);
        current.put(
            change.datasetUrn(),
            new DatasetState(
                state != null && state.hasForeignSchema(),
                deprecation.value().path("deprecated").asBoolean(),
                change.note()));
      }
    }
    return proposals;
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

  /** What DataHub holds for the datasets, or {@code null} when it could not be read. */
  private Map<String, DatasetState> read(Collection<String> urns) throws InterruptedException {
    ObjectNode variables = MAPPER.createObjectNode();
    urns.forEach(variables.putArray("urns")::add);
    JsonNode data;
    try {
      data = client.graphql(DATASETS_QUERY, variables);
    } catch (IOException e) {
      reportReadFailure(e);
      return null;
    }
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
  // Delivery
  // ---------------------------------------------------------------------------------------------

  /**
   * Writes the proposals in batches. A batch DataHub refuses as a whole (one invalid proposal fails
   * the request) is retried one proposal at a time, so a single bad aspect costs that aspect and
   * not the run's lineage.
   */
  private Result send(List<Mcp> proposals) throws InterruptedException {
    int rejected = 0;
    for (int start = 0; start < proposals.size(); start += MAX_BATCH) {
      List<Mcp> batch = proposals.subList(start, Math.min(proposals.size(), start + MAX_BATCH));
      DataHubClient.Response response = ingestWithRetries(batch);
      if (response.ok()) {
        continue;
      }
      if (OpenLineageEmitter.failureOf(response.status()) == Result.RETRY) {
        logFailure(response, batch.size());
        return Result.RETRY;
      }
      for (Mcp mcp : batch) {
        DataHubClient.Response single = ingestWithRetries(List.of(mcp));
        if (single.ok()) {
          continue;
        }
        if (OpenLineageEmitter.failureOf(single.status()) == Result.RETRY) {
          logFailure(single, 1);
          return Result.RETRY;
        }
        rejected++;
        log.logError(
            "DataHub refused "
                + mcp.aspectName()
                + " of "
                + mcp.entityUrn()
                + " (HTTP "
                + single.status()
                + "): "
                + abbreviate(single.body()));
      }
    }
    return rejected == proposals.size() ? Result.REJECTED : Result.SENT;
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
        Thread.sleep(retryBackoffMs << Math.min(attempt, 16));
      }
    }
    return response;
  }

  private void logFailure(DataHubClient.Response response, int proposals) {
    log.logError(
        "Unable to write "
            + proposals
            + " proposal(s) to DataHub"
            + (response.status() > 0 ? " (HTTP " + response.status() + ")" : "")
            + "; the event is kept and sent again later: "
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
