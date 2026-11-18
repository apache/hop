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

package org.apache.hop.lineage.hub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.metadata.FakeLineageSink;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.spi.ILineageSink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The sinks runs select in their run configurations, next to the ones set with variables. */
class LineageHubRoutingTest {

  private static final LineageConfiguration LINEAGE_OFF =
      LineageConfiguration.forTesting(false, 100, 10, 50L, Set.of());
  private static final LineageConfiguration LINEAGE_ON =
      LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());

  /** The sinks the factory made, by URL. */
  private final Map<String, FakeLineageSink> started = new ConcurrentHashMap<>();

  private final AtomicInteger created = new AtomicInteger();

  private final AtomicInteger flakyFailures = new AtomicInteger(1);

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  private LineageHub hub(LineageConfiguration configuration, List<ILineageSink> sinks) {
    return LineageHub.newIsolatedForTesting(
        configuration,
        sinks,
        target -> {
          created.incrementAndGet();
          if ("broken".equals(target.name())) {
            throw new HopException("simulated: the plugin is not installed");
          }
          if ("flaky".equals(target.name()) && flakyFailures.getAndDecrement() > 0) {
            throw new HopException("simulated: the trust store is missing");
          }
          FakeLineageSink sink =
              new FakeLineageSink() {
                @Override
                public void init(IVariables variables, ILogChannel log) {
                  super.init(variables, log);
                  started.put(getUrl(), this);
                }
              };
          return sink;
        });
  }

  private static LineageSinkTarget target(String name, String url) {
    return new LineageSinkTarget(name, "fake-sink", Map.of("FAKE_URL", url));
  }

  private static LineageEvent event(String pipeline, String... sinkKeys) {
    return LineageEvent.of(
        LineageEventKind.RUN_LIFECYCLE,
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName(pipeline)
            .sinks(Set.of(sinkKeys))
            .build(),
        null);
  }

  private static List<String> pipelines(FakeLineageSink sink) {
    List<String> names = new ArrayList<>();
    for (LineageEvent event : sink.getEvents()) {
      names.add(event.getContext().getPipelineName());
    }
    return names;
  }

  @Test
  void aRunsSinkGetsItsEventsWhileLineageIsOff() {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    assertFalse(hub.isEnabled(), "only the runs that selected the sink observe lineage");
    hub.emit(event("routed", marquez));
    hub.emit(event("not routed"));
    hub.flush();
    hub.shutdown();

    FakeLineageSink sink = started.get("http://marquez:5000");
    assertEquals(List.of("routed"), pipelines(sink), "the sink got its settings and its event");
    assertTrue(sink.isShutdown());
  }

  @Test
  void theSinksSetWithVariablesStillGetEveryEvent() {
    List<String> global = new CopyOnWriteArrayList<>();
    ILineageSink variablesSink =
        batch -> batch.forEach(e -> global.add(e.getContext().getPipelineName()));
    LineageHub hub = hub(LINEAGE_ON, List.of(variablesSink));

    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    hub.emit(event("routed", marquez));
    hub.emit(event("not routed"));
    hub.flush();
    hub.shutdown();

    assertEquals(List.of("routed", "not routed"), global);
    assertEquals(List.of("routed"), pipelines(started.get("http://marquez:5000")));
  }

  @Test
  void eachSinkGetsOnlyTheEventsOfItsRuns() {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String dev = hub.useSink(target("lineage", "http://dev:5000"));
    String prod = hub.useSink(target("lineage", "http://prod:5000"));
    hub.emit(event("dev run", dev));
    hub.emit(event("prod run", prod));
    hub.emit(event("both", dev, prod));
    hub.flush();
    hub.shutdown();

    assertEquals(List.of("dev run", "both"), pipelines(started.get("http://dev:5000")));
    assertEquals(List.of("prod run", "both"), pipelines(started.get("http://prod:5000")));
  }

  @Test
  void runsThatResolveASinkTheSameWayShareIt() {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String first = hub.useSink(target("marquez", "http://marquez:5000"));
    String second = hub.useSink(target("marquez", "http://marquez:5000"));
    hub.emit(event("first", first));
    hub.emit(event("second", second));
    hub.flush();
    hub.shutdown();

    assertEquals(first, second);
    assertEquals(1, created.get());
    assertEquals(List.of("first", "second"), pipelines(started.get("http://marquez:5000")));
  }

  @Test
  void aSinkThatFailsToStartIsTriedOnceAndLeavesTheOthersAlone() {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String broken = hub.useSink(target("broken", "http://nowhere"));
    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    hub.emit(event("one", broken, marquez));
    hub.flush();
    hub.emit(event("two", broken, marquez));
    hub.flush();
    hub.shutdown();

    assertEquals(List.of("one", "two"), pipelines(started.get("http://marquez:5000")));
    assertEquals(2, created.get(), "the broken sink is created once, the other one once");
  }

  @Test
  void aSinkStartsAgainAfterAShutdown() {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    hub.emit(event("before", marquez));
    hub.flush();
    hub.shutdown();
    FakeLineageSink first = started.get("http://marquez:5000");

    hub.emit(event("after", marquez));
    hub.flush();
    hub.shutdown();
    FakeLineageSink second = started.get("http://marquez:5000");

    assertTrue(first.isShutdown());
    assertEquals(List.of("before"), pipelines(first));
    assertEquals(List.of("after"), pipelines(second));
  }

  @Test
  void aSinkReplacedByANewerVersionIsShutDownOnceNoRunHoldsIt() throws Exception {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String first = hub.useSink(target("marquez", "http://old:5000"));
    String alsoFirst = hub.useSink(target("marquez", "http://old:5000"));
    hub.emit(event("before the edit", first));
    hub.flush();
    FakeLineageSink old = started.get("http://old:5000");

    String edited = hub.useSink(target("marquez", "http://new:5000"));
    hub.emit(event("after the edit", edited));
    hub.release(Set.of(first));
    hub.flush();
    assertFalse(old.isShutdown(), "a run still holds the old sink");

    hub.release(Set.of(alsoFirst));
    hub.flush();
    waitUntil(old::isShutdown);
    hub.emit(event("later", edited));
    hub.flush();
    hub.shutdown();

    assertEquals(first, alsoFirst);
    assertEquals(List.of("before the edit"), pipelines(old));
    assertEquals(List.of("after the edit", "later"), pipelines(started.get("http://new:5000")));
  }

  @Test
  void aSinkEditedBetweenTwoRunsIsShutDown() throws Exception {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String first = hub.useSink(target("marquez", "http://old:5000"));
    hub.emit(event("the run before the edit", first));
    hub.release(Set.of(first));
    hub.flush();
    FakeLineageSink old = started.get("http://old:5000");
    assertFalse(old.isShutdown(), "the latest version stays after its run");

    String edited = hub.useSink(target("marquez", "http://new:5000"));
    waitUntil(old::isShutdown);
    hub.emit(event("the run after the edit", edited));
    hub.flush();
    hub.shutdown();

    assertEquals(List.of("the run before the edit"), pipelines(old));
    assertEquals(List.of("the run after the edit"), pipelines(started.get("http://new:5000")));
    assertEquals(1, hub.sinkStatus().size(), "only the edited version is listed");
  }

  @Test
  void anInterruptedRunStillLetsGoOfItsSinkAfterItsEvents() throws Exception {
    LineageHub hub = hub(LINEAGE_OFF, List.of());
    String old = hub.useSink(target("marquez", "http://old:5000"));
    hub.emit(event("start", old));
    hub.flush();
    hub.useSink(target("marquez", "http://new:5000"));
    for (int i = 0; i < 200; i++) {
      hub.emit(event("event " + i, old));
    }

    Thread.currentThread().interrupt();
    hub.release(Set.of(old));
    assertTrue(Thread.interrupted(), "the interrupt is kept for the caller");
    hub.flush();
    FakeLineageSink sink = started.get("http://old:5000");
    waitUntil(sink::isShutdown);
    hub.shutdown();

    assertEquals(201, pipelines(sink).size(), "every event before the release reached it");
  }

  @Test
  void theLatestVersionOfASinkStaysWhenItsRunsEnd() {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    hub.emit(event("first run", marquez));
    hub.release(Set.of(marquez));
    hub.flush();
    hub.emit(event("an event after it ended", marquez));
    hub.flush();
    FakeLineageSink sink = started.get("http://marquez:5000");
    assertFalse(sink.isShutdown());
    hub.shutdown();

    assertEquals(List.of("first run", "an event after it ended"), pipelines(sink));
    assertEquals(1, created.get());
  }

  @Test
  void aSinkThatFailedToStartIsTriedAgainLater() {
    LineageHub hub = hub(LINEAGE_OFF, List.of());
    hub.retryFailedSinkMs = 0;

    String flaky = hub.useSink(target("flaky", "http://flaky:5000"));
    hub.emit(event("lost", flaky));
    hub.flush();
    assertTrue(hub.failure(flaky).contains("trust store"), hub.failure(flaky));

    hub.emit(event("delivered", flaky));
    hub.flush();
    hub.shutdown();

    assertEquals(List.of("delivered"), pipelines(started.get("http://flaky:5000")));
    assertNull(hub.failure(flaky));
  }

  @Test
  void theStatusListsEverySinkWithItsHealth() {
    ILineageSink variablesSink = batch -> {};
    LineageHub hub = hub(LINEAGE_ON, List.of(variablesSink));

    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    String broken = hub.useSink(target("broken", "http://nowhere"));
    hub.useSink(target("idle", "http://idle:5000"));
    hub.emit(event("one", marquez, broken));
    hub.emit(event("two", marquez));
    hub.flush();
    List<LineageSinkStatus> status = hub.sinkStatus();
    hub.shutdown();

    assertEquals(4, status.size(), status.toString());
    LineageSinkStatus global = status.get(0);
    assertFalse(global.fromRunConfiguration());
    assertTrue(global.started());
    LineageSinkStatus running = find(status, "marquez");
    assertTrue(running.fromRunConfiguration());
    assertTrue(running.started());
    assertEquals(2, running.health().sent());
    LineageSinkStatus failed = find(status, "broken");
    assertFalse(failed.started());
    assertTrue(failed.startError().contains("not installed"), failed.startError());
    LineageSinkStatus idle = find(status, "idle");
    assertFalse(idle.started());
    assertNull(idle.startError());
    assertNull(idle.health());
  }

  @Test
  void aReInitShutsDownTheSinksRunsStarted() throws Exception {
    LineageHub hub = hub(LINEAGE_OFF, List.of());

    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    hub.emit(event("before", marquez));
    hub.flush();
    FakeLineageSink first = started.get("http://marquez:5000");

    hub.environmentReady();
    waitUntil(first::isShutdown);
    hub.emit(event("after", marquez));
    hub.flush();
    FakeLineageSink second = started.get("http://marquez:5000");
    hub.shutdown();

    assertNotSame(first, second);
    assertEquals(List.of("before"), pipelines(first));
    assertEquals(List.of("after"), pipelines(second));
  }

  @Test
  void aSinkThatFailsToInitShutsDownTheOnesThatStartedAndIsNotTriedAgain() {
    FakeLineageSink fine = new FakeLineageSink();
    AtomicInteger inits = new AtomicInteger();
    FakeLineageSink failing =
        new FakeLineageSink() {
          @Override
          public void init(IVariables variables, ILogChannel log) {
            inits.incrementAndGet();
            throw new IllegalStateException("simulated: the spool folder is read-only");
          }
        };
    LineageHub hub = hub(LINEAGE_ON, List.of(fine, failing));

    hub.emit(event("before"));
    hub.flush();
    assertTrue(fine.isShutdown(), "the sink that started is shut down");

    // A run's own sink starts a new worker, which doesn't start the failing sinks again
    String marquez = hub.useSink(target("marquez", "http://marquez:5000"));
    hub.emit(event("routed", marquez));
    hub.flush();
    hub.shutdown();

    assertEquals(1, inits.get());
    assertEquals(List.of("routed"), pipelines(started.get("http://marquez:5000")));
  }

  private static LineageSinkStatus find(List<LineageSinkStatus> status, String name) {
    return status.stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
  }

  private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 5_000;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(condition.getAsBoolean(), "timed out");
  }
}
