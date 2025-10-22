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

package org.apache.hop.concurrency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.Test;

/**
 * Test to verify that WorkflowTracker enforces uniqueness per workflow instance when multiple
 * threads attempt to add duplicate trackers concurrently (as happens when actions run in parallel).
 * Each workflow (identified by name + filename) should only have one tracker.
 */
public class WorkflowTrackerUniquenessTest {

  private static WorkflowMeta mockWorkflowMeta(String name) {
    WorkflowMeta meta = mock(WorkflowMeta.class);
    when(meta.getName()).thenReturn(name);
    return meta;
  }

  /**
   * Test that when multiple threads try to add WorkflowTrackers for the same workflow, only one is
   * actually added (uniqueness is enforced at workflow level).
   */
  @Test
  public void testUniquenessWithSameWorkflow() throws Exception {
    WorkflowTracker tracker = new WorkflowTracker(mockWorkflowMeta("parent-workflow"));

    final int NUM_THREADS = 10;

    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(NUM_THREADS);

    // All threads will try to add a tracker for the same child workflow
    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadNum = i;
      executor.submit(
          () -> {
            try {
              startLatch.await(); // Wait for all threads to be ready

              ActionResult result =
                  new ActionResult(
                      null,
                      "log-channel-" + threadNum, // Different log channels
                      "Action started",
                      null,
                      "action-" + threadNum, // Different actions
                      null);

              // All threads create trackers for the SAME workflow (same name)
              WorkflowTracker childTracker =
                  new WorkflowTracker(mockWorkflowMeta("child-workflow"), result);
              tracker.addWorkflowTracker(childTracker);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown(); // Start all threads
    assertTrue("Threads did not complete in time", doneLatch.await(5, TimeUnit.SECONDS));
    executor.shutdown();

    // Verify only ONE tracker was added for the child workflow (not 10)
    assertEquals("Expected only 1 unique tracker per workflow", 1, tracker.nrWorkflowTrackers());
  }

  /**
   * Test that when multiple threads add WorkflowTrackers for DIFFERENT workflows, all are added
   * (uniqueness is per workflow).
   */
  @Test
  public void testUniquenessWithDifferentWorkflows() throws Exception {
    WorkflowTracker tracker = new WorkflowTracker(mockWorkflowMeta("parent-workflow"));

    final int NUM_THREADS = 10;

    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(NUM_THREADS);

    // Each thread adds a tracker for a DIFFERENT child workflow
    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadNum = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();

              ActionResult result =
                  new ActionResult(
                      null,
                      "log-channel-" + threadNum,
                      "Action started",
                      null,
                      "test-action-" + threadNum,
                      null);

              // Each thread creates a tracker for a DIFFERENT workflow
              WorkflowTracker childTracker =
                  new WorkflowTracker(mockWorkflowMeta("child-workflow-" + threadNum), result);
              tracker.addWorkflowTracker(childTracker);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue("Threads did not complete in time", doneLatch.await(5, TimeUnit.SECONDS));
    executor.shutdown();

    // Verify all 10 unique workflow trackers were added (one per workflow)
    assertEquals(
        "Expected all 10 unique workflows to be tracked",
        NUM_THREADS,
        tracker.nrWorkflowTrackers());

    // Verify all identifiers are unique
    Set<String> identifiers = new HashSet<>();
    for (int i = 0; i < tracker.nrWorkflowTrackers(); i++) {
      WorkflowTracker child = tracker.getWorkflowTracker(i);
      String identifier = child.getUniqueIdentifier();
      assertTrue("Identifier should be unique: " + identifier, identifiers.add(identifier));
    }
  }

  /**
   * Test that duplicate prevention works with workflow-level identifiers. Multiple actions for the
   * same workflow should only create one tracker.
   */
  @Test
  public void testUniquenessPerWorkflow() throws Exception {
    WorkflowTracker tracker = new WorkflowTracker(mockWorkflowMeta("parent-workflow"));

    final int NUM_THREADS = 5;

    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(NUM_THREADS);

    // Create multiple trackers for the same workflow but different actions
    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadNum = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();

              ActionResult result = new ActionResult();
              result.setActionName("action-" + threadNum);
              result.setComment("Action started");

              // All threads try to add trackers for the SAME child workflow
              WorkflowTracker childTracker =
                  new WorkflowTracker(mockWorkflowMeta("same-child-workflow"), result);
              tracker.addWorkflowTracker(childTracker);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue("Threads did not complete in time", doneLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();

    // Only one tracker should be added for the workflow (not one per action)
    assertEquals("Expected only 1 tracker for the workflow", 1, tracker.nrWorkflowTrackers());
  }

  /**
   * Test the getUniqueIdentifier method directly. It should use workflow name and filename only.
   */
  @Test
  public void testGetUniqueIdentifier() {
    WorkflowMeta meta = mockWorkflowMeta("test-workflow");

    // Test with action result
    ActionResult result1 = new ActionResult(null, "channel-123", null, null, "action-1", null);

    WorkflowTracker tracker1 = new WorkflowTracker(meta, result1);
    String identifier1 = tracker1.getUniqueIdentifier();

    // Should contain workflow name
    assertTrue("Identifier should contain workflow name", identifier1.contains("test-workflow"));

    // Test that different actions in same workflow have same identifier
    ActionResult result2 = new ActionResult(null, "channel-456", "Started", null, "action-2", null);

    WorkflowTracker tracker2 = new WorkflowTracker(meta, result2);
    String identifier2 = tracker2.getUniqueIdentifier();

    // Should be the same identifier (workflow-level, not action-level)
    assertEquals("Same workflow should have same identifier", identifier1, identifier2);
  }

  /**
   * Test equals and hashCode methods. Trackers for the same workflow should be equal, regardless of
   * action.
   */
  @Test
  public void testEqualsAndHashCode() {
    WorkflowMeta meta1 = mockWorkflowMeta("workflow-A");
    WorkflowMeta meta2 = mockWorkflowMeta("workflow-B");

    // Create two trackers for the same workflow but different actions
    ActionResult result1 = new ActionResult(null, "channel-1", null, null, "action-1", null);
    ActionResult result2 = new ActionResult(null, "channel-2", null, null, "action-2", null);

    WorkflowTracker tracker1 = new WorkflowTracker(meta1, result1);
    WorkflowTracker tracker2 = new WorkflowTracker(meta1, result2);

    // Should be equal because they're for the same workflow
    assertEquals("Trackers for same workflow should be equal", tracker1, tracker2);
    assertEquals(
        "Hash codes should match for equal objects", tracker1.hashCode(), tracker2.hashCode());

    // Create tracker for different workflow
    ActionResult result3 = new ActionResult(null, "channel-3", null, null, "action-1", null);
    WorkflowTracker tracker3 = new WorkflowTracker(meta2, result3);

    // Should not be equal (different workflow)
    assertTrue("Trackers for different workflows should not be equal", !tracker1.equals(tracker3));
  }
}
