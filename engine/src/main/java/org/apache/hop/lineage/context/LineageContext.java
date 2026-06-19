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

package org.apache.hop.lineage.context;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/**
 * Immutable correlation context attached to every {@link
 * org.apache.hop.lineage.model.LineageEvent}.
 *
 * <p>Sinks use the context to identify which subject (pipeline, workflow, transform, or action)
 * produced an observation. Most fields are nullable; which ones are populated depends on the {@code
 * subjectType}:
 *
 * <ul>
 *   <li><b>PIPELINE</b> — {@code pipelineName} and {@code logChannelId} (the pipeline's log
 *       channel)
 *   <li><b>WORKFLOW</b> — {@code workflowName} and {@code logChannelId} (the workflow's log
 *       channel)
 *   <li><b>TRANSFORM</b> — {@code pipelineName}, {@code transformName}, {@code copyNr}, and {@code
 *       logChannelId} (the transform's log channel)
 *   <li><b>ACTION</b> — {@code workflowName}, {@code actionName}, and {@code logChannelId} (varies
 *       by lifecycle phase between the workflow's and the action instance's log channel)
 *   <li><b>OTHER</b> — sentinel for events that do not map to a single subject
 * </ul>
 *
 * <p>Build instances via {@link #builder()}. Correlation data not covered by the named fields goes
 * into the {@code attributes} map.
 */
@Getter
public final class LineageContext {

  /** Kind of subject this event describes. Never null. */
  private final LineageSubjectType subjectType;

  /**
   * Hop log channel id of the running subject — the same id Hop's logging system uses for the
   * pipeline / workflow / transform / action instance. May be null when no log channel is known
   * (e.g. test or synthetic events).
   */
  private final String logChannelId;

  /**
   * Pipeline name when the subject is a pipeline or one of its transforms; otherwise null.
   *
   * <p>This is the meta name, not the filename — see {@code hopFilename} for that.
   */
  private final String pipelineName;

  /** Workflow name when the subject is a workflow or one of its actions; otherwise null. */
  private final String workflowName;

  /** Transform name when the subject is a transform; otherwise null. */
  private final String transformName;

  /** Action name when the subject is a workflow action; otherwise null. */
  private final String actionName;

  /**
   * Transform copy number as a string ({@code "0"} for single-copy transforms) when the subject is
   * a transform; otherwise null. Stored as a string because not every pipeline engine exposes a
   * numeric copy value at runtime.
   */
  private final String copyNr;

  /**
   * Filename of the pipeline or workflow definition as it appears on the engine or meta; may be
   * null for in-memory artifacts.
   */
  private final String hopFilename;

  /**
   * Project-relative form of {@code hopFilename}, e.g. {@code ${PROJECT_HOME}/path/job.hwf}.
   * Suitable for correlating the same artifact across environments where the absolute path differs.
   *
   * <p>Null when {@code PROJECT_HOME} is unset, the filename is not under the project home, or the
   * value cannot be resolved (see {@link LineagePortableFilename}).
   */
  private final String hopFilenamePortableKey;

  /**
   * Extra correlation data not covered by the named fields, in insertion order. Sinks should not
   * rely on any particular key being present — producers may add or remove attributes over time.
   *
   * <p>Returned map is unmodifiable and never null.
   */
  private final Map<String, String> attributes;

  private LineageContext(Builder b) {
    this.subjectType = Objects.requireNonNull(b.subjectType, "subjectType");
    this.logChannelId = b.logChannelId;
    this.pipelineName = b.pipelineName;
    this.workflowName = b.workflowName;
    this.transformName = b.transformName;
    this.actionName = b.actionName;
    this.copyNr = b.copyNr;
    this.hopFilename = b.hopFilename;
    this.hopFilenamePortableKey = b.hopFilenamePortableKey;
    this.attributes =
        b.attributes.isEmpty()
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(b.attributes));
  }

  /** Creates a new builder with subject type defaulting to {@link LineageSubjectType#OTHER}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns an empty context with subject type {@link LineageSubjectType#OTHER} and no fields set.
   * Useful as a placeholder when an event has no meaningful subject (mostly tests).
   */
  public static LineageContext empty() {
    return builder().subjectType(LineageSubjectType.OTHER).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LineageContext that)) {
      return false;
    }
    return subjectType == that.subjectType
        && Objects.equals(logChannelId, that.logChannelId)
        && Objects.equals(pipelineName, that.pipelineName)
        && Objects.equals(workflowName, that.workflowName)
        && Objects.equals(transformName, that.transformName)
        && Objects.equals(actionName, that.actionName)
        && Objects.equals(copyNr, that.copyNr)
        && Objects.equals(hopFilename, that.hopFilename)
        && Objects.equals(hopFilenamePortableKey, that.hopFilenamePortableKey)
        && attributes.equals(that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        subjectType,
        logChannelId,
        pipelineName,
        workflowName,
        transformName,
        actionName,
        copyNr,
        hopFilename,
        hopFilenamePortableKey,
        attributes);
  }

  /**
   * Debug-friendly summary. Attribute values are omitted (only the count is shown) because they may
   * carry URIs or other identifiers that are noisy or sensitive in logs.
   */
  @Override
  public String toString() {
    return "LineageContext{subjectType="
        + subjectType
        + ", logChannelId="
        + logChannelId
        + ", pipelineName="
        + pipelineName
        + ", workflowName="
        + workflowName
        + ", transformName="
        + transformName
        + ", actionName="
        + actionName
        + ", copyNr="
        + copyNr
        + ", hopFilename="
        + hopFilename
        + ", attributeCount="
        + attributes.size()
        + '}';
  }

  /** Mutable builder for {@link LineageContext}. Not thread-safe; build one per event. */
  public static final class Builder {
    private LineageSubjectType subjectType = LineageSubjectType.OTHER;
    private String logChannelId;
    private String pipelineName;
    private String workflowName;
    private String transformName;
    private String actionName;
    private String copyNr;
    private String hopFilename;
    private String hopFilenamePortableKey;
    private final Map<String, String> attributes = new LinkedHashMap<>();

    /**
     * Sets the subject type. Must be non-null; fails fast with {@link NullPointerException} rather
     * than deferring the failure to {@link #build()}.
     */
    public Builder subjectType(LineageSubjectType subjectType) {
      this.subjectType = Objects.requireNonNull(subjectType, "subjectType");
      return this;
    }

    /** Sets the subject's log channel id; null clears any previously set value. */
    public Builder logChannelId(String logChannelId) {
      this.logChannelId = logChannelId;
      return this;
    }

    /** Sets the pipeline name; null clears any previously set value. */
    public Builder pipelineName(String pipelineName) {
      this.pipelineName = pipelineName;
      return this;
    }

    /** Sets the workflow name; null clears any previously set value. */
    public Builder workflowName(String workflowName) {
      this.workflowName = workflowName;
      return this;
    }

    /** Sets the transform name; null clears any previously set value. */
    public Builder transformName(String transformName) {
      this.transformName = transformName;
      return this;
    }

    /** Sets the action name; null clears any previously set value. */
    public Builder actionName(String actionName) {
      this.actionName = actionName;
      return this;
    }

    /** Sets the transform copy number; null clears any previously set value. */
    public Builder copyNr(String copyNr) {
      this.copyNr = copyNr;
      return this;
    }

    /** Sets the Hop filename; null clears any previously set value. */
    public Builder hopFilename(String hopFilename) {
      this.hopFilename = hopFilename;
      return this;
    }

    /** Sets the portable form of the Hop filename; null clears any previously set value. */
    public Builder hopFilenamePortableKey(String hopFilenamePortableKey) {
      this.hopFilenamePortableKey = hopFilenamePortableKey;
      return this;
    }

    /**
     * Adds a correlation attribute. If either {@code key} or {@code value} is null the call is
     * silently ignored, so callers can pass optional values without null-checking them.
     */
    public Builder putAttribute(String key, String value) {
      if (key != null && value != null) {
        attributes.put(key, value);
      }
      return this;
    }

    /** Builds the immutable {@link LineageContext}. Subject type must have been set to non-null. */
    public LineageContext build() {
      return new LineageContext(this);
    }
  }
}
