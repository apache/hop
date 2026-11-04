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

package org.apache.hop.metadata.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.memory.SharedMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

/** Labels for the providers of a metadata stack. */
public final class MetadataPlaces {

  private MetadataPlaces() {}

  public static MetadataPlace sharedPlace(SharedMetadataProvider shared) {
    String folder = shared.getConfiguredFolder();
    return new MetadataPlace(
        shared.getDescription(),
        "Shared metadata : " + folder,
        "Loaded with the Hop configuration, before any project is opened. A project object with"
            + " the same name overrides this one. Path: "
            + shared.getFolder(),
        false);
  }

  /** Labels taken from the provider descriptions. The last provider is the active place. */
  public static void applyDefaults(MultiMetadataProvider multi) {
    List<IHopMetadataProvider> providers = HopMetadataUtil.getProviders(multi);
    List<MetadataPlace> places = new ArrayList<>();
    for (int i = 0; i < providers.size(); i++) {
      places.add(generic(providers.get(i), i == providers.size() - 1));
    }
    multi.setPlaces(places);
  }

  /**
   * Keep friendly labels from a previous stack when the folder paths are unchanged, and rebuild the
   * shared place from the provider that is current now.
   */
  public static void reuseLabels(MultiMetadataProvider next, MultiMetadataProvider previous) {
    applyDefaults(next);
    if (previous == null || previous.getPlaces() == null || previous.getPlaces().isEmpty()) {
      return;
    }
    List<IHopMetadataProvider> providers = HopMetadataUtil.getProviders(next);
    List<MetadataPlace> merged = new ArrayList<>();
    for (int i = 0; i < providers.size(); i++) {
      IHopMetadataProvider provider = providers.get(i);
      boolean last = i == providers.size() - 1;
      if (provider instanceof SharedMetadataProvider shared) {
        merged.add(sharedPlace(shared));
        continue;
      }
      MetadataPlace earlier = find(previous.getPlaces(), provider.getDescription());
      if (earlier == null) {
        merged.add(generic(provider, last));
      } else {
        merged.add(
            new MetadataPlace(
                earlier.providerDescription(), earlier.label(), earlier.tooltip(), last));
      }
    }
    next.setPlaces(merged);
  }

  /** Add the shared place in front of labels that were already assigned. */
  public static void prependShared(MultiMetadataProvider multi, SharedMetadataProvider shared) {
    List<MetadataPlace> places = multi.getPlaces();
    if (places == null || places.isEmpty()) {
      applyDefaults(multi);
      return;
    }
    if (shared.getDescription().equals(places.get(0).providerDescription())) {
      return;
    }
    List<MetadataPlace> updated = new ArrayList<>();
    updated.add(sharedPlace(shared));
    updated.addAll(places);
    multi.setPlaces(updated);
  }

  public static List<MetadataPlace> of(MultiMetadataProvider multi) {
    if (multi == null) {
      return List.of();
    }
    if (multi.getPlaces() == null || multi.getPlaces().isEmpty()) {
      applyDefaults(multi);
    }
    return multi.getPlaces();
  }

  public static String labelOf(List<MetadataPlace> places, String providerDescription) {
    MetadataPlace place = find(places, providerDescription);
    if (place == null || place.label() == null) {
      return providerDescription;
    }
    return place.label();
  }

  public static MetadataPlace find(List<MetadataPlace> places, String providerDescription) {
    if (places == null || providerDescription == null) {
      return null;
    }
    for (MetadataPlace place : places) {
      if (providerDescription.equals(place.providerDescription())) {
        return place;
      }
    }
    return null;
  }

  public static MetadataPlace generic(IHopMetadataProvider provider, boolean active) {
    if (provider instanceof SharedMetadataProvider shared) {
      MetadataPlace place = sharedPlace(shared);
      if (!active) {
        return place;
      }
      return new MetadataPlace(place.providerDescription(), place.label(), place.tooltip(), true);
    }
    String folder =
        provider instanceof JsonMetadataProvider json
            ? json.getBaseFolder()
            : provider.getDescription();
    return new MetadataPlace(
        provider.getDescription(),
        "Metadata : " + folder,
        "Metadata stored in " + folder + ".",
        active);
  }
}
