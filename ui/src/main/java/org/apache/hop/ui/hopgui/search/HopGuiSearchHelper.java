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

package org.apache.hop.ui.hopgui.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchQuery;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.core.search.SearchResult;
import org.apache.hop.core.search.SearchableAnalyserPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.workflow.WorkflowMeta;

/**
 * Shared logic to run a search against a {@link ISearchablesLocation} using the registered {@link
 * ISearchableAnalyser} plugins. Used by both the dedicated search perspective and the global search
 * popup so they behave identically.
 */
public final class HopGuiSearchHelper {

  private HopGuiSearchHelper() {
    // Utility class
  }

  /**
   * Load the registered searchable analysers, mapped by the class of object they can analyse.
   *
   * @return a map of searchable object class to its analyser
   * @throws HopException in case an analyser plugin can't be loaded
   */
  public static Map<Class<ISearchableAnalyser>, ISearchableAnalyser> loadSearchableAnalysers()
      throws HopException {
    Map<Class<ISearchableAnalyser>, ISearchableAnalyser> analyserMap = new HashMap<>();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin analyserPlugin : registry.getPlugins(SearchableAnalyserPluginType.class)) {
      ISearchableAnalyser searchableAnalyser =
          (ISearchableAnalyser) registry.loadClass(analyserPlugin);
      analyserMap.put(searchableAnalyser.getSearchableClass(), searchableAnalyser);
    }
    return analyserMap;
  }

  /**
   * Search a single location and collect all the matching results.
   *
   * @param location the location to enumerate searchables from
   * @param query the search query
   * @param analyserMap the analysers to use, see {@link #loadSearchableAnalysers()}
   * @param metadataProvider the metadata provider
   * @param variables the variables
   * @return the list of search results, never null
   * @throws HopException in case something goes wrong while searching
   */
  public static List<ISearchResult> searchLocation(
      ISearchablesLocation location,
      ISearchQuery query,
      Map<Class<ISearchableAnalyser>, ISearchableAnalyser> analyserMap,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    return analyse(enumerateSearchables(location, metadataProvider, variables), query, analyserMap);
  }

  /**
   * Enumerate (load) all the searchable objects of a location. This is the potentially expensive
   * part (it may load pipelines/workflows and metadata from disk), so callers that search
   * repeatedly - such as the live search popup - should enumerate once and reuse the result with
   * {@link #analyse(java.util.Collection, ISearchQuery, Map)}.
   *
   * @param location the location to enumerate
   * @param metadataProvider the metadata provider
   * @param variables the variables
   * @return the list of searchables
   * @throws HopException in case the location can't be enumerated
   */
  public static List<ISearchable> enumerateSearchables(
      ISearchablesLocation location, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    List<ISearchable> searchables = new ArrayList<>();
    Iterator<ISearchable> iterator = location.getSearchables(metadataProvider, variables);
    while (iterator.hasNext()) {
      searchables.add(iterator.next());
    }
    return searchables;
  }

  /**
   * Run the (cheap, in-memory) analyser matching over a set of already-enumerated searchables.
   *
   * @param searchables the searchables to match against, see {@link #enumerateSearchables}
   * @param query the search query
   * @param analyserMap the analysers to use, see {@link #loadSearchableAnalysers()}
   * @return the list of matching results, never null
   */
  public static List<ISearchResult> analyse(
      Collection<ISearchable> searchables,
      ISearchQuery query,
      Map<Class<ISearchableAnalyser>, ISearchableAnalyser> analyserMap) {
    List<ISearchResult> results = new ArrayList<>();
    for (ISearchable searchable : searchables) {
      Object object = searchable.getSearchableObject();
      if (object != null) {
        ISearchableAnalyser searchableAnalyser = analyserMap.get(object.getClass());
        if (searchableAnalyser != null) {
          results.addAll(searchableAnalyser.search(searchable, query));
        }
      }
    }
    return results;
  }

  /**
   * Like {@link #analyse} but additionally (when {@code fuzzyNames} is on and not a regex search)
   * matches object <em>names</em> fuzzily - so a typo'd name still turns up - and sorts every
   * result by relevance: name matches first (ranked by how well the name matches), then content
   * matches.
   *
   * @param searchables the searchables to match against
   * @param query the search query
   * @param analyserMap the analysers to use
   * @param fuzzyNames whether to add fuzzy name matches and rank results
   * @return the ranked list of results, never null
   */
  public static List<ISearchResult> analyseRanked(
      Collection<ISearchable> searchables,
      ISearchQuery query,
      Map<Class<ISearchableAnalyser>, ISearchableAnalyser> analyserMap,
      boolean fuzzyNames) {
    List<ISearchResult> results = new ArrayList<>(analyse(searchables, query, analyserMap));

    SearchMatcher nameMatcher =
        new SearchMatcher(query.getSearchString(), query.isCaseSensitive(), query.isRegEx(), true);

    if (fuzzyNames && !query.isRegEx()) {
      Set<String> covered = new HashSet<>();
      for (ISearchResult result : results) {
        covered.add(searchableKey(result.getMatchingSearchable()));
      }
      for (ISearchable searchable : searchables) {
        String name = searchable.getName();
        if (name != null
            && covered.add(searchableKey(searchable))
            && nameMatcher.score(name) > 0.0) {
          // A name that matches (often only fuzzily) without any other hit on the object.
          results.add(new SearchResult(searchable, name, "name", null, name));
        }
      }
    }

    Map<ISearchResult, Double> relevance = new IdentityHashMap<>();
    for (ISearchResult result : results) {
      relevance.put(result, relevance(result, nameMatcher));
    }
    results.sort((a, b) -> Double.compare(relevance.get(b), relevance.get(a)));
    return results;
  }

  /**
   * Relevance score for ranking: name matches (ranked by name similarity) outrank content matches.
   */
  private static double relevance(ISearchResult result, SearchMatcher nameMatcher) {
    ISearchable searchable = result.getMatchingSearchable();
    String name = Const.NVL(searchable.getName(), "");
    boolean isNameMatch = name.equalsIgnoreCase(Const.NVL(result.getMatchingString(), ""));
    if (isNameMatch) {
      return 0.5 + 0.5 * Math.max(0.0, nameMatcher.score(name));
    }
    return 0.3;
  }

  /** A stable identity for a searchable (type + name + normalized path) used for de-duplication. */
  public static String searchableKey(ISearchable searchable) {
    return Const.NVL(searchable.getType(), "")
        + "|"
        + Const.NVL(searchable.getName(), "")
        + "|"
        + stripScheme(Const.NVL(searchable.getFilename(), "").replace('\\', '/'));
  }

  /** Audit type under which the shared search-string history is stored (popup + results panel). */
  public static final String AUDIT_TYPE_SEARCH_STRING = "search-string";

  // --- Cross-location enumeration + grouping (shared by the popup and the perspective) -----------

  /** Internal section key for objects that are currently open in a Hop GUI tab. */
  public static final String SECTION_OPEN = "open";

  /** Internal section key for everything else (project files, metadata, variables, ...). */
  public static final String SECTION_PROJECT = "project";

  /**
   * Enumerate the searchables of <em>all</em> the given locations once, de-duplicating objects that
   * several locations report. The first occurrence wins; since the GUI location (open files) is
   * passed first, an open file is kept over the same file found on disk. For every kept searchable
   * the index of the location it came from is recorded (0 = the first/GUI location), which lets
   * {@link #isOpenObject} tell open files apart from project files.
   *
   * @param locations the locations to enumerate, GUI location first
   * @param metadataProvider the metadata provider
   * @param variables the variables
   * @param log the log channel for per-location errors (may be null)
   * @return the de-duplicated searchables and their source-location indexes
   */
  public static EnumeratedSearchables enumerateAll(
      List<ISearchablesLocation> locations,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ILogChannel log) {
    List<ISearchable> searchables = new ArrayList<>();
    Map<String, Integer> sourceByKey = new HashMap<>();
    Set<String> seen = new HashSet<>();
    for (int locationIndex = 0; locationIndex < locations.size(); locationIndex++) {
      ISearchablesLocation location = locations.get(locationIndex);
      try {
        for (ISearchable searchable : enumerateSearchables(location, metadataProvider, variables)) {
          String key = searchableKey(searchable);
          if (seen.add(key)) {
            searchables.add(searchable);
            sourceByKey.put(key, locationIndex);
          }
        }
      } catch (Exception e) {
        if (log != null) {
          log.logError(
              "Error loading searchables from location " + location.getLocationDescription(), e);
        }
      }
    }
    return new EnumeratedSearchables(searchables, sourceByKey);
  }

  /**
   * Whether a searchable is an object that is currently <em>open</em> in a Hop GUI tab. Only files
   * (pipelines and workflows) can be open; metadata items and variables are always project-wide.
   *
   * @param searchable the searchable to classify
   * @param sourceByKey the source-location map from {@link #enumerateAll}
   * @return true if this is an open pipeline/workflow tab
   */
  public static boolean isOpenObject(ISearchable searchable, Map<String, Integer> sourceByKey) {
    Object object = searchable.getSearchableObject();
    if (object instanceof PipelineMeta || object instanceof WorkflowMeta) {
      Integer source = sourceByKey.get(searchableKey(searchable));
      return source != null && source == 0;
    }
    return false;
  }

  /** Whether a result matched the object's own <em>name</em> (rather than some inner field). */
  public static boolean isNameMatch(ISearchResult result) {
    String name = Const.NVL(result.getMatchingSearchable().getName(), "");
    return !name.isEmpty() && name.equalsIgnoreCase(Const.NVL(result.getMatchingString(), ""));
  }

  /**
   * Turn a flat (ranked) list of results into the two-tier tree both search windows render: section
   * ({@link #SECTION_OPEN} / {@link #SECTION_PROJECT}) &#8594; type (Pipeline, Workflow, metadata
   * type, ...) &#8594; object &#8594; the individual field/transform matches inside it. A match on
   * the object's own name does not become a child row; it just marks the object node.
   *
   * @param ranked the ranked results, see {@link #analyseRanked}
   * @param sourceByKey the source-location map from {@link #enumerateAll}
   * @return the ordered sections, never null
   */
  public static List<SearchSection> groupResults(
      List<ISearchResult> ranked, Map<String, Integer> sourceByKey) {
    // Bucket results by object, keeping the (rank) order in which objects first appear.
    Map<String, ObjectAccumulator> byObject = new LinkedHashMap<>();
    for (ISearchResult result : ranked) {
      String key = searchableKey(result.getMatchingSearchable());
      ObjectAccumulator accumulator =
          byObject.computeIfAbsent(key, k -> new ObjectAccumulator(result.getMatchingSearchable()));
      if (isNameMatch(result)) {
        if (accumulator.nameMatch == null) {
          accumulator.nameMatch = result;
        }
      } else if (accumulator.seenMatchKeys.add(matchIdentity(result))) {
        // Collapse matches that point at the same component with the same matched text - e.g. a
        // transform whose own name AND its plugin name both equal the query produce two otherwise
        // identical-looking rows.
        accumulator.matches.add(result);
      }
    }

    // section -> type -> objects, preserving rank order of objects within a type.
    Map<String, Map<String, List<SearchObjectGroup>>> sections = new LinkedHashMap<>();
    sections.put(SECTION_OPEN, new LinkedHashMap<>());
    sections.put(SECTION_PROJECT, new LinkedHashMap<>());
    for (ObjectAccumulator accumulator : byObject.values()) {
      ISearchResult primary =
          accumulator.nameMatch != null
              ? accumulator.nameMatch
              : (accumulator.matches.isEmpty() ? null : accumulator.matches.get(0));
      if (primary == null) {
        continue;
      }
      String sectionKey =
          isOpenObject(accumulator.searchable, sourceByKey) ? SECTION_OPEN : SECTION_PROJECT;
      String type = Const.NVL(accumulator.searchable.getType(), "");
      sections
          .get(sectionKey)
          .computeIfAbsent(type, t -> new ArrayList<>())
          .add(new SearchObjectGroup(accumulator.searchable, primary, accumulator.matches));
    }

    List<SearchSection> result = new ArrayList<>();
    for (String sectionKey : new String[] {SECTION_OPEN, SECTION_PROJECT}) {
      Map<String, List<SearchObjectGroup>> typeMap = sections.get(sectionKey);
      if (typeMap.isEmpty()) {
        continue;
      }
      List<String> types = new ArrayList<>(typeMap.keySet());
      types.sort(
          Comparator.comparingInt(HopGuiSearchHelper::typeRank)
              .thenComparing(t -> t.toLowerCase(Locale.ROOT)));
      List<SearchTypeGroup> typeGroups = new ArrayList<>();
      for (String type : types) {
        typeGroups.add(new SearchTypeGroup(type, typeMap.get(type)));
      }
      result.add(new SearchSection(sectionKey, typeGroups));
    }
    return result;
  }

  /**
   * Sort order of type groups within a section: pipelines, workflows, then the rest, variables
   * last.
   */
  private static int typeRank(String type) {
    if (HopPipelineFileType.PIPELINE_FILE_TYPE_DESCRIPTION.equals(type)) {
      return 0;
    }
    if (HopWorkflowFileType.WORKFLOW_FILE_TYPE_DESCRIPTION.equals(type)) {
      return 1;
    }
    if ("Variable".equals(type)) {
      return 100;
    }
    return 50;
  }

  /** A match's user-visible identity within its object: where it is plus what matched. */
  private static String matchIdentity(ISearchResult result) {
    return Const.NVL(result.getComponent(), "") + " " + Const.NVL(result.getMatchingString(), "");
  }

  /** Mutable per-object bucket used only while {@link #groupResults grouping}. */
  private static final class ObjectAccumulator {
    private final ISearchable searchable;
    private ISearchResult nameMatch;
    private final List<ISearchResult> matches = new ArrayList<>();
    private final Set<String> seenMatchKeys = new HashSet<>();

    private ObjectAccumulator(ISearchable searchable) {
      this.searchable = searchable;
    }
  }

  /** The de-duplicated searchables of all locations plus where each came from. */
  public static final class EnumeratedSearchables {
    private final List<ISearchable> searchables;
    private final Map<String, Integer> sourceByKey;

    public EnumeratedSearchables(List<ISearchable> searchables, Map<String, Integer> sourceByKey) {
      this.searchables = searchables;
      this.sourceByKey = sourceByKey;
    }

    public List<ISearchable> getSearchables() {
      return searchables;
    }

    public Map<String, Integer> getSourceByKey() {
      return sourceByKey;
    }
  }

  /** A top-level section (Open / Project) of the grouped results. */
  public static final class SearchSection {
    private final String key;
    private final List<SearchTypeGroup> typeGroups;

    private SearchSection(String key, List<SearchTypeGroup> typeGroups) {
      this.key = key;
      this.typeGroups = typeGroups;
    }

    public String getKey() {
      return key;
    }

    public List<SearchTypeGroup> getTypeGroups() {
      return typeGroups;
    }

    /** The number of matching objects in this section (across all its type groups). */
    public int getObjectCount() {
      int count = 0;
      for (SearchTypeGroup typeGroup : typeGroups) {
        count += typeGroup.getObjects().size();
      }
      return count;
    }
  }

  /** A type group (Pipeline, Workflow, a metadata type, Variable, ...) within a section. */
  public static final class SearchTypeGroup {
    private final String type;
    private final List<SearchObjectGroup> objects;

    private SearchTypeGroup(String type, List<SearchObjectGroup> objects) {
      this.type = type;
      this.objects = objects;
    }

    public String getType() {
      return type;
    }

    public List<SearchObjectGroup> getObjects() {
      return objects;
    }
  }

  /** A single matching object and the individual field/transform matches found inside it. */
  public static final class SearchObjectGroup {
    private final ISearchable searchable;
    private final ISearchResult primary;
    private final List<ISearchResult> matches;

    private SearchObjectGroup(
        ISearchable searchable, ISearchResult primary, List<ISearchResult> matches) {
      this.searchable = searchable;
      this.primary = primary;
      this.matches = matches;
    }

    public ISearchable getSearchable() {
      return searchable;
    }

    /**
     * The representative result for the object node itself (its name match, else its first match).
     */
    public ISearchResult getPrimary() {
      return primary;
    }

    /** The inner matches shown as child rows (empty when only the object's name matched). */
    public List<ISearchResult> getMatches() {
      return matches;
    }
  }

  /**
   * A condensed, human-readable description of <em>where</em> a result matched: the component (e.g.
   * a transform name) and the field, for example "Detect Language &#8594; url" or "description".
   *
   * @param result the search result
   * @return the condensed location of the match, never null
   */
  public static String matchWhere(ISearchResult result) {
    String field = result.getDescription();
    if (field != null) {
      int idx = field.lastIndexOf(" : ");
      if (idx >= 0) {
        field = field.substring(idx + 3);
      }
    }
    field = Const.NVL(field, "");
    String component = result.getComponent();
    if (component != null && !component.isEmpty()) {
      return field.isEmpty() ? component : component + " → " + field;
    }
    return field;
  }

  /**
   * A short snippet of the matched text, windowed around the search term where possible, so the
   * user can see <em>what</em> matched without opening the object.
   *
   * @param result the search result
   * @param searchTerm the (plain, non-regex) search term, used to center the snippet; may be null
   * @return the condensed match snippet, never null
   */
  public static String matchSnippet(ISearchResult result, String searchTerm) {
    String value = Const.NVL(result.getValue(), result.getMatchingString());
    if (value == null) {
      return "";
    }
    value = value.replaceAll("\\s+", " ").trim();
    int window = 70;
    if (searchTerm != null && !searchTerm.isEmpty()) {
      int idx = value.toLowerCase(Locale.ROOT).indexOf(searchTerm.toLowerCase(Locale.ROOT));
      if (idx >= 0) {
        int start = Math.max(0, idx - 25);
        int end = Math.min(value.length(), idx + searchTerm.length() + 40);
        String snippet = value.substring(start, end);
        if (start > 0) {
          snippet = "…" + snippet;
        }
        if (end < value.length()) {
          snippet = snippet + "…";
        }
        return snippet;
      }
    }
    return value.length() > window ? value.substring(0, window) + "…" : value;
  }

  /**
   * Make a file path nicer to display: strip the {@code file://} scheme and, when the path is
   * inside the current project, replace the project home prefix with the literal {@code
   * ${PROJECT_HOME}}.
   *
   * @param rawPath the path to sanitize (may be a {@code file://} URI)
   * @param variables the variables used to resolve {@code ${PROJECT_HOME}}
   * @return the sanitized, project-relative path
   */
  public static String sanitizePath(String rawPath, IVariables variables) {
    if (rawPath == null || rawPath.isEmpty()) {
      return Const.NVL(rawPath, "");
    }
    String path = stripScheme(rawPath.replace('\\', '/'));
    if (variables != null) {
      String home = variables.resolve(Const.VAR_PROJECT_HOME);
      if (home != null && !home.isEmpty() && !Const.VAR_PROJECT_HOME.equals(home)) {
        home = stripScheme(home.replace('\\', '/'));
        if (path.startsWith(home)) {
          String rel = path.substring(home.length());
          return Const.VAR_PROJECT_HOME + (rel.startsWith("/") ? rel : "/" + rel);
        }
      }
    }
    return path;
  }

  private static String stripScheme(String path) {
    if (path.startsWith("file://")) {
      return path.substring("file://".length());
    }
    if (path.startsWith("file:")) {
      return path.substring("file:".length());
    }
    return path;
  }

  /**
   * The label to show for an object in the results tree: its name, prefixed with the folder path
   * relative to the project home when it is a project file. This disambiguates several
   * pipelines/workflows that share a name in different folders (e.g. {@code data/template} vs
   * {@code samples/template}). Non-file searchables (metadata, variables) and files outside the
   * project just show their name.
   *
   * @param searchable the searchable to label
   * @param variables the variables used to resolve {@code ${PROJECT_HOME}}
   * @return the display label
   */
  public static String displayLabel(ISearchable searchable, IVariables variables) {
    String name = Const.NVL(searchable.getName(), "");
    String filename = searchable.getFilename();
    if (filename == null || filename.isEmpty()) {
      return name;
    }
    String folder = relativeFolder(filename, variables);
    return folder.isEmpty() ? name : folder + name;
  }

  /** The folder of a file relative to the project home (with a trailing slash), or "" otherwise. */
  private static String relativeFolder(String rawPath, IVariables variables) {
    String sanitized = sanitizePath(rawPath, variables);
    if (!sanitized.startsWith(Const.VAR_PROJECT_HOME)) {
      // Outside the project (absolute path): keep just the name rather than a long prefix.
      return "";
    }
    String rel = sanitized.substring(Const.VAR_PROJECT_HOME.length());
    if (rel.startsWith("/")) {
      rel = rel.substring(1);
    }
    int lastSlash = rel.lastIndexOf('/');
    return lastSlash < 0 ? "" : rel.substring(0, lastSlash + 1);
  }
}
