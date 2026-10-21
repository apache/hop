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

package org.apache.hop.ai.engine;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.core.util.Utils;

/**
 * Decides what a question needs besides its context. The proposal format, the metadata type list
 * and the database plugin list take about 3,000 tokens, and serve only answers that propose a
 * change. A question that only asks for an explanation goes without them.
 *
 * <p>When in doubt a question counts as a change request: leaving the format out of one would stop
 * the model from proposing, while sending it to an explanation only costs tokens.
 */
public final class AiProposalIntent {

  /** Request attribute: whether the question was sent with the proposal format. */
  public static final String ATTR_PROPOSALS_OFFERED = "hop.ai.proposalsOffered";

  /** Scenarios that are about adding things, whatever the question says. */
  private static final List<String> CHANGE_SCENARIOS =
      List.of("pipeline-design", "transform-selection", "workflow-design", "action-selection");

  private static final int FLAGS =
      Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS;

  /** Asking for a change, in English, Dutch, German, French and Spanish. */
  private static final Pattern CHANGE =
      Pattern.compile(
          "\\b(add|adds|adding|create|build|make|connect|insert|remove|delete|rename|replace"
              + "|change|modify|fix|repair|configure|set|update|move|generate|propose"
              + "|apply|implement|split|merge|convert|refactor"
              + "|voeg|toevoegen|maak|bouw|verbind|verwijder|hernoem|vervang|wijzig|aanpassen"
              + "|pas\\s+\\w+\\s+aan|repareer|herstel|configureer"
              + "|füge|hinzufügen|erstelle|baue|verbinde|lösche|entferne|benenne|ersetze|ändere"
              + "|repariere|konfiguriere"
              + "|ajoute|ajouter|crée|créer|construis|connecte|supprime|renomme|remplace|modifie"
              + "|corrige|configure"
              + "|añade|agrega|crea|construye|conecta|elimina|borra|renombra|reemplaza|cambia"
              + "|modifica|corrige|configura)\\b",
          FLAGS);

  /** Asking for an explanation. */
  private static final Pattern EXPLAIN =
      Pattern.compile(
          "\\b(explain|describe|summari[sz]e|what|why|which|compare|difference|diagnose"
              + "|tell\\s+me|how\\s+does|how\\s+do\\s+\\w+\\s+work|walk\\s+me\\s+through"
              + "|leg\\s+uit|uitleg|beschrijf|wat|waarom|welke|vergelijk|verschil"
              + "|erkläre|erklär|beschreibe|was\\s+(?:macht|ist|tut|bedeutet)|warum|welche|vergleiche|unterschied"
              + "|explique|expliquer|décris|quoi|pourquoi|quel|quelle|compare"
              + "|explica|describe|qué|por\\s+qué|cuál|compara)\\b",
          FLAGS);

  /** A question about databases or connections, for the database plugin list. */
  private static final Pattern DATABASE =
      Pattern.compile(
          "\\b(database|databases|db|connection|connections|rdbms|jdbc|sql|table|schema"
              + "|postgres\\w*|mysql|mariadb|oracle|mssql|sql\\s*server|snowflake|redshift"
              + "|bigquery|duckdb|sqlite|db2|teradata|vertica|clickhouse|h2|hive|impala"
              + "|databank|verbinding|datenbank|verbindung|base\\s+de\\s+données|connexion"
              + "|base\\s+de\\s+datos|conexión)\\b",
          FLAGS);

  private AiProposalIntent() {}

  /**
   * Whether the question goes with the proposal format: not when it only asks for an explanation.
   * An empty question, as in the preview, gets everything.
   */
  public static boolean offersProposals(String scenarioId, String question) {
    if (Utils.isEmpty(question) || scenarioId != null && CHANGE_SCENARIOS.contains(scenarioId)) {
      return true;
    }
    if (CHANGE.matcher(question).find()) {
      return true;
    }
    return !EXPLAIN.matcher(question).find();
  }

  public static boolean offersProposals(AiAdvisorRequest request) {
    return request == null || offersProposals(request.getScenarioId(), request.getUserPrompt());
  }

  /**
   * Whether the database plugin list goes along: a proposal can save a database connection, and
   * needs a plugin id from that list, when the question is about databases or one is selected.
   */
  public static boolean needsDatabasePlugins(AiAdvisorRequest request) {
    if (request == null || Utils.isEmpty(request.getUserPrompt())) {
      return true;
    }
    if (DATABASE.matcher(request.getUserPrompt()).find()) {
      return true;
    }
    if (request.getMetadataSelections() != null) {
      for (AiAdvisorMetadataSelection selection : request.getMetadataSelections()) {
        if (selection != null && "rdbms".equals(selection.getTypeKey())) {
          return true;
        }
      }
    }
    return false;
  }

  /** Remember on the request whether it went with the proposal format. */
  public static void markOffered(AiAdvisorRequest request, boolean offered) {
    if (request != null && request.getAttributes() != null) {
      request.getAttributes().put(ATTR_PROPOSALS_OFFERED, offered);
    }
  }

  /** Whether the request went with the proposal format; true when it is not known. */
  public static boolean wasOffered(AiAdvisorRequest request) {
    Map<String, Object> attributes = request != null ? request.getAttributes() : null;
    Object value = attributes != null ? attributes.get(ATTR_PROPOSALS_OFFERED) : null;
    return !(value instanceof Boolean offered) || offered;
  }
}
