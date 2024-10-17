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

package org.apache.hop.pipeline.transforms.stanford.nlp.simple;

import static java.util.Set.of;

import java.util.Collection;

// Part-of-Speech Stanford Tagset (Penn Treebank)
// Source: https://stanfordnlp.github.io/CoreNLP/tools_pos_tagger_faq.html
// https://www.cis.upenn.edu/~bies/manuals/tagguide.pdf
public enum PennTreebankPartOfSpeech {
  CC("Coordinating conjunction"),
  CD("Cardinal number"),
  DT("Determiner"),
  EX("Existential there"),
  FW("Foreign word"),
  IN("Preposition or subordinating conjunction"),
  JJ("Adjective"),
  JJR("Adjective, comparative"),
  JJS("Adjective, superlative"),
  LS("List item marker"),
  MD("Modal"),
  NN("Noun, singular or mass"),
  NNS("Noun, plural"),
  NNP("Proper noun, singular"),
  NNPS("Proper noun, plural"),
  PDT("Predeterminer"),
  POS("Possessive ending"),
  PRP("Personal pronoun"),
  PRP$("Possessive pronoun"),
  RB("Adverb"),
  RBR("Adverb, comparative"),
  RBS("Adverb, superlative"),
  RP("Particle"),
  SYM("Symbol"),
  TO("to"),
  UH("Interjection"),
  VB("Verb, base form"),
  VBD("Verb, past tense"),
  VBG("Verb, gerund or present participle"),
  VBN("Verb, past participle"),
  VBP("Verb, non-3rd person singular present"),
  VBZ("Verb, 3rd person singular present"),
  WDT("Wh-determiner"),
  WP("Wh-pronoun"),
  WP$("Possessive wh-pronoun"),
  WRB("Wh-adverb"),
  AFX("Affix"),
  GW("Additional word in multi-word expression");

  public static final Collection<PennTreebankPartOfSpeech> NOUNS = of(NN, NNP, NNPS, NNS);
  public static final Collection<PennTreebankPartOfSpeech> VERBS = of(VB, VBD, VBG, VBN, VBP, VBZ);
  public static final Collection<PennTreebankPartOfSpeech> ADVERBS = of(RB, RBR, RBS, WRB);
  public static final Collection<PennTreebankPartOfSpeech> ADJECTIVES = of(JJ, JJR, JJS);

  private final String description;

  PennTreebankPartOfSpeech(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }
}
