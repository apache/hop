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
package org.apache.hop.pipeline.transforms.fake;

import org.apache.commons.lang3.StringUtils;

/**
 * Legacy compatibility table for the Fake transform.
 *
 * <p>Historically a {@link FakeField}'s {@code type} stored one of these enum constant names and
 * {@code topic} stored the provider method. Since the move to DataFaker, new fields store the
 * DataFaker accessor method directly (e.g. {@code name}, {@code phoneNumber}). This enum only
 * survives so that pipelines saved with the old constant names ({@code Name}, {@code PhoneNumber},
 * {@code DateAndTime}, ...) keep resolving. See {@link #resolveAccessorMethod(String)}.
 */
@SuppressWarnings("java:S115")
public enum FakerType {
  Ancient("ancient"),
  App("app"),
  Artist("artist"),
  Avatar("avatar"),
  Aviation("aviation"),
  Lorem("lorem"),
  Music("music"),
  Name("name"),
  Number("number"),
  Internet("internet"),
  PhoneNumber("phoneNumber"),
  Pokemon("pokemon"),
  Address("address"),
  Book("book"),
  Business("business"),
  ChuckNorris("chuckNorris"),
  Color("color"),
  IdNumber("idNumber"),
  Hacker("hacker"),
  Company("company"),
  Crypto("crypto"),
  Elder("elderScrolls"),
  Commerce("commerce"),
  Currency("currency"),
  Options("options"),
  Code("code"),
  File("file"),
  Finance("finance"),
  Food("food"),
  GameOfThrones("gameOfThrones"),
  DateAndTime("date"),
  Demographic("demographic"),
  Dog("dog"),
  Educator("educator"),
  Shakespeare("shakespeare"),
  SlackEmoji("slackEmoji"),
  Space("space"),
  Superhero("superhero"),
  Team("team"),
  Bool("bool"),
  Beer("beer"),
  University("university"),
  Cat("cat"),
  Stock("stock"),
  LordOfTheRings("lordOfTheRings"),
  Zelda("zelda"),
  HarryPotter("harryPotter"),
  RockBand("rockBand"),
  Esports("esports"),
  Friends("friends"),
  Hipster("hipster"),
  Job("job"),
  TwinPeaks("twinPeaks"),
  RickAndMorty("rickAndMorty"),
  Yoda("yoda"),
  Matz("matz"),
  Witcher("witcher"),
  DragonBall("dragonBall"),
  FunnyName("funnyName"),
  HitchhikersGuideToTheGalaxy("hitchhikersGuideToTheGalaxy"),
  Hobbit("hobbit"),
  HowIMetYourMother("howIMetYourMother"),
  LeagueOfLegends("leagueOfLegends"),
  Overwatch("overwatch"),
  Robin("robin"),
  StarTrek("starTrek"),
  Weather("weather"),
  Lebowski("lebowski"),
  Medical("medical"),
  Country("country"),
  Animal("animal"),
  BackToTheFuture("backToTheFuture"),
  PrincessBride("princessBride"),
  Buffy("buffy"),
  Relationships("relationships"),
  Nation("nation"),
  Dune("dune"),
  AquaTeenHungerForce("aquaTeenHungerForce"),
  ProgrammingLanguage("programmingLanguage");

  private final String fakerMethod;

  FakerType(String fakerMethod) {
    this.fakerMethod = fakerMethod;
  }

  public String getFakerMethod() {
    return fakerMethod;
  }

  /** Look up a legacy type by its (case-insensitive) enum constant name. */
  public static FakerType getTypeUsingName(String name) {
    if (StringUtils.isEmpty(name)) {
      return null;
    }
    for (FakerType fakerType : values()) {
      if (fakerType.name().equalsIgnoreCase(name)) {
        return fakerType;
      }
    }
    return null;
  }

  /**
   * Translate a stored {@link FakeField#getType()} value into a DataFaker accessor method name.
   *
   * <p>Pipelines created before the DataFaker migration stored the enum constant name (e.g. {@code
   * DateAndTime}); newer ones store the accessor directly (e.g. {@code date}). Both resolve here.
   *
   * @param storedType the {@code type} value persisted in the pipeline metadata
   * @return the DataFaker {@code Faker} accessor method to invoke
   */
  public static String resolveAccessorMethod(String storedType) {
    FakerType legacy = getTypeUsingName(storedType);
    return legacy != null ? legacy.fakerMethod : storedType;
  }
}
