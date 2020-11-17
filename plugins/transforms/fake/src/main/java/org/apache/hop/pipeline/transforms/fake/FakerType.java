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

public enum FakerType {

  Ancient( com.github.javafaker.Ancient.class, "ancient", "Ancient" ),
  App( com.github.javafaker.App.class, "app", "App" ),
  Artist( com.github.javafaker.Artist.class, "artist", "Artist" ),
  Avatar( com.github.javafaker.Avatar.class, "avatar", "Avatar" ),
  Aviation( com.github.javafaker.Aviation.class, "aviation", "Aviation" ),
  Lorem( com.github.javafaker.Lorem.class, "lorem", "Lorem" ),
  Music( com.github.javafaker.Music.class, "music", "Music" ),
  Name( com.github.javafaker.Name.class, "name", "Name" ),
  Number( com.github.javafaker.Number.class, "number", "Number" ),
  Internet( com.github.javafaker.Internet.class, "internet", "Internet" ),
  PhoneNumber( com.github.javafaker.PhoneNumber.class, "phoneNumber", "Phone number" ),
  Pokemon( com.github.javafaker.Pokemon.class, "pokemon", "Pokemon" ),
  Address( com.github.javafaker.Address.class, "address", "Address" ),
  Book( com.github.javafaker.Book.class, "book", "Book" ),
  Business( com.github.javafaker.Business.class, "business", "Business" ),
  ChuckNorris( com.github.javafaker.ChuckNorris.class, "chuckNorris", "Chuck Norris" ),
  Color( com.github.javafaker.Color.class, "Color", "Color" ),
  IdNumber( com.github.javafaker.IdNumber.class, "idNumber", "Id/Number" ),
  Hacker( com.github.javafaker.Hacker.class, "hacker", "Hacker" ),
  Company( com.github.javafaker.Company.class, "company", "Company" ),
  Crypto( com.github.javafaker.Crypto.class, "crypto", "Crypto" ),
  Elder( com.github.javafaker.ElderScrolls.class, "elder", "Elder" ),
  Commerce( com.github.javafaker.Commerce.class, "commerce", "Commerce" ),
  Currency( com.github.javafaker.Currency.class, "currency", "Currency" ),
  Options( com.github.javafaker.Options.class, "options", "Options" ),
  Code( com.github.javafaker.Code.class, "code", "Code" ),
  File( com.github.javafaker.File.class, "file", "File" ),
  Finance( com.github.javafaker.Finance.class, "finance", "Finance" ),
  Food( com.github.javafaker.Food.class, "food", "Food" ),
  GameOfThrones( com.github.javafaker.GameOfThrones.class, "gameOfThrones", "Game of Thrones" ),
  DateAndTime( com.github.javafaker.DateAndTime.class, "dateAndTime", "Date and Time" ),
  Demographic( com.github.javafaker.Demographic.class, "demographic", "Demographic" ),
  Dog( com.github.javafaker.Dog.class, "dog", "Dog" ),
  Educator( com.github.javafaker.Educator.class, "educator", "Educator" ),
  Shakespeare( com.github.javafaker.Shakespeare.class, "shakespeare", "Shakespeare" ),
  SlackEmoji( com.github.javafaker.SlackEmoji.class, "slackEmoji", "Slack emoji" ),
  Space( com.github.javafaker.Space.class, "space", "Space" ),
  Superhero( com.github.javafaker.Superhero.class, "superhero", "Superhero" ),
  Team( com.github.javafaker.Team.class, "team", "Team" ),
  Bool( com.github.javafaker.Bool.class, "bool", "Bool" ),
  Beer( com.github.javafaker.Beer.class, "beer", "Beer" ),
  University( com.github.javafaker.University.class, "university", "University" ),
  Cat( com.github.javafaker.Cat.class, "cat", "Cat" ),
  Stock( com.github.javafaker.Stock.class, "stock", "Stock" ),
  LordOfTheRings( com.github.javafaker.LordOfTheRings.class, "lordOfTheRings", "Lord of the rings" ),
  Zelda( com.github.javafaker.Zelda.class, "zelda", "Zelda" ),
  HarryPotter( com.github.javafaker.HarryPotter.class, "harryPotter", "Harry Potter" ),
  RockBand( com.github.javafaker.RockBand.class, "rockBand", "Rock Band" ),
  Esports( com.github.javafaker.Esports.class, "esports", "e-Sports" ),
  Friends( com.github.javafaker.Friends.class, "friends", "Friends" ),
  Hipster( com.github.javafaker.Hipster.class, "hipster", "Hipster" ),
  Job( com.github.javafaker.Job.class, "job", "Job" ),
  TwinPeaks( com.github.javafaker.TwinPeaks.class, "twinPeaks", "Twin Peaks" ),
  RickAndMorty( com.github.javafaker.RickAndMorty.class, "rickAndMorty", "Rick and Morty" ),
  Yoda( com.github.javafaker.Yoda.class, "yoda", "Yoda" ),
  Matz( com.github.javafaker.Matz.class, "matz", "Matz" ),
  Witcher( com.github.javafaker.Witcher.class, "witcher", "Witcher" ),
  DragonBall( com.github.javafaker.DragonBall.class, "dragonBall", "Dragon Ball" ),
  FunnyName( com.github.javafaker.FunnyName.class, "funnyName", "Funny name" ),
  HitchhikersGuideToTheGalaxy( com.github.javafaker.HitchhikersGuideToTheGalaxy.class, "hitchhikersGuideToTheGalaxy", "Hitchhikers guide to the galaxy" ),
  Hobbit( com.github.javafaker.Hobbit.class, "hobbit", "Hobbit" ),
  HowIMetYourMother( com.github.javafaker.HowIMetYourMother.class, "howIMetYourMother", "How I met your mother" ),
  LeagueOfLegends( com.github.javafaker.LeagueOfLegends.class, "leagueOfLegends", "League of Legends" ),
  Overwatch( com.github.javafaker.Overwatch.class, "overwatch", "Overwatch" ),
  Robin( com.github.javafaker.Robin.class, "robin", "Robin" ),
  StarTrek( com.github.javafaker.StarTrek.class, "starTrek", "Star Trek" ),
  Weather( com.github.javafaker.Weather.class, "weather", "Weather" ),
  Lebowski( com.github.javafaker.Lebowski.class, "lebowski", "The Big Lebowski" ),
  Medical( com.github.javafaker.Medical.class, "medical", "Medical" ),
  Country( com.github.javafaker.Country.class, "country", "Country" ),
  Animal( com.github.javafaker.Animal.class, "animal", "Animal" ),
  BackToTheFuture( com.github.javafaker.BackToTheFuture.class, "backToTheFuture", "Back to the future" ),
  PrincessBride( com.github.javafaker.PrincessBride.class, "princessBride", "Princess bride" ),
  Buffy( com.github.javafaker.Buffy.class, "buffy", "Buffy" ),
  Relationships( com.github.javafaker.Relationships.class, "relationshops", "Relationships" ),
  Nation( com.github.javafaker.Nation.class, "nation", "Nation" ),
  Dune( com.github.javafaker.Dune.class, "dune", "Dune" ),
  AquaTeenHungerForce( com.github.javafaker.AquaTeenHungerForce.class, "aquaTeenHungerForce", "Aqua teen hunger force" ),
  ProgrammingLanguage( com.github.javafaker.ProgrammingLanguage.class, "programmingLanguage", "Programming language" );

  private Class<?> fakerClass;
  private String fakerMethod;
  private String description;

  FakerType( Class<?> fakerClass, String fakerMethod, String description ) {
    this.fakerClass = fakerClass;
    this.fakerMethod = fakerMethod;
    this.description = description;
  }

  public static final String[] getTypeNames() {
    String[] names = new String[values().length];
    for (int i=0;i<names.length;i++) {
      names[i] = values()[i].name();
    }
    return names;
  }

  public static final FakerType getTypeUsingName(String name) {
    for (FakerType fakerType : values()) {
      if (fakerType.name().equalsIgnoreCase( name )) {
        return fakerType;
      }
    }
    return null;
  }

  public static final String[] getTypeDescriptions() {
    String[] descriptions = new String[values().length];
    for (int i=0;i<descriptions.length;i++) {
      descriptions[i] = values()[i].getDescription();
    }
    return descriptions;
  }

  public static final FakerType getTypeUsingDescription(String description) {
    for (FakerType fakerType : values()) {
      if (fakerType.getDescription().equalsIgnoreCase( description )) {
        return fakerType;
      }
    }
    return null;
  }



  /**
   * Gets fakerClass
   *
   * @return value of fakerClass
   */
  public Class<?> getFakerClass() {
    return fakerClass;
  }

  /**
   * @param fakerClass The fakerClass to set
   */
  public void setFakerClass( Class<?> fakerClass ) {
    this.fakerClass = fakerClass;
  }

  /**
   * Gets fakerMethod
   *
   * @return value of fakerMethod
   */
  public String getFakerMethod() {
    return fakerMethod;
  }

  /**
   * @param fakerMethod The fakerMethod to set
   */
  public void setFakerMethod( String fakerMethod ) {
    this.fakerMethod = fakerMethod;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }
}
