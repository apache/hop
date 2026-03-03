/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.execution;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;
import lombok.Getter;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

@Getter
public enum LastPeriod implements IEnumHasCodeAndDescription {
  NONE("-"),
  ONE_MINUTE("< 1m"),
  TWO_MINUTES("< 2m"),
  FIVE_MINUTES("< 5m"),
  FIFTEEN_MINUTES("< 15m"),
  THIRTY_MINUTES("< 30m"),
  ONE_HOUR("< 1h"),
  TWO_HOURS("< 2h"),
  THREE_HOURS("< 3h"),
  FOUR_HOURS("< 4h"),
  FIVE_HOURS("< 5h"),
  SIX_HOURS("< 6h"),
  TWELVE_HOURS("< 12h"),
  ONE_DAY("< 1d"),
  TWO_DAYS("< 2d"),
  THREE_DAYS("< 3d"),
  FOUR_DAYS("< 4d"),
  ONE_WEEK("< 1w"),
  TWO_WEEKS("< 2w"),
  ONE_MONTH("< 1M"),
  TWO_MONTHS("< 2M"),
  THREE_MONTHS("< 3M"),
  SIX_MONTHS("< 6M"),
  ONE_YEAR("< 1Y"),
  ;
  private final String description;

  private LastPeriod(String description) {
    this.description = description;
  }

  public String getCode() {
    return name();
  }

  public static LastPeriod lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(LastPeriod.class, description, ONE_DAY);
  }

  public boolean matches(Date date) {
    LocalDateTime pastTime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    LocalDateTime now = LocalDateTime.now();

    return switch (this) {
      case ONE_MINUTE -> Duration.between(pastTime, now).toMinutes() <= 1;
      case TWO_MINUTES -> Duration.between(pastTime, now).toMinutes() <= 2;
      case FIVE_MINUTES -> Duration.between(pastTime, now).toMinutes() <= 5;
      case FIFTEEN_MINUTES -> Duration.between(pastTime, now).toMinutes() <= 15;
      case THIRTY_MINUTES -> Duration.between(pastTime, now).toMinutes() <= 30;
      case ONE_HOUR -> Duration.between(pastTime, now).toHours() <= 1;
      case TWO_HOURS -> Duration.between(pastTime, now).toHours() <= 2;
      case THREE_HOURS -> Duration.between(pastTime, now).toHours() <= 3;
      case FOUR_HOURS -> Duration.between(pastTime, now).toHours() <= 4;
      case FIVE_HOURS -> Duration.between(pastTime, now).toHours() <= 5;
      case SIX_HOURS -> Duration.between(pastTime, now).toHours() <= 6;
      case TWELVE_HOURS -> Duration.between(pastTime, now).toHours() <= 12;
      case ONE_DAY -> Duration.between(pastTime, now).toDays() <= 1;
      case TWO_DAYS -> Duration.between(pastTime, now).toDays() <= 2;
      case THREE_DAYS -> Duration.between(pastTime, now).toDays() <= 3;
      case FOUR_DAYS -> Duration.between(pastTime, now).toDays() <= 4;
      case ONE_WEEK -> Duration.between(pastTime, now).toDays() <= 7;
      case TWO_WEEKS -> Duration.between(pastTime, now).toDays() <= 14;
      case ONE_MONTH -> Period.between(pastTime.toLocalDate(), now.toLocalDate()).getMonths() <= 1;
      case TWO_MONTHS -> Period.between(pastTime.toLocalDate(), now.toLocalDate()).getMonths() <= 2;
      case THREE_MONTHS ->
          Period.between(pastTime.toLocalDate(), now.toLocalDate()).getMonths() <= 3;
      case SIX_MONTHS -> Period.between(pastTime.toLocalDate(), now.toLocalDate()).getMonths() <= 6;
      case ONE_YEAR -> Period.between(pastTime.toLocalDate(), now.toLocalDate()).getYears() <= 1;
      default -> true;
    };
  }

  public LocalDateTime calculateStartDate() {
    LocalDateTime now = LocalDateTime.now();

    return switch (this) {
      case ONE_MINUTE -> now.minusMinutes(1);
      case TWO_MINUTES -> now.minusMinutes(2);
      case FIVE_MINUTES -> now.minusMinutes(5);
      case FIFTEEN_MINUTES -> now.minusMinutes(15);
      case THIRTY_MINUTES -> now.minusMinutes(30);
      case ONE_HOUR -> now.minusHours(1);
      case TWO_HOURS -> now.minusHours(2);
      case THREE_HOURS -> now.minusHours(3);
      case FOUR_HOURS -> now.minusHours(4);
      case FIVE_HOURS -> now.minusHours(5);
      case SIX_HOURS -> now.minusHours(6);
      case TWELVE_HOURS -> now.minusHours(12);
      case ONE_DAY -> now.minusDays(1);
      case TWO_DAYS -> now.minusDays(2);
      case THREE_DAYS -> now.minusDays(3);
      case FOUR_DAYS -> now.minusDays(4);
      case ONE_WEEK -> now.minusDays(7);
      case TWO_WEEKS -> now.minusDays(14);
      case ONE_MONTH -> now.minusMonths(1);
      case TWO_MONTHS -> now.minusMonths(2);
      case THREE_MONTHS -> now.minusMonths(3);
      case SIX_MONTHS -> now.minusMonths(6);
      case ONE_YEAR -> now.minusYears(1);
      default -> LocalDateTime.of(1900, 1, 1, 0, 0);
    };
  }
}
