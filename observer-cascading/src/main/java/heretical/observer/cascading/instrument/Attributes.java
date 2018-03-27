/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.observer.cascading.instrument;

/**
 *
 */
public class Attributes
  {
  public static final String DURATION = "duration";

  public static final String[] TIMES = new String[]{
    "pendingTime",
    "startTime",
    "submitTime",
    "runTime",
    "finishedTime"
  };

  public static final String[] durations = new String[ TIMES.length * ( TIMES.length - 1 ) / 2 ];

  static
    {
    int count = 0;
    for( int i = 0; i < TIMES.length; i++ )
      {
      for( int j = i + 1; j < TIMES.length; j++ )
        durations[ count++ ] = TIMES[ i ] + ":" + TIMES[ j ];
      }
    }
  }
