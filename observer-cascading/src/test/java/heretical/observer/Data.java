/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.observer;

/**
 *
 */
public interface Data
  {
  String TEST_DATA_PATH = "test.data.path";

  String inputPath = System.getProperty( TEST_DATA_PATH, "../observer-cascading/src/test/resources/data/" );

  String inputFileApache = inputPath + "apache.10.txt";
  }
