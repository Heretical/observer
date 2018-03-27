/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.observer.cascading.instrument;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.flow.FlowNode;
import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.CascadingStats;

/**
 *
 */
public class ObserverClientState extends ClientState
  {
  public ObserverClientState()
    {
    }

  @Override
  public void recordStats( CascadingStats stats )
    {
    if( stats == null )
      return;

    record( stats.getID(), stats );
    }

  @Override
  public void recordCascade( Cascade cascade )
    {
    }

  @Override
  public void recordFlow( Flow flow )
    {
    }

  @Override
  public void recordFlowStep( FlowStep flowStep )
    {
    }

  @Override
  public void recordFlowNode( FlowNode flowNode )
    {
    }
  }
