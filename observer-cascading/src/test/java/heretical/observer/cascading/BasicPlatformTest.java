/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.observer.cascading;

import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import heretical.observer.Tracings;
import heretical.observer.cascading.instrument.ObserverClientState;
import heretical.observer.cascading.instrument.ObserverSpanService;
import org.junit.Rule;
import org.junit.Test;
import zipkin.Span;
import zipkin.junit.ZipkinRule;

import static heretical.observer.Data.inputFileApache;

/** */
public class BasicPlatformTest extends PlatformTestCase
  {
  @Rule
  public ZipkinRule zipkin = new ZipkinRule();

  @Override
  public Map<Object, Object> getProperties()
    {
    Map<Object, Object> properties = super.getProperties();

    properties.put( "cascading3.management.state.service.classname", ObserverClientState.class.getName() );
    properties.put( "cascading3.management.document.service.classname", ObserverSpanService.class.getName() );
    properties.put( Tracings.LOGGER_ENABLED, "true" );
    properties.put( Tracings.ZIPKIN_ENABLED, "true" );
    properties.put( Tracings.ZIPKIN_URL, zipkin.httpUrl() + "/api/v2/spans" );

    return properties;
    }

  @Test
  public void testSimpleGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe =
      new Each(
        pipe,
        new Fields( "line" ),
        new RegexParser( new Fields( "ip" ), "^[^ ]*" ),
        new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simple" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, null );

    ObserverSpanService.getService().stopService();

    List<List<Span>> traces = zipkin.getTraces();

    assertEquals( 3, traces.get( 0 ).size() );
    }
  }
