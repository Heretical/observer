/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.observer.cascading.instrument;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.management.DocumentService;
import cascading.property.AppProps;
import cascading.property.PropertyUtil;
import cascading.stats.CascadingStats;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowSliceStats;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.ProvidesCounters;
import heretical.observer.Model;
import heretical.observer.Tracings;
import heretical.observer.util.JARUtil;
import io.opencensus.exporter.trace.logging.LoggingTraceExporter;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static heretical.observer.cascading.instrument.Attributes.TIMES;
import static heretical.observer.cascading.instrument.Attributes.durations;

/**
 *
 */
public class ObserverSpanService implements DocumentService
  {
  private static final Logger LOG = LoggerFactory.getLogger( ObserverSpanService.class );

  private static final Set<ObserverSpanService> services = new LinkedHashSet<>();

  /**
   * For testing.
   */
  public static ObserverSpanService getService()
    {
    return services.iterator().next();
    }

  private static final Sampler sampler = Samplers.alwaysSample();
  private static Tracer tracer;
  private static String appID;
  private static Span appSpan;

  Map<CascadingStats, Span> spanMap = new IdentityHashMap<>();
  Map<String, Span> sliceSpanMap = new IdentityHashMap<>();
  ThreadLocal<FlowNodeStats> currentNode = new ThreadLocal<>();

  private Map<Object, Object> properties;

  public ObserverSpanService()
    {
    services.add( this );

    if( services.size() > 1 )
      LOG.warn( "duplicate services instantiated" );
    }

  @Override
  public void setProperties( Map<Object, Object> properties )
    {
    this.properties = new LinkedHashMap<>( properties );

    appID = AppProps.getApplicationID( properties );
    }

  @Override
  public synchronized void startService()
    {
    if( tracer != null )
      return;

    boolean loggerEnabled = PropertyUtil.getBooleanProperty( properties, Tracings.LOGGER_ENABLED, false );
    boolean zipkinEnabled = PropertyUtil.getBooleanProperty( properties, Tracings.ZIPKIN_ENABLED, false );
    String zipkinURL = PropertyUtil.getProperty( properties, Tracings.ZIPKIN_URL, "http://127.0.0.1:9411/api/v2/spans" );
    String containingJar = JARUtil.findContainingJar( ObserverSpanService.class );
    String jarName = null;
    String appName = PropertyUtil.getProperty( properties, Tracings.APP_NAME );

    if( containingJar != null )
      jarName = Paths.get( containingJar ).getFileName().toString();

    if( appName == null && jarName != null )
      appName = jarName;

    if( appName == null )
      appName = "observed";

    if( loggerEnabled )
      LoggingTraceExporter.register();

    if( zipkinEnabled )
      ZipkinTraceExporter.createAndRegister( zipkinURL, appName );

    tracer = Tracing.getTracer();

    appSpan = tracer.spanBuilderWithExplicitParent( Model.app + ":" + appID, null )
      .setSampler( sampler )
      .setRecordEvents( true )
      .startSpan();

    LOG.info( "observer beginning app trace with id: {}", appSpan.getContext().getTraceId().toLowerBase16() );
    }

  @Override
  public void stopService()
    {
    tracer = null;

    if( appSpan != null )
      {
      LOG.info( "observer ending app trace with id: {}", appSpan.getContext().getTraceId().toLowerBase16() );

      appSpan.end();
      appSpan = null;
      }

    Tracing.getExportComponent().shutdown();
    }

  @Override
  public boolean isEnabled()
    {
    return true;
    }

  @Override
  public void put( String string, Object object )
    {
    if( ( object instanceof CascadingStats ) )
      handleCascadingStats( (CascadingStats) object );
    else if( object instanceof FlowSliceStats )
      handleFlowSliceStats( (FlowSliceStats) object );
    }

  private void handleCascadingStats( CascadingStats stats )
    {
    spanMap.computeIfAbsent( stats, this::span );

    if( stats instanceof FlowNodeStats )
      ( (FlowNodeStats) stats ).recordChildStats(); // recursive child span generation

    if( stats.isFinished() )
      spanMap.remove( stats );
    }

  private void handleFlowSliceStats( FlowSliceStats stats )
    {
    FlowNodeStats flowNodeStats = currentNode.get();

    sliceSpanMap.computeIfAbsent( stats.getID(), id -> sliceSpan( flowNodeStats, stats, Model.slice ) );

    if( flowNodeStats.isFinished() )
      sliceSpanMap.remove( stats.getID() );
    }

  private Span sliceSpan( FlowNodeStats parent, FlowSliceStats stats, Model model )
    {
    Span parentSpan = spanMap.get( parent );
    Span span = tracer.spanBuilderWithExplicitParent( model + ":" + stats.getID(), parentSpan )
      .setRecordEvents( true )
      .startSpan();

    writeDurations( stats, span );

    if( stats instanceof ProvidesCounters )
      writeCounters( (ProvidesCounters) stats );

    Status status = Status.OK;

    switch( stats.getStatus() )
      {
      case SKIPPED:
        status = Status.ALREADY_EXISTS;
        break;
      case STOPPED:
        status = Status.CANCELLED;
        break;
      case FAILED:
        status = Status.INTERNAL;
        break;
      }

    span.end( EndSpanOptions.builder().setStatus( status ).build() );

    return span;
    }

  private Span span( CascadingStats stats )
    {
    Model model = Model.app;
    Span parentSpan = null;

    if( stats instanceof FlowStats )
      {
      model = Model.flow;
      parentSpan = appSpan;
      }
    else if( stats instanceof FlowStepStats )
      {
      model = Model.step;
      parentSpan = spanMap.get( ( (FlowStepStats) stats ).getFlowStep().getFlow().getFlowStats() );
      }
    else if( stats instanceof FlowNodeStats )
      {
      model = Model.node;
      parentSpan = spanMap.get( ( (FlowNodeStats) stats ).getFlowNode().getFlowStep().getFlowStepStats() );
      currentNode.set( (FlowNodeStats) stats );
      }

    return createSpan( stats, model, parentSpan );
    }

  private Span createSpan( CascadingStats stats, Model model, Span parentSpan )
    {
    Span span = tracer.spanBuilderWithExplicitParent( model + ":" + stats.getID(), parentSpan )
      .setRecordEvents( true )
      .startSpan();

    stats.addListener( ( current, fromStatus, toStatus ) ->
    {
    writeDurations( current, span );
    writeCounters( stats );

    if( toStatus.isFinished() )
      {
      Status status = Status.OK;

      switch( toStatus )
        {
        case SKIPPED:
          status = Status.ALREADY_EXISTS;
          break;
        case STOPPED:
          status = Status.CANCELLED;
          break;
        case FAILED:
          status = Status.INTERNAL;
          break;
        }

      span.end( EndSpanOptions.builder().setStatus( status ).build() );
      }
    } );

    return span;
    }

  private void writeCounters( ProvidesCounters stats )
    {

    }

  private void writeDurations( CascadingStats stats, Span span )
    {
    Long[] times = new Long[]{
      stats.getPendingTime(),
      stats.getStartTime(),
      stats.getSubmitTime(),
      stats.getRunTime(),
      stats.getFinishedTime()
    };

    writeDurations( span, times );
    }

  private void writeDurations( FlowSliceStats stats, Span span )
    {
    Long[] times = new Long[]{
      stats.getProcessPendingTime(),
      stats.getProcessStartTime(),
      stats.getProcessSubmitTime(),
      stats.getProcessRunTime(),
      stats.getProcessFinishTime()
    };

    writeDurations( span, times );
    }

  private void writeDurations( Span span, Long[] times )
    {
    long currentTime = System.currentTimeMillis();

    if( times[ 1 ] == 0 ) // not yet started
      span.putAttribute( Attributes.DURATION, AttributeValue.longAttributeValue( 0 ) );
    else if( times[ 4 ] == 0 ) // not finished
      span.putAttribute( Attributes.DURATION, AttributeValue.longAttributeValue( currentTime - times[ 1 ] ) ); // shows progress
    else // finished - started
      span.putAttribute( Attributes.DURATION, AttributeValue.longAttributeValue( times[ 4 ] - times[ 1 ] ) );

    for( int i = 0; i < TIMES.length; i++ )
      span.putAttribute( TIMES[ i ], AttributeValue.longAttributeValue( times[ i ] ) );

    int count = 0;
    for( int i = 0; i < TIMES.length; i++ )
      {
      if( times[ i ] != 0 ) // beginning time is undefined so the process is not in this 'state' - skip over
        {
        for( int j = i + 1; j < times.length; j++ )
          span.putAttribute( durations[ count++ ], AttributeValue.longAttributeValue( Math.max( 0, getNextTime( times, currentTime, j, TIMES.length ) - times[ i ] ) ) ); // zero or positive
        }
      else
        count += ( TIMES.length - 1 - i );
      }
    }

  private Long getNextTime( Long[] times, Long stopTime, int ord, int stopOrd )
    {
    if( times[ ord ] != 0 )
      return times[ ord ];
    if( ord + 1 == stopOrd )
      return stopTime;
    return getNextTime( times, stopTime, ord + 1, stopOrd );
    }

  @Override
  public void put( String type, String key, Object object )
    {

    }

  @Override
  public Map get( String string, String string1 )
    {
    return Collections.emptyMap();
    }

  @Override
  public boolean supportsFind()
    {
    return false;
    }

  @Override
  public List<Map<String, Object>> find( String string, String[] strings )
    {
    return Collections.emptyList();
    }
  }
