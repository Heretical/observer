/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.observer.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

/**
 *
 */
public class JARUtil
  {
  public static String findContainingJar( Class<?> type )
    {
    ClassLoader classLoader = type.getClassLoader();

    String classFile = type.getName().replaceAll( "\\.", "/" ) + ".class";

    try
      {
      for( Enumeration<URL> iterator = classLoader.getResources( classFile ); iterator.hasMoreElements(); )
        {
        URL url = iterator.nextElement();

        if( !"jar".equals( url.getProtocol() ) )
          continue;

        String path = url.getPath();

        if( path.startsWith( "file:" ) )
          path = path.substring( "file:".length() );

        path = URLDecoder.decode( path, "UTF-8" );

        return path.replaceAll( "!.*$", "" );
        }
      }
    catch( IOException exception )
      {
//      System.err.println( "unable to find containing jar for: " + type.getName() );
      }

    return null;
    }
  }
