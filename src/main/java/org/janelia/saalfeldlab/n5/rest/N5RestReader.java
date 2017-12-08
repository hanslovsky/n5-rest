/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;

import org.janelia.saalfeldlab.n5.AbstractGsonReader;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * N5 implementation using Google Cloud Storage backend.
 *
 * This implementation enforces that an empty attributes file is present for
 * each group. It is used for determining group existence and listing groups.
 *
 * @author Philipp Hanslovsky
 */
class N5RestReader extends AbstractGsonReader
{

	private static final String jsonFile = "attributes.json";

	private static final String delimiter = "/";

	private static final String GET = "GET";

	private static final int RESPONSE_OK = 200;

	private final URI groupUrl;

	private final int connectionTimeout;

	private final int readTimeout;

	/**
	 * Opens an {@link N5GoogleRestReader} with a custom {@link GsonBuilder} to
	 * support custom attributes at a specified URL.
	 *
	 * @param url
	 * @param gsonBuilder
	 */
	public N5RestReader( final String groupUrl, final GsonBuilder gsonBuilder, final int connectionTimeout, final int readTimeout )
	{

		super( gsonBuilder );
		this.groupUrl = URI.create( groupUrl );
		this.connectionTimeout = connectionTimeout;
		this.readTimeout = readTimeout;
	}

	@Override
	public boolean exists( final String dataset )
	{
		final URI url = getAttributesUrl( dataset );
		try
		{
			final HttpURLConnection connection = getConnection( url.toURL() );
			return connection.getResponseCode() == RESPONSE_OK;
		}
		catch ( final IOException e )
		{
			return false;
		}
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public HashMap< String, JsonElement > getAttributes( final String dataset ) throws IOException
	{
		final URI url = getAttributesUrl( dataset );
		final HttpURLConnection connection = getConnection( url.toURL() );
		try (InputStream inputStream = connection.getInputStream())
		{
			return gson.fromJson( new InputStreamReader( inputStream ), HashMap.class );
		}
	}

	@Override
	public DataBlock< ? > readBlock(
			final String dataset,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition ) throws IOException
	{
		final URI dataBlockUri = getDataBlockUri( dataset, gridPosition );
		final HttpURLConnection connection = getConnection( dataBlockUri.toURL() );

		try (final InputStream in = connection.getInputStream())
		{
			return DefaultBlockReader.readBlock( in, datasetAttributes, gridPosition );
		}
	}

	@Override
	public String[] list( final String pathName ) throws IOException
	{

		return new String[] {};
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid
	 * position.
	 *
	 * The returned path is
	 *
	 * <pre>
	 * $datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param datasetPathName
	 * @param gridPosition
	 * @return
	 */
	private URI getDataBlockUri(
			final String dataset,
			final long[] gridPosition )
	{

		return groupUrl.resolve( dataset ).resolve( String.join( delimiter, Arrays.stream( gridPosition ).mapToObj( Long::toString ).toArray( String[]::new ) ) );
	}

	/**
	 * Constructs the url for the attributes file of a group or dataset.
	 *
	 * @param dataset
	 * @return
	 */
	private URI getAttributesUrl( final String dataset )
	{
		return this.groupUrl.resolve( dataset ).resolve( jsonFile );
	}

	private HttpURLConnection getConnection( final URL url ) throws IOException
	{

		final HttpURLConnection connection = ( HttpURLConnection ) url.openConnection();
		connection.setRequestMethod( GET );
		connection.setConnectTimeout( connectionTimeout );
		connection.setReadTimeout( readTimeout );
		return connection;
	}
}
