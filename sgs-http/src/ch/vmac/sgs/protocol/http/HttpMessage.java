/*
 *  Copyright 2010 Christiaan Verbree.
 *  This file is part of sgs-http-protocol.
 *
 *  Sgs-http-protocol is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  Sgs-http-protocol is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with sgs-http-protocol.  If not, see <http://www.gnu.org/licenses/>.
 */

package ch.vmac.sgs.protocol.http;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.sgs.impl.sharedutil.LoggerWrapper;

/**
 * This is a helper class for receiving http request.
 * @author Christiaan Verbree
 * 
 */
public class HttpMessage {
	static final LoggerWrapper logger = new LoggerWrapper(Logger.getLogger(HttpMessage.class.getName()));

	private final static byte HEADER_DELIM = ':';
	
	public static enum HttpMethod {
		GET, POST
	}

	private HttpMethod method;
	private String uri;
	private String version;
	private boolean hasRequestLine;
	private Map<String, String> headers;
	private ByteBuffer body;

	public HttpMessage() {
		headers = new HashMap<String, String>();
		body = ByteBuffer.allocate(0);
	}
	
	public HttpMessage(HttpMessage orginal) {
		method = orginal.method;
		uri = orginal.uri;
		version = orginal.version;
		hasRequestLine = orginal.hasRequestLine;
		headers = new HashMap<String, String>(orginal.headers);
		body = orginal.body.duplicate();
	}

	/**
	 * @return the method
	 */
	public HttpMethod getMethod() {
		return method;
	}

	public void setMethod(String string) {
		for (HttpMethod m : HttpMethod.values()) {
			if (m.toString().equalsIgnoreCase(string)) {
				setMethod(m);
				return;
			}
		}

		throw new IllegalArgumentException("Unknown http method: " + string);
	}

	/**
	 * @param method
	 *            the method to set
	 */
	public void setMethod(HttpMethod method) {
		this.method = method;
	}

	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @param uri
	 *            the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	/**
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * @param version
	 *            the version to set
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	public boolean hasHeader(String field) {
		return headers.containsKey(field);
	}

	/**
	 * @return the headers
	 */
	public String getHeader(String field) {
		return headers.get(field);
	}
	
	public int getHeaderCount() {
		return headers.size();
	}
	
	public Iterator<Entry<String, String>> getHeaderIterator() {
		return headers.entrySet().iterator();
	}

	/**
	 * Will parse a header line. It expects the http request line when
	 * first called. After this it expects http header fields (with a colon as delimiter).
	 * @param line The header line to add.
	 */
	public void addHeader(String line) {
		if(logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "Found header line: {0}", line);
		}

		if (!hasRequestLine) {
			if (line.indexOf(HEADER_DELIM) == -1) {

				String[] rl = line.split(" ");
				setMethod(rl[0]);
				setUri(rl[1]);
				setVersion(rl[2]);
				hasRequestLine = true;
				return;
			}
			throw new IllegalStateException("first header needs to be the request line!");
		}

		String[] h = line.split(":");
		addHeader(h[0].trim(), h[1].trim());
	}

	/**
	 * @param headers
	 *            the headers to set
	 */
	public void addHeader(String field, String value) {
		headers.put(field, value);
	}

	/**
	 * @return the body
	 */
	public ByteBuffer getBody() {
		return (ByteBuffer) body.duplicate().flip();
	}

	/**
	 * @param body
	 *            the body to set
	 */
	public void setBody(ByteBuffer body) {
		this.body = body;
	}
	
	@Override
	public String toString() {
		return "[" + getMethod() + " " + getUri() + " " + getVersion() + "]";
	}
}
