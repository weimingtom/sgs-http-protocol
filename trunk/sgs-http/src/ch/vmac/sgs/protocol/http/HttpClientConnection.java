/*
 *  Copyright 2010 Christiaan Verbree.
 *  This file is part of sgs-http-protocol. And is based in part of the
 *  simple sgs protocol from project darkstar.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.sgs.auth.Identity;
import com.sun.sgs.impl.sharedutil.HexDumper;
import com.sun.sgs.impl.sharedutil.LoggerWrapper;
import com.sun.sgs.impl.util.AbstractKernelRunnable;
import com.sun.sgs.nio.channels.AsynchronousByteChannel;
import com.sun.sgs.nio.channels.ClosedAsynchronousChannelException;
import com.sun.sgs.nio.channels.CompletionHandler;
import com.sun.sgs.nio.channels.IoFuture;
import com.sun.sgs.nio.channels.ReadPendingException;
import com.sun.sgs.protocol.ProtocolListener;
import com.sun.sgs.protocol.simple.SimpleSgsProtocol;

/**
 * This class represents a http connection to the client. Upon creation this
 * class will read the http request and
 * <ul>
 * <li>create a new session</li>
 * <li>forward the data to the session</li>
 * <li>return the requested file (for debugging only).
 * </ul>
 * Then the connection is ready for writing. You can only write once to a
 * connection. After this the connection will be closed.
 * 
 * @author Christiaan Verbree
 * 
 */
public class HttpClientConnection {
	private static final LoggerWrapper logger = new LoggerWrapper(Logger.getLogger(HttpClientConnection.class.getName()));

	private final static String CRLF = "\r\n";

	private static final String SERVER_VERSION = "sgs-http-protocol-1";

	private final SgsHttpProtocolAcceptor acceptor;
	private final ProtocolListener protocolListener;

	private HttpClientSession clientSession = null;

	/**
	 * The underlying channel.
	 */
	private AsynchronousMessageChannel asyncMsgChannel;

	/** The identity. */
	private volatile Identity identity;

	/** The completion handler for reading from the I/O channel. */
	private volatile ReadHandler readHandler = new ConnectedReadHandler();

	/** The completion handler for writing to the I/O channel. */
	private volatile WriteHandler writeHandler = new ConnectedWriteHandler();

	/**
	 * @param acceptor
	 * @param protocolListener
	 */
	public HttpClientConnection(SgsHttpProtocolAcceptor acceptor, ProtocolListener protocolListener, AsynchronousByteChannel byteChannel) {
		this.acceptor = acceptor;
		this.protocolListener = protocolListener;

		this.asyncMsgChannel = new AsynchronousMessageChannel(byteChannel, acceptor.readBufferSize);
		scheduleReadOnReadHandler();
	}

	public boolean isOpen() {
		return asyncMsgChannel.isOpen();
	}

	/**
	 * Schedules an asynchronous task to resume reading.
	 */
	protected void scheduleReadOnReadHandler() {
		acceptor.scheduleNonTransactionalTask(new AbstractKernelRunnable("ResumeReadOnReadHandler") {
			public void run() {
				if (asyncMsgChannel.isOpen()) {
					// synchronized (handlerLock) {
					if (asyncMsgChannel.isOpen()) {
						logger.log(Level.FINER, "resuming reads protocol:{0} hascode: {1} readHandler: {2}", this, HttpClientConnection.this
								.hashCode(), readHandler);
						readHandler.read();
					}
					// }

				}
			}
		});
	}

	/**
	 * Writes a message to the write handler.
	 * 
	 * @param buf
	 *            a buffer containing a complete protocol message
	 */
	public void writeAndCloseConnection(ByteBuffer message) {
		try {
			// write header:
			ByteBuffer bb = ByteBuffer.allocate(Short.MAX_VALUE);
			bb.put(sendHttpResponseHeaders(message.limit(), null));
			bb.put(message);
			bb.flip();
			writeHandler.write(bb);
			// writeHandler.write(sendHttpResponseHeaders(message.limit(),
			// null));

			// write body:
			// writeHandler.write(message);

		} catch (RuntimeException e) {
			if (logger.isLoggable(Level.WARNING)) {
				logger.logThrow(Level.WARNING, e, "writeToWriteHandler protocol:{0} throws", this);
			}
		}
	}

	private ByteBuffer sendHttpResponseHeaders(int contentLength, Map<String, String> headers) {
		if (headers == null) {
			headers = new HashMap<String, String>();
		}

		StringBuffer sb = new StringBuffer();
		sb.append("HTTP/1.0 200 OK" + CRLF);
		sb.append("Content-Length: " + contentLength + CRLF);

		if (!headers.containsKey("Server")) {
			headers.put("Server", SERVER_VERSION);
		}
		if (!headers.containsKey("Content-type")) {
			headers.put("Content-type", "text/html; charset=UTF-8");
		}

		for (Entry<String, String> e : headers.entrySet()) {
			sb.append(e.getKey());
			sb.append(':');
			sb.append(e.getValue());
			sb.append(CRLF);
		}

		sb.append(CRLF);

		String rsp = sb.toString();
		return ByteBuffer.wrap(rsp.getBytes());
	}

	private void sendHttpError(String error, String uri) {
		StringBuffer sb = new StringBuffer();
		sb.append("HTTP/1.0 " + error + CRLF);
		sb.append("Server: " + SERVER_VERSION + CRLF);
		sb.append("Content-type: text/html" + CRLF);
		sb.append("Content-Length: 0" + CRLF);
		sb.append(CRLF);

		String rsp = sb.toString();
		writeHandler.write(ByteBuffer.wrap(rsp.getBytes()));
	}

	/**
	 * Method to disconnect the connection.
	 */
	public void disconnect() {
		// synchronized (handlerLock) {
		try {
			if (logger.isLoggable(Level.FINER)) {
				logger.log(Level.FINER, "discconect connection");
			}
			asyncMsgChannel.close();
			readHandler = new ClosedReadHandler();
			writeHandler = new ClosedWriteHandler();
		} catch (IOException e) {
			logger.logThrow(Level.FINE, e, "IO Exception while disconecting the connection for session {0}", clientSession);
		}
		// }
	}

	/** A completion handler for writing to a connection. */
	private abstract class WriteHandler implements CompletionHandler<Void, Void> {
		/** Writes the specified message. */
		abstract void write(ByteBuffer message);
	}

	/** A completion handler for writing that always fails. */
	private class ClosedWriteHandler extends WriteHandler {

		ClosedWriteHandler() {
		}

		@Override
		void write(ByteBuffer message) {
			throw new ClosedAsynchronousChannelException();
		}

		public void completed(IoFuture<Void, Void> result) {
			throw new AssertionError("should be unreachable");
		}
	}

	/** A completion handler for writing to the session's channel. */
	private class ConnectedWriteHandler extends WriteHandler {

		/**
		 * The lock for accessing the fields {@code pendingWrites} and {@code
		 * isWriting}. The locks {@code lock} and {@code writeLock} should only
		 * be acquired in that specified order.
		 */
		private final Object writeLock = new Object();

		/** An unbounded queue of messages waiting to be written. */
		private final LinkedList<ByteBuffer> pendingWrites = new LinkedList<ByteBuffer>();

		/** Whether a write is underway. */
		private boolean isWriting = false;

		/** Creates an instance of this class. */
		ConnectedWriteHandler() {
		}

		/**
		 * Adds the message to the queue, and starts processing the queue if
		 * needed.
		 */
		@Override
		void write(ByteBuffer message) {
			if (message.remaining() > SimpleSgsProtocol.MAX_PAYLOAD_LENGTH) {
				throw new IllegalArgumentException("message too long: " + message.remaining() + " > " + SimpleSgsProtocol.MAX_PAYLOAD_LENGTH);
			}
			boolean first;
			synchronized (writeLock) {
				first = pendingWrites.isEmpty();
				pendingWrites.add(message);
			}
			if (logger.isLoggable(Level.FINEST)) {
				logger.log(Level.FINEST, "write protocol:{0} message:{1} first:{2}", HttpClientConnection.this, HexDumper.format(message, 0x20),
						first);
			}
			if (first) {
				processQueue();
			}
		}

		/** Start processing the first element of the queue, if present. */
		private void processQueue() {
			ByteBuffer message;
			synchronized (writeLock) {
				if (isWriting) {
					return;
				}
				message = pendingWrites.peek();
				if (message == null) {
					return;
				}
				isWriting = true;
			}
			if (logger.isLoggable(Level.FINEST)) {
				logger.log(Level.FINEST, "processQueue protocol:{0} size:{1,number,#} head={2} pos: {3,number,#} limit: {4,number,#}",
						HttpClientConnection.this, pendingWrites.size(), HexDumper.format(message, 0x20), message.position(), message.limit());
			}
			try {
				asyncMsgChannel.write(message, this);
			} catch (RuntimeException e) {
				logger.logThrow(Level.SEVERE, e, "{0} processing message {1}", HttpClientConnection.this, HexDumper.format(message, 0x20));
				throw e;
			}
		}

		/** Done writing the first request in the queue. */
		public void completed(IoFuture<Void, Void> result) {
			ByteBuffer message;
			synchronized (writeLock) {
				message = pendingWrites.remove();
				isWriting = false;
			}
			if (logger.isLoggable(Level.FINEST)) {
				ByteBuffer resetMessage = message.duplicate();
				resetMessage.reset();
				logger.log(Level.FINEST, "completed write protocol:{0} message:{1}", HttpClientConnection.this, HexDumper.format(resetMessage, 0x50));
			}
			try {
				result.getNow();
				/* Keep writing */
				processQueue();
			} catch (ExecutionException e) {
				/*
				 * TBD: If we're expecting the session to close, don't complain.
				 */
				if (logger.isLoggable(Level.FINE)) {
					logger.logThrow(Level.FINE, e, "write protocol:{0} message:{1} throws", HttpClientConnection.this, HexDumper
							.format(message, 0x50));
				}
				synchronized (writeLock) {
					pendingWrites.clear();
				}
				if (logger.isLoggable(Level.FINEST)) {
					logger.log(Level.FINEST, "got execution exception disconnect XXX");
				}
				disconnect();
			}
		}
	}

	/** A completion handler for reading from a connection. */
	private abstract class ReadHandler implements CompletionHandler<HttpMessage, Void> {
		/** Initiates the read request. */
		abstract void read();
	}

	/** A completion handler for reading that always fails. */
	private class ClosedReadHandler extends ReadHandler {

		ClosedReadHandler() {
		}

		@Override
		void read() {
			throw new ClosedAsynchronousChannelException();
		}

		public void completed(IoFuture<HttpMessage, Void> result) {
			throw new AssertionError("should be unreachable");
		}
	}

	/** A completion handler for reading from the session's channel. */
	private class ConnectedReadHandler extends ReadHandler {

		/**
		 * The lock for accessing the {@code isReading} field. The locks {@code
		 * lock} and {@code readLock} should only be acquired in that specified
		 * order.
		 */
		private final Object readLock = new Object();

		/** Whether a read is underway. */
		private boolean isReading = false;

		/** Creates an instance of this class. */
		ConnectedReadHandler() {
		}

		/** Reads a message from the connection. */
		@Override
		void read() {
			synchronized (readLock) {
				if (isReading) {
					throw new ReadPendingException();
				}
				isReading = true;
			}
			asyncMsgChannel.read(this);
		}

		/** Handles the completed read operation. */
		public void completed(IoFuture<HttpMessage, Void> result) {
			synchronized (readLock) {
				isReading = false;
			}
			try {
				HttpMessage message = result.getNow();
				if (message == null) {
					if (logger.isLoggable(Level.FINEST)) {
						logger.log(Level.FINEST, "message is null disconnect! XXX");
					}
					disconnect();
					return;
				}
				if (logger.isLoggable(Level.FINEST)) {
					// logger.log(Level.FINEST,
					// "completed read protocol:{0} message:{1}",
					// HttpClientConnection.this, HexDumper.format(message,
					// 0x50));
				}

				handleHttpRequest(message);

			} catch (Exception e) {

				/*
				 * TBD: If we're expecting the channel to close, don't complain.
				 */

				if (logger.isLoggable(Level.FINE)) {
					logger.logThrow(Level.FINE, e, "Read completion exception {0}", asyncMsgChannel);
				}
				disconnect();
			}
		}

		private void handleHttpRequest(HttpMessage message) {
			if (message.getUri().startsWith("/d/")) {
				// serves files which are in the sgs working dir
				String file = message.getUri().substring(3);
				if (file.length() == 0) {
					file = "index.html";
				}
				File f = new File(file);
				logger.log(Level.INFO, "Load file {0}", f);
				String c = readFile(f);
				writeAndCloseConnection(ByteBuffer.wrap(c.getBytes()));
				return;
			}

			// Is valid sgs message?
			if (!message.hasHeader("Sgs-Op")) {
				sendHttpError("400 Bad request", message.getUri());
				if (logger.isLoggable(Level.FINE)) {
					logger.log(Level.FINE, "Got invalid message (missing Sgs-Op Header) for uri: {0}!", message.getUri());
				}
				// disconnect();
				return;
			}

			byte op = Byte.parseByte(message.getHeader("Sgs-Op"));
			if (!message.hasHeader("Sgs-Session")) {
				if (op != HttpClientSession.LOGIN_REQUEST) {
					if (logger.isLoggable(Level.FINE)) {
						logger.log(Level.FINE, "Got invalid message (missing Sgs-Session Header) for uri: {0}!", message.getUri());
					}
					sendHttpError("400 Bad request", message.getUri());
					// disconnect();
					return;
				}
				if (!message.hasHeader("Sgs-User") || !message.hasHeader("Sgs-Pass")) {
					if (logger.isLoggable(Level.FINE)) {
						logger.log(Level.FINE, "Got invalid login message (missing Sgs-User or Sgs-Pass Header) for uri: {0}!", message.getUri());
					}
					sendHttpError("400 Bad request", message.getUri());
					// disconnect();
					return;
				}

				String name = message.getHeader("Sgs-User");
				String pass = message.getHeader("Sgs-Pass");
				logger.log(Level.INFO, "user {0} pass {1}", name, pass);

				try {
					identity = acceptor.authenticate(name, pass);
				} catch (Exception e) {
					logger.logThrow(Level.FINEST, e, "login authentication failed for name:{0}", name);
					sendHttpError("403 FORBIDDEN", message.getUri());
					// disconnect();
					return;
				}

				logger.log(Level.INFO, "Identity is {0}", identity);

				// The client trying to login create
				// a new session for this client:
				clientSession = new HttpClientSession(acceptor);
				protocolListener.newLogin(identity, clientSession, clientSession);
				clientSession.addClientConnection(HttpClientConnection.this);
				return;
			}

			String session = message.getHeader("Sgs-Session");

			// Test if session is valid:
			if (!acceptor.isValidSession(session)) {
				if (logger.isLoggable(Level.FINE)) {
					logger.log(Level.FINE, "Got invalid session {0}", session);
				}
				sendHttpError("400 Bad request", message.getUri());
				// disconnect();
				return;
			}

			// Get session an forward request to it:
			if (logger.isLoggable(Level.FINE)) {
				logger.log(Level.FINE, "Search client session for key {0}", session);
			}
			clientSession = acceptor.getSessionHandler(session);
			clientSession.addClientConnection(HttpClientConnection.this);
			clientSession.handleIncommingMessage(op, message);
		}

		private String readFile(File f) {
			if (!f.exists() || !f.canRead() || !f.isFile()) {
				throw new IllegalArgumentException("Could not acces file " + f);
			}
			FileInputStream fis;
			try {
				fis = new FileInputStream(f);
			} catch (FileNotFoundException e) {
				throw new IllegalArgumentException("Could not open file!");
			}

			int bytes = 0;
			StringBuffer sb = new StringBuffer();
			try {
				while ((bytes = fis.read()) != -1) {
					sb.append((char) bytes);
				}
			} catch (IOException e) {
				throw new IllegalArgumentException("Could not read filde");
			}

			return sb.toString();
		}
	}
}
