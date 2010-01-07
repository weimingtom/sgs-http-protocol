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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.sgs.app.Delivery;
import com.sun.sgs.impl.sharedutil.HexDumper;
import com.sun.sgs.impl.sharedutil.LoggerWrapper;
import com.sun.sgs.protocol.LoginFailureException;
import com.sun.sgs.protocol.LoginRedirectException;
import com.sun.sgs.protocol.ProtocolDescriptor;
import com.sun.sgs.protocol.RequestCompletionHandler;
import com.sun.sgs.protocol.SessionProtocol;
import com.sun.sgs.protocol.SessionProtocolHandler;

/**
 * This class manages the session of one client. The session can have none, one
 * or two connections at the same time. If no actions happens for a specific
 * time this session will be invalidated which make a client logout.
 * 
 * Data from the server will get collected in a queue an will be sent to the client
 * if an connection is available.
 * 
 * @author Christiaan Verbree
 * 
 */
public class HttpClientSession implements SessionProtocol, RequestCompletionHandler<SessionProtocolHandler> {
	private static final LoggerWrapper logger = new LoggerWrapper(Logger.getLogger(HttpClientSession.class.getName()));

	private static final Random random = new Random();

	/**
	 * If the client has nothing to send to the server but we need to create a
	 * new long polling connection.
	 */
	private final static byte NOP = 0;

	/**
	 * Login request from a client to a server. This message should only be sent
	 * to a server; if received by a client it should be ignored. <br>
	 * Opcode: {@code 0x10} <br>
	 * Payload:
	 * <ul>
	 * <li>(byte) protocol version
	 * <li>(String) name
	 * <li>(String) password
	 * </ul>
	 * The {@code protocol version} will be checked by the server to insure that
	 * the client and server are using the same protocol or versions of the
	 * protocol that are compatible. If the server determines that the protocol
	 * version used by the sender and the protocol version or versions required
	 * by the server are not compatible, the server will disconnect the client.
	 * In cases where the protocols being used are not compatible, no other
	 * communication between the client and the server is guaranteed to be
	 * understood.
	 * <p>
	 * The {@code name} and {@code password} strings are passed to the server's
	 * authentication mechanism. After the server processes the login request,
	 * the server sends one of the following acknowledgments to the client:
	 * <ul>
	 * <li> {@link #LOGIN_SUCCESS}, if user authentication succeeds and invoking
	 * the {@code loggedIn}' method on the application's {@code AppListener}
	 * with the user's {@code ClientSession} returns a non-null, serializable
	 * {@code ClientSessionListener};
	 * <li>{@link #LOGIN_REDIRECT}, if user authentication succeeds, but the
	 * server requests that the client redirect the login request to another
	 * node; or
	 * <li>{@link #LOGIN_FAILURE}, if user authentication fails, or if the user
	 * is already logged in and the server is configured to reject new logins
	 * for the same user, or if invoking the {@code loggedIn} method on the
	 * application's {@code AppListener} with the user's {@code ClientSession}
	 * returns a null, or non-serializable {@code ClientSessionListener} or the
	 * method does not complete successfully.
	 * </ul>
	 * <p>
	 * If a client is currently logged in, the result of receiving a
	 * LOGIN_REQUEST is not defined by the protocol, but is an
	 * implementation-dependent detail of the server.
	 */
	public final static byte LOGIN_REQUEST = 1;

	/**
	 * Login success. Server response to a client's {@link #LOGIN_REQUEST}. <br>
	 * Opcode: {@code 0x11} <br>
	 * Payload:
	 * <ul>
	 * <li>(ByteArray) reconnectionKey
	 * </ul>
	 * The {@code reconnectionKey} is an opaque reference that can be held by
	 * the client for use in case the client is disconnected and wishes to
	 * reconnect to the server with the same identity using a
	 * {@link #RECONNECT_REQUEST}.
	 */
	public static final byte LOGIN_SUCCESS = 11;

	/**
	 * Login failure. Server response to a client's {@link #LOGIN_REQUEST}. <br>
	 * Opcode: {@code 0x12} <br>
	 * Payload:
	 * <ul>
	 * <li>(String) reason
	 * </ul>
	 * This message indicates that the server rejects the {@link #LOGIN_REQUEST}
	 * for some reason, for example
	 * <ul>
	 * <li>user authentication failure,
	 * <li>failure during application processing of the client session, or
	 * <li>a user with the same identity is already logged in, and the server is
	 * configured to reject new logins for clients who are currently logged in
	 * </ul>
	 * 
	 */
	public static final byte LOGIN_FAILURE = 12;

	/**
	 * Login redirect. Server response to a client's {@link #LOGIN_REQUEST}. <br>
	 * Opcode: {@code 0x13} <br>
	 * Payload:
	 * <ul>
	 * <li>(String) hostname
	 * <li>(int) port
	 * </ul>
	 * This message indicates a redirection from the node to which the
	 * {@link #LOGIN_REQUEST} was sent to another node. The client receiving
	 * this request should shut down the connection to the original node and
	 * establish a connection to the node indicated by the {@code hostname} and
	 * {@code port} in the payload. The client should then attempt to log in to
	 * the node to which it has been redirected by sending a
	 * {@link #LOGIN_REQUEST} to that node.
	 */
	public static final byte LOGIN_REDIRECT = 13;

	/**
	 * Reconnection request. Client requesting reconnect to a server. <br>
	 * Opcode: {@code 0x20} <br>
	 * Payload:
	 * <ul>
	 * <li>(byte) protocol version
	 * <li>(ByteArray) reconnectionKey
	 * </ul>
	 * This message requests that the client be reconnected to an existing
	 * client session with the server. The {@code reconnectionKey} must match
	 * the one that the client received in the previous {@link #LOGIN_SUCCESS}
	 * or {@link #RECONNECT_SUCCESS} message (if reconnection was performed
	 * subsequent to login). If reconnection is successful, the server
	 * acknowledges the request with a {@link #RECONNECT_SUCCESS} message
	 * containing a new {@code reconnectionKey}. If reconnection is not
	 * successful, a {@link #RECONNECT_FAILURE} message is sent to the client.
	 * If the client receives a {@code RECONNECT_FAILURE} message, the client
	 * should disconnect from the server.
	 */
	public static final byte RECONNECT_REQUEST = 5;

	/**
	 * Reconnect success. Server response to a client's
	 * {@link #RECONNECT_REQUEST}. <br>
	 * Opcode: {@code 0x21} <br>
	 * Payload:
	 * <ul>
	 * <li>(ByteArray) reconnectionKey
	 * </ul>
	 * Indicates that a {@link #RECONNECT_REQUEST} has been successful. The
	 * message will include a {@code reconnectionKey} that can be used in a
	 * subsequent reconnect requests from the client. Reciept of this message
	 * indicates that the client session has been re-established.
	 */
	public static final byte RECONNECT_SUCCESS = 14;

	/**
	 * Reconnect failure. Server response to a client's
	 * {@link #RECONNECT_REQUEST}. <br>
	 * Opcode: {@code 0x22} <br>
	 * Payload:
	 * <ul>
	 * <li>(String) reason
	 * </ul>
	 * This response indicates that a reconnect request could not be honored by
	 * the server. This could be because of an invalid reconnect key, or because
	 * too much time has elapsed between the session disconnection and the
	 * reconnect request (which, in turn, may cause the server to discard the
	 * session state). The string returned details the reason for the denial of
	 * reconnection.
	 */
	public static final byte RECONNECT_FAILURE = 15;

	/**
	 * Session message. May be sent by the client or the server. Maximum length
	 * is {@value #MAX_PAYLOAD_LENGTH} bytes. Larger messages require
	 * fragmentation and reassembly above this protocol layer. <br>
	 * Opcode: {@code 0x30} <br>
	 * Payload:
	 * <ul>
	 * <li>(ByteArray) message
	 * </ul>
	 * This message allows information to be sent between the client and the
	 * server. The content of the message is application dependent, and the
	 * mechanisms for constructing and parsing these messages is an
	 * application-level task.
	 */
	public final static byte SESSION_MESSAGE = 3;

	/**
	 * Logout request from a client to a server. <br>
	 * Opcode: {@code 0x40} <br>
	 * No payload. <br>
	 * This message will cause the client to be logged out of the server. The
	 * server will remove all of the client's channel memberships. Any message
	 * (other than {@link #LOGIN_REQUEST}) sent by the client after sending this
	 * message will be ignored, and any message will need to be sent on a new
	 * connection to the server.
	 */
	public final static byte LOGOUT_REQUEST = 2;

	/**
	 * Logout success. Server response to a client's {@link #LOGOUT_REQUEST}. <br>
	 * Opcode: {@code 0x41} <br>
	 * No payload. <br>
	 * This message is sent from the server to the client to indicate that a
	 * {@link #LOGOUT_REQUEST} has been received and that the client has been
	 * logged out of the current session. On receipt of this message, the client
	 * should shut down any networking resources that are used to communicate
	 * with the server.
	 */
	public static final byte LOGOUT_SUCCESS = 99;

	/**
	 * Channel join. Server notifying a client that it has joined a channel. <br>
	 * Opcode: {@code 0x50} <br>
	 * Payload:
	 * <ul>
	 * <li>(String) channel name
	 * <li>(ByteArray) channel ID
	 * </ul>
	 * This message is sent from the server to the client to indicate that the
	 * client has been added to the channel identified by the {@code channel
	 * name} and {@code channel ID} contained in the message.
	 */
	public static final byte CHANNEL_JOIN = 50;

	/**
	 * Channel leave. Server notifying a client that the client has left a
	 * channel. <br>
	 * Opcode: {@code 0x51} <br>
	 * Payload:
	 * <ul>
	 * <li>(ByteArray) channel ID
	 * </ul>
	 * This message is sent from the server indicating to the client that the
	 * client has been removed from the channel with the indicated {@code
	 * channel ID}. The client can no longer send messages on the channel.
	 */
	public static final byte CHANNEL_LEAVE = 51;

	/**
	 * Channel message. May be sent by the client or the server. Maximum length
	 * is {@value #MAX_PAYLOAD_LENGTH} bytes minus the sum of the {@code channel
	 * ID} size and two bytes (the size of the unsigned short indicating the
	 * {@code channel Id} size). Larger messages require fragmentation and
	 * reassembly above this protocol layer. <br>
	 * Opcode: {@code 0x52} <br>
	 * Payload:
	 * <ul>
	 * <li>(unsigned short) channel ID size
	 * <li>(ByteArray) channel ID
	 * <li>(ByteArray) message
	 * </ul>
	 * This message requests that the specified message be sent to all members
	 * of the specified channel. If the client sending the request is not a
	 * member of the channel, the message will be rejected by the server. The
	 * server may also refuse to send the message, or alter the message, because
	 * of application-specific logic.
	 */
	public final static byte CHANNEL_MESSAGE = 4;

	/**
	 * The maximum length of a protocol message: {@value #MAX_MESSAGE_LENGTH}
	 * bytes.
	 */
	public static final int MAX_MESSAGE_LENGTH = 65535;

	/** The set of supported delivery requirements. */
	protected final Set<Delivery> deliverySet = new HashSet<Delivery>();

	private final SgsHttpProtocolAcceptor acceptor;
	private final String sessionKey;

	private SessionProtocolHandler protocolHandler;

	private HttpClientConnection activeConnection;

	/** Messages enqueued to be sent after a login ack is sent. */
	private List<OutgoingMessage> messageQueue = new ArrayList<OutgoingMessage>();

	/** A lock for {@code loginHandled} and {@code messageQueue} fields. */
	private Object lock = new Object();

	/** Indicates whether the client's login ack has been sent. */
	private boolean loginHandled = false;

	private long lastCientAction;

	/**
	 * @param acceptor
	 * @param listener
	 */
	public HttpClientSession(SgsHttpProtocolAcceptor acceptor) {
		this.acceptor = acceptor;
		this.sessionKey = generateSessionKey();

		deliverySet.add(Delivery.RELIABLE);
		logger.log(Level.FINE, "SessionKey: {0}", sessionKey);
		acceptor.addSessionHandler(sessionKey, this);

		lastCientAction = System.currentTimeMillis();
	}

	/**
	 * @return the sessionKey
	 */
	public String getSessionKey() {
		return sessionKey;
	}

	public void addClientConnection(HttpClientConnection clientConnection) {
		synchronized (lock) {
			if (activeConnection != null) {
				if (loginHandled && messageQueue.size() > 0) {
					// we can only flush when logged in
					// otherwise it is possible that the order
					// of messages get changed.
					flushMessageQueue();
				} else {
					closeActiveConnection();
				}
			}
			activeConnection = clientConnection;
			if (loginHandled) {
				flushMessageQueue();
			}
		}
	}

	private void closeActiveConnection() {
		synchronized (lock) {
			if (activeConnection != null) {
				// close active connection with empty data (no error)
				ByteBuffer bb = ByteBuffer.wrap("[]".getBytes());
				activeConnection.writeAndCloseConnection(bb);
			}
		}
	}

	public void handleIncommingMessage(byte operation, HttpMessage message) {

		lastCientAction = System.currentTimeMillis();

		if (logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "handle message with opcode: {0}", operation);
		}
		switch (operation) {
		case NOP:
			break;
		case LOGOUT_REQUEST:
			acceptor.removeSessionHandler(sessionKey);
			protocolHandler.logoutRequest(new LogoutHandler());
			break;
		case SESSION_MESSAGE:
			try {
				byte[] d = Base64.decode(message.getHeader("Sgs-Data"));
				logger.log(Level.FINE, "Got session msgs decoded is: {0}", HexDumper.format(ByteBuffer.wrap(d), 0));
				protocolHandler.sessionMessage(ByteBuffer.wrap(d), new RequestHandler());
			} catch (IOException e) {
				if (logger.isLoggable(Level.FINE)) {
					logger.logThrow(Level.FINE, e, "Invalid base64 data!");
				}
			}
			break;
		case CHANNEL_MESSAGE:
			if (!message.hasHeader("Sgs-Channel")) {
				if (logger.isLoggable(Level.FINE)) {
					logger.log(Level.FINE, "Got invalid channel message id missing");
				}
				break;
			}
			String channelId = message.getHeader("Sgs-Channel");
			if (channelId.length() == 0) {
				if (logger.isLoggable(Level.FINE)) {
					logger.log(Level.FINE, "Got invalid channel message id is empty");
				}
				break;
			}
			BigInteger channelRefId = new BigInteger(channelId);
			try {
				byte[] d = Base64.decode(message.getHeader("Sgs-Data"));
				protocolHandler.channelMessage(channelRefId, ByteBuffer.wrap(d), new RequestHandler());
			} catch (IOException e) {
				if (logger.isLoggable(Level.FINE)) {
					logger.logThrow(Level.FINE, e, "Invalid base64 data!");
				}
			}
			break;
		default:
			logger.log(Level.FINE, "Unknown opCode: {0}", operation);
		}
	}

	public long getLastClientAction() {
		return lastCientAction;
	}

	@Override
	public void channelJoin(String name, BigInteger channelId, Delivery delivery) throws IOException {
		JsonMessage jm = new JsonMessage(CHANNEL_JOIN);
		jm.addData("s", sessionKey);
		jm.addData("c", name);
		jm.addData("i", channelId.toString());
		if (logger.isLoggable(Level.FINER)) {
			logger.log(Level.FINER, "Json: {0}", jm.toJson());
		}
		writeOrEnqueue(jm);
	}

	@Override
	public void channelLeave(BigInteger channelId) throws IOException {
		JsonMessage jm = new JsonMessage(CHANNEL_LEAVE);
		jm.addData("s", sessionKey);
		jm.addData("i", channelId.toString());
		if (logger.isLoggable(Level.FINER)) {
			logger.log(Level.FINER, "Json: {0}", jm.toJson());
		}
		writeOrEnqueue(jm);
	}

	@Override
	public void channelMessage(BigInteger channelId, ByteBuffer message, Delivery delivery) throws IOException {
		JsonMessage jm = new JsonMessage(CHANNEL_MESSAGE);
		jm.addData("i", channelId.toString());
		jm.addData("b", Base64.encodeBytes(message.array()));
		if (logger.isLoggable(Level.FINER)) {
			logger.log(Level.FINER, "Json: {0}", jm.toJson());
		}
		writeOrEnqueue(jm);
	}

	@Override
	public void sessionMessage(ByteBuffer message, Delivery delivery) throws IOException {
		OutgoingMessage om = new OutgoingMessage(SESSION_MESSAGE, message);
		if (logger.isLoggable(Level.FINER)) {
			logger.log(Level.FINER, "Json: {0}", om.toJson());
		}
		writeOrEnqueue(om);
	}

	@Override
	public void disconnect(DisconnectReason reason) throws IOException {
		close();
	}

	@Override
	public Set<Delivery> getDeliveries() {
		return Collections.unmodifiableSet(deliverySet);
	}

	@Override
	public int getMaxMessageLength() {
		// largest message size is max for channel messages
		return HttpClientSession.MAX_MESSAGE_LENGTH - 1 - // Opcode
		2 - // channel ID size
		8; // (max) channel ID bytes
	}

	@Override
	public void close() throws IOException {
		if (logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "Close session {0} has active connection: {1}", sessionKey, activeConnection != null);
		}
		closeActiveConnection();
		activeConnection = null;
	}

	@Override
	public boolean isOpen() {
		return activeConnection.isOpen();
	}

	public void sessionTimeout() {
		logger.log(Level.INFO, "Session {0} has timed out. Last action was on: {1,number,#}", sessionKey, lastCientAction);
		if (protocolHandler != null) {
			protocolHandler.logoutRequest(new LogoutHandler());
		}
		try {
			close();
		} catch (IOException e) {
			if (logger.isLoggable(Level.FINE)) {
				logger.logThrow(Level.FINE, e, "Exception while closing session");
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <p>
	 * This implementation invokes the {@code get} method on the specified
	 * {@code future} to obtain the session's protocol handler.
	 * 
	 * <p>
	 * If the login request completed successfully (without throwing an
	 * exception), it sends a login success message to the client.
	 * 
	 * <p>
	 * Otherwise, if the {@code get} invocation throws an {@code
	 * ExecutionException} and the exception's cause is a
	 * {@link LoginRedirectException}, it sends a login redirect message to the
	 * client with the redirection information obtained from the exception. If
	 * the {@code ExecutionException}'s cause is a {@link LoginFailureException}
	 * , it sends a login failure message to the client.
	 * 
	 * <p>
	 * If the {@code get} method throws an exception other than {@code
	 * ExecutionException}, or the {@code ExecutionException}'s cause is not
	 * either a {@code LoginFailureException} or a {@code
	 * LoginRedirectException}, then a login failed message is sent to the
	 * client.
	 */
	@Override
	public void completed(Future<SessionProtocolHandler> future) {
		try {
			protocolHandler = future.get();
			loginSuccess();
		} catch (ExecutionException e) {
			// login failed
			Throwable cause = e.getCause();
			if (cause instanceof LoginRedirectException) {
				// redirect
				LoginRedirectException redirectException = (LoginRedirectException) cause;
				loginRedirect(redirectException.getNodeId(), redirectException.getProtocolDescriptors());
			} else if (cause instanceof LoginFailureException) {
				loginFailure(cause.getMessage(), cause.getCause());
			} else {
				loginFailure(e.getMessage(), e.getCause());
			}
		} catch (Exception e) {
			loginFailure(e.getMessage(), e.getCause());
		}
	}

	protected String generateSessionKey() {
		return "" + HttpClientSession.random.nextInt(1000);
	}

	/**
	 * Notifies the associated client that the previous login attempt was
	 * successful.
	 */
	private void loginSuccess() {
		synchronized (lock) {
			JsonMessage jm = new JsonMessage(LOGIN_SUCCESS);
			jm.addData("s", sessionKey);
			if (logger.isLoggable(Level.FINER)) {
				logger.log(Level.FINER, "Json: {0}", jm.toJson());
			}
			loginHandled = true;
			writeOrEnqueue(jm, true);
		}
	}

	/**
	 * Notifies the associated client that it should redirect its login to the
	 * specified {@code node} with the specified protocol {@code descriptors}.
	 * 
	 * @param nodeId
	 *            the ID of the node to redirect the login
	 * @param descriptors
	 *            a set of protocol descriptors supported by {@code node}
	 */
	private void loginRedirect(long nodeId, Set<ProtocolDescriptor> descriptors) {
		for (ProtocolDescriptor descriptor : descriptors) {
			if (acceptor.getDescriptor().supportsProtocol(descriptor)) {
				JsonMessage jm = new JsonMessage(LOGIN_REDIRECT);
				jm.addData("s", sessionKey);
				jm.addData("r", new String(((SgsHttpProtocolDescriptor) descriptor).getConnectionData()));
				if (logger.isLoggable(Level.FINER)) {
					logger.log(Level.FINER, "Json: {0}", jm.toJson());
				}
				writeOrEnqueue(jm);
				acceptor.monitorDisconnection(this);
				return;
			}
		}
		loginFailure("redirect failed", null);
		logger.log(Level.SEVERE, "redirect node {0} does not support a compatable protocol", nodeId);
	}

	/**
	 * Notifies the associated client that the previous login attempt was
	 * unsuccessful for the specified {@code reason}. The specified {@code
	 * throwable}, if non-{@code null} is an exception that occurred while
	 * processing the login request. The message channel should be careful not
	 * to reveal to the associated client sensitive data that may be present in
	 * the specified {@code throwable}.
	 * 
	 * @param reason
	 *            a reason why the login was unsuccessful
	 * @param throwable
	 *            an exception that occurred while processing the login request,
	 *            or {@code null}
	 */
	private void loginFailure(String reason, Throwable ignore) {
		JsonMessage jm = new JsonMessage(LOGIN_FAILURE);
		jm.addData("s", sessionKey);
		jm.addData("r", reason);
		if (logger.isLoggable(Level.FINER)) {
			logger.log(Level.FINER, "Json: {0}", jm.toJson());
		}
		writeOrEnqueue(jm, true);
		flushMessageQueue(); // we need this to flush the message to the client!
		// acceptor.monitorDisconnection(this);
	}

	/**
	 * Notifies the associated client that it has successfully logged out.
	 */
	private void logoutSuccess() {
		JsonMessage jm = new JsonMessage(LOGOUT_SUCCESS);
		jm.addData("s", sessionKey);
		if (logger.isLoggable(Level.FINER)) {
			logger.log(Level.FINER, "Json: {0}", jm.toJson());
		}
		writeOrEnqueue(jm, true);
		acceptor.monitorDisconnection(this);
	}

	private void writeOrEnqueue(OutgoingMessage message) {
		writeOrEnqueue(message, false);
	}

	private void writeOrEnqueue(OutgoingMessage message, boolean importantMessage) {
		synchronized (lock) {
			if (importantMessage) {
				messageQueue.add(0, message);
			} else {
				messageQueue.add(message);
			}
			logger.log(Level.FINER, "add message {0} to queue size is {1} activeConnection: {2} loginHandled: {3}", message.toJson(), messageQueue
					.size(), activeConnection, loginHandled);
			if (activeConnection != null && loginHandled) {
				flushMessageQueue();
			}
		}
	}

	/**
	 * Writes all enqueued messages to the write handler.
	 */
	private void flushMessageQueue() {
		synchronized (lock) {
			if (messageQueue.size() == 0) {
				logger.log(Level.FINER, "queue is empty");
				return;
			}

			if (activeConnection == null) {
				logger.log(Level.FINE, "Cannot flush messages because no active connetion is found!");
				return;
			}

			logger.log(Level.FINER, "flushing {0} messages to client.", messageQueue.size());
			StringBuffer sb = new StringBuffer();
			sb.append("[");
			boolean isFirst = true;
			for (OutgoingMessage nextMessage : messageQueue) {
				if (!isFirst) {
					sb.append(',');
				}
				isFirst = false;
				sb.append(nextMessage.toJson());
			}
			sb.append("]");
			messageQueue.clear();

			logger.log(Level.FINER, "Json is: {0}", sb);

			ByteBuffer bb = ByteBuffer.wrap(sb.toString().getBytes());

			// Send data to client:
			try {
				activeConnection.writeAndCloseConnection(bb);
				// the active connection is now used clear it:
				activeConnection = null;
			} catch (RuntimeException e) {
				if (logger.isLoggable(Level.WARNING)) {
					logger.logThrow(Level.WARNING, e, "thrown when sending data for protocol:{0}", this);
				}
			}
		}

	}

	private class OutgoingMessage {
		protected byte opcode;
		private ByteBuffer body;

		public OutgoingMessage(byte opcode, ByteBuffer body) {
			this.opcode = opcode;
			this.body = body;
		}

		public OutgoingMessage(byte opcode, String body) {
			this(opcode, ByteBuffer.wrap(body.getBytes()));
		}

		public OutgoingMessage(byte opcode) {
			this(opcode, (ByteBuffer) null);
		}

		public StringBuffer toJson() {
			StringBuffer sb = new StringBuffer();

			sb.append("{o:" + opcode);
			if (body != null) {
				sb.append(",b: \"");
				// sb.append(new String(body.array()));
				sb.append(Base64.encodeBytes(body.array()));
				sb.append("\"}");
			} else {
				sb.append("}");
			}
			return sb;
		}
	}

	private class JsonMessage extends OutgoingMessage {
		private Map<String, String> data = new HashMap<String, String>();

		public JsonMessage(byte opcode) {
			super(opcode);
		}

		public void addData(String key, String value) {
			data.put(key, value);
		}

		@Override
		public StringBuffer toJson() {
			StringBuffer sb = new StringBuffer();
			sb.append('{');
			sb.append('o');
			sb.append(':');
			sb.append(opcode);

			for (Entry<String, String> e : data.entrySet()) {
				sb.append(',');
				sb.append(e.getKey());
				sb.append(':');
				sb.append('"');
				sb.append(e.getValue());
				sb.append('"');
			}
			sb.append('}');
			return sb;
		}
	}

	/**
	 * A completion handler that is notified when its associated request has
	 * completed processing.
	 */
	private class RequestHandler implements RequestCompletionHandler<Void> {

		/**
		 * {@inheritDoc}
		 * 
		 * <p>
		 * This implementation schedules a task to resume reading.
		 */
		public void completed(Future<Void> future) {
			try {
				future.get();
			} catch (Exception e) {
				if (logger.isLoggable(Level.WARNING)) {
					logger.logThrow(Level.WARNING, e, "Obtaining request result throws ");
				}
			}
			// scheduleReadOnReadHandler();
		}
	}

	/**
	 * A completion handler that is notified when the associated logout request
	 * has completed processing.
	 */
	private class LogoutHandler implements RequestCompletionHandler<Void> {

		/**
		 * {@inheritDoc}
		 * 
		 * <p>
		 * This implementation sends a logout success message to the client .
		 */
		public void completed(Future<Void> future) {
			try {
				future.get();
			} catch (Exception e) {
				if (logger.isLoggable(Level.WARNING)) {
					logger.logThrow(Level.WARNING, e, "Obtaining logout result throws ");
				}
			}
			logoutSuccess();
		}
	}

}
