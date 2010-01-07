/*
 * Copyright 2010 C. Verbree.
 *
 * This file is part of sgs-http-protocol. And is based in part of the
 * simple sgs protocol from project darkstar.
 *
 * This file is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation and
 * distributed hereunder to you.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package ch.vmac.sgs.protocol.http;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.vmac.sgs.protocol.http.HttpMessage.HttpMethod;

import com.sun.sgs.impl.nio.DelegatingCompletionHandler;
import com.sun.sgs.impl.sharedutil.LoggerWrapper;
import com.sun.sgs.nio.channels.AsynchronousByteChannel;
import com.sun.sgs.nio.channels.CompletionHandler;
import com.sun.sgs.nio.channels.IoFuture;
import com.sun.sgs.nio.channels.ReadPendingException;
import com.sun.sgs.nio.channels.WritePendingException;

/**
 * A wrapper channel that reads and writes complete messages by framing messages
 * with a 2-byte message length, and masking (and re-issuing) partial I/O
 * operations. Also enforces a fixed buffer size when reading.
 */
public class AsynchronousMessageChannel implements Channel {

	/** The logger for this class. */
	static final LoggerWrapper logger = new LoggerWrapper(Logger.getLogger(AsynchronousMessageChannel.class.getName()));

	/**
	 * The underlying channel (possibly another layer of abstraction, e.g.
	 * compression, retransmission...)
	 */
	final AsynchronousByteChannel channel;

	/** Whether there is a read underway. */
	final AtomicBoolean readPending = new AtomicBoolean();

	/** Whether there is a write underway. */
	final AtomicBoolean writePending = new AtomicBoolean();

	/** The read buffer. */
	final ByteBuffer readBuffer;

	private HttpMessage message;
	
	/**
	 * Creates a new instance of this class with the given channel and read
	 * buffer size.
	 * 
	 * @param channel
	 *            a channel
	 * @param readBufferSize
	 *            the number of bytes in the read buffer
	 * @throws IllegalArgumentException
	 *             if {@code readBufferSize} is smaller than
	 *             {@value #PREFIX_LENGTH}
	 */
	public AsynchronousMessageChannel(AsynchronousByteChannel channel, int readBufferSize) {
		this.channel = channel;
		readBuffer = ByteBuffer.allocateDirect(readBufferSize);
	}

	/**
	 * Initiates reading a complete message from this channel. Returns a future
	 * which will contain a read-only view of a buffer containing the complete
	 * message. Calls {@code handler} when the read operation has completed, if
	 * {@code handler} is not {@code null}. The buffer's position will be set to
	 * {@code 0} and it's limit will be set to the length of the complete
	 * message. The contents of the buffer will remain valid until the next call
	 * to {@code read}.
	 * 
	 * @param handler
	 *            the completion handler object; can be {@code null}
	 * @return a future representing the result of the operation
	 * @throws BufferOverflowException
	 *             if the buffer does not contain enough space to read the next
	 *             message
	 * @throws ReadPendingException
	 *             if a read is in progress
	 */
	public IoFuture<HttpMessage, Void> read(CompletionHandler<HttpMessage, Void> handler) {
		if (!readPending.compareAndSet(false, true)) {
			throw new ReadPendingException();
		}
		return new Reader(handler).start();
	}

	/**
	 * Initiates writing a complete message from the given buffer to the
	 * underlying channel, and returns a future for controlling the operation.
	 * Writes bytes starting at the buffer's current position and up to its
	 * limit.
	 * 
	 * @param src
	 *            the buffer from which bytes are to be retrieved
	 * @param handler
	 *            the completion handler object; can be {@code null}
	 * @return a future representing the result of the operation
	 * @throws WritePendingException
	 *             if a write is in progress
	 */
	public IoFuture<Void, Void> write(ByteBuffer src, CompletionHandler<Void, Void> handler) {
		if (!writePending.compareAndSet(false, true)) {
			throw new WritePendingException();
		}
		return new Writer(handler, src).start();
	}
	
	/** {@inheritDoc} */
	@Override
	public void close() throws IOException {
		channel.close();
	}

	/** {@inheritDoc} */
	@Override
	public boolean isOpen() {
		return channel.isOpen();
	}

	/**
	 * Implement a completion handler for reading a complete message from the
	 * underlying byte stream.
	 */
	private final class Reader extends DelegatingCompletionHandler<HttpMessage, Void, Integer, Void> {
		/** The length of the message, or -1 if not yet known. */
		private int messageLen = -1;
		
		/** Creates an instance with the specified attachment and handler. */
		Reader(CompletionHandler<HttpMessage, Void> handler) {
			super(null, handler);
			message = new HttpMessage();
		}

		/** Clear the readPending flag. */
		@Override
		protected void done() {
			readPending.set(false);
			super.done();
		}

		/** Start reading into the buffer. */
		@Override
		protected IoFuture<Integer, Void> implStart() {
			return processBuffer();
		}

		/** Process the results of reading so far and read more if needed. */
		@Override
		protected IoFuture<Integer, Void> implCompleted(IoFuture<Integer, Void> result) throws ExecutionException, EOFException {
			int bytesRead = result.getNow();
			if (bytesRead < 0) {
				// Not sure what to do in this case
				// but I don't think we need the exception
				return null;
				//throw new EOFException("The message was incomplete");
			}
			return processBuffer();
		}

		/**
		 * Process the results of reading into the buffer, and return a future
		 * to read more if needed.
		 */
		private IoFuture<Integer, Void> processBuffer() {
			// Are we in header:
			if(messageLen < 0) {
				// Headers are line based consume only complete lines from buffer:
				if (logger.isLoggable(Level.FINER)) {
					logger.log(Level.FINER, "{0} header read incomplete {1}:{2} limit={3}", this, messageLen, readBuffer.position(), readBuffer.limit());
				}
				
				boolean eoh = consumeHeaderLines(readBuffer);
				if(eoh) {
					logger.log(Level.FINER, "Found end of header decide if we need to read more method {0}", message.getMethod());
					if(message.getMethod() == HttpMethod.GET) {
						set(message);
						return null;
					}
					
					// this is a post operation we need to read the body
					// TBD
					throw new IllegalArgumentException("POST handling missing!");
					
				}
			}
			else {
				// read body
			}
			
			// we havn't read all data yet:
			if (logger.isLoggable(Level.FINER)) {
				logger.log(Level.FINER, "{0} read incomplete {1}:{2} limit={3}", this, messageLen, readBuffer.position(), readBuffer.limit());
			}
			return channel.read(readBuffer, this);
		}
		
		private boolean consumeHeaderLines(ByteBuffer buffer) {
			buffer.flip();
			int startOfLine = 0;
			boolean foundHeaderEnd = false;
			byte b0 = 0;
			byte b1 = 0;
			byte b2 = 0;
			byte b3 = 0;
			StringBuffer line = new StringBuffer();
			for(int i = buffer.position(); i < buffer.limit();i++) {
				b3 = b2;
				b2 = b1;
				b1 = b0;
				b0 = buffer.get();
				
				//logger.log(Level.FINER, "Found i={0} {1} {2} {3}", b3, b2, b1, b0);
				if(b3 == 13 && b2 == 10 && b1 == 13 && b0 == 10) {
					startOfLine = i + 1;
					foundHeaderEnd = true;
					break;
				}
				else if(b1 == 13 && b0 == 10) {
					// end of line but only if the line is not empty
					if(line.length() == 0) {
						continue;
					}
					message.addHeader(line.toString());
					line = new StringBuffer();
					startOfLine = i + 1;
				}
				else if(b0 == 13 || b0 == 10) {
					// ignore this chars (we need at least one more char)
					// to decide what we want to do.
				}
				else {
					line.append((char) b0);
				}
			}
			
			// Clean read data from buffer
			if(startOfLine < buffer.limit()) {
				logger.log(Level.FINE, "need to compact sol is {0} limit is {1} position is {2}", startOfLine, buffer.limit(), buffer.position());
				buffer.position(startOfLine);
				buffer.compact();
				logger.log(Level.FINE, "after compact sol is {0} limit is {1} position is {2}", startOfLine, buffer.limit(), buffer.position());
			}
			else {
				logger.log(Level.FINE, "clear buffer sol is {0} limit is {1} position is {2}", startOfLine, buffer.limit(), buffer.position());
				buffer.clear();
			}
			
			return foundHeaderEnd;
		}
	}

	/**
	 * Implement a completion handler for writing a complete message to the
	 * underlying byte stream.
	 */
	private final class Writer extends DelegatingCompletionHandler<Void, Void, Integer, Void> {
		/**
		 * The byte buffer containing the bytes to send
		 */
		private final ByteBuffer srcWithSize;

		/**
		 * Creates an instance with the specified attachment and handler, and
		 * sending the bytes in the specified buffer.
		 */
		Writer(CompletionHandler<Void, Void> handler, ByteBuffer src) {
			super(null, handler);
			int size = src.remaining();
			assert size < Short.MAX_VALUE;

			srcWithSize = ByteBuffer.allocate(size);
			srcWithSize.put(src).flip();
		}

		/** Clear the writePending flag. */
		@Override
		protected void done() {
			writePending.set(false);
			super.done();
		}

		/** Start writing from the buffer. */
		@Override
		protected IoFuture<Integer, Void> implStart() {
			return channel.write(srcWithSize, this);
		}

		/** Process the results of writing so far and write more if needed. */
		@Override
		protected IoFuture<Integer, Void> implCompleted(IoFuture<Integer, Void> result) throws ExecutionException {
			/* See if computation already failed. */
			result.getNow();
			if (srcWithSize.hasRemaining()) {
				/* Write some more */
				return channel.write(srcWithSize, this);
			} else {
				/* Finished */
				return null;
			}
		}
	}
}
