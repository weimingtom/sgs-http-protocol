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

import com.sun.sgs.protocol.ProtocolDescriptor;
import com.sun.sgs.transport.TransportDescriptor;
import java.io.Serializable;

/**
 * A protocol descriptor with an underlying transport descriptor. A
 * protocol may use this class directly or extend it, overriding methods
 * for protocol and/or transport-specific needs.
 */
public class SgsHttpProtocolDescriptor
    implements ProtocolDescriptor, Serializable
{

    private static final long serialVersionUID = 1L;

    /** The transport descriptor for this protocol. */
    protected final TransportDescriptor transportDesc;   
        
    /**
     * Constructs an instance with the specified transport descriptor.
     *
     * @param	transportDesc transport descriptor
     */
    public SgsHttpProtocolDescriptor(TransportDescriptor transportDesc) {
        if (transportDesc == null) {
            throw new NullPointerException("null transportDesc");
        }
        this.transportDesc = transportDesc;
    }

    /** {@inheritDoc}
     *
     * <p>This implementation returns {@code true} if the specified {@code
     * descriptor} is an instance of this class and this descriptor's
     * underlying transport descriptor is compatible with the specified
     * {@code descriptor}'s transport descriptor.
     */
    public boolean supportsProtocol(ProtocolDescriptor descriptor) {
        if (!(descriptor instanceof SgsHttpProtocolDescriptor)) {
            return false;
	}
        
        SgsHttpProtocolDescriptor desc =
	    (SgsHttpProtocolDescriptor) descriptor;
        
        return transportDesc.supportsTransport(desc.transportDesc);
    }

    /**
     * Return the protocol specific connection data as a byte array. The data
     * can be used by a client to connect to a server. The format of the data
     * may be dependent on the transport configured with this protocol.
     * @return the connection data
     */
    public byte[] getConnectionData() {
	return transportDesc.getConnectionData();
    }

    /**
     * Returns a string representation of this descriptor.
     *
     * @return	a string representation of this descriptor
     */
    public String toString() {
	return "SimpleSgsProtocolDescriptor[" + transportDesc.toString() + "]";
    }
}
