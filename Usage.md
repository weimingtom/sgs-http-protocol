# Introduction #

Add the jar file into the deploy folder of your darkstar installation this will enable the protocol. To use the protocol in your application add the following line to your app properties:

_com.sun.sgs.impl.service.session.protocol.acceptor=ch.vmac.sgs.protocol.http.SgsHttpProtocolAcceptor_

Currently the protocol can also be used as a simple http server. Under serves the files in the working directory of ds under the following url: http://localhost:1139/d/FILENAME. If filename is missing it tries to serve an _index.html_ file if available.

**This mode is intended to help developing client code for this protocol and should not be used in production code. Use a proper setup and hide the sgs-server behind an appache or another webserver.**