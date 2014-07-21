package swift.indigo.proto;

import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class RequestWithReply extends ClientRequest {

    private Envelope handle;
    private ClientRequest request;

    public RequestWithReply(Envelope handle, ClientRequest request) {
        this.request = request;
        this.handle = handle;
    }

    public ClientRequest getRequest() {
        return request;
    }

    public void setRequest(ClientRequest request) {
        this.request = request;
    }

    @Override
    public void deliverTo(Envelope handle, MessageHandler handler) {
        System.out.println("NOT IMPLEMENTED");
        System.exit(0);

    }

    public Envelope getHandle() {
        return handle;
    }

    public void setHandle(Envelope handle) {
        this.handle = handle;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RequestWithReply) {
            if (this.request.equals(((RequestWithReply) o).request))
                return true;
        }
        return false;
    }

    public String toString() {
        return "RequestWithReply: " + request;
    }
}
