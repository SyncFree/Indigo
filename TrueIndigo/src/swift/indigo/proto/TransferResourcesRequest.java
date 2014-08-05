package swift.indigo.proto;

import java.util.Collection;

import swift.clocks.Timestamp;
import swift.indigo.ReservationsProtocolHandler;
import swift.indigo.ResourceRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class TransferResourcesRequest extends AcquireResourcesRequest {

    private String destinationId;

    public TransferResourcesRequest() {

    }

    public TransferResourcesRequest(String requesterId, String destinationId, Timestamp cltTimestamp,
            Collection<ResourceRequest<?>> resources) {
        super(requesterId, cltTimestamp, resources);
        this.destinationId = destinationId;
    }

    public TransferResourcesRequest(String destinationId, AcquireResourcesRequest request) {
        super(request);
        this.destinationId = destinationId;
    }

    @Override
    public void deliverTo(Envelope handle, MessageHandler handler) {
        ((ReservationsProtocolHandler) handler).onReceive(handle, this);

    }

    public String toString() {
        return String.format("Transfer request to: %s, TS:%s ----> %s", destinationId, super.clientTs,
                super.requests.toString());
    }

    public String getDestination() {
        return destinationId;
    }

}
