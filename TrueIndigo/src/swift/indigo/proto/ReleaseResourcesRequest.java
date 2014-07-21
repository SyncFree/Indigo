package swift.indigo.proto;

import swift.clocks.Timestamp;
import swift.indigo.ReservationsProtocolHandler;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class ReleaseResourcesRequest extends ClientRequest {

    private Timestamp clientTs;
    private long serial;

    public ReleaseResourcesRequest() {
        super();
    }

    public ReleaseResourcesRequest(Timestamp clientTs) {
        super();
        this.clientTs = clientTs;
    }

    public ReleaseResourcesRequest(long serial, String clientId, Timestamp clientTs) {
        super(clientId);
        this.clientTs = clientTs;
        this.serial = serial;
    }

    @Override
    public void deliverTo(Envelope conn, MessageHandler handler) {
        ((ReservationsProtocolHandler) handler).onReceive(conn, this);

    }

    public Timestamp getClientTs() {
        return clientTs;
    }

    public String toString() {
        return String.format("%s)", clientTs);
    }

    public long serial() {
        return serial;
    }

}
