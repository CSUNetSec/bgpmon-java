package edu.colostate.netsec.session;

import java.util.Map;

import edu.colostate.netsec.BgpmonOuterClass;

public abstract class Session {
    protected String sessionId;
    protected Map<String, Session> sessions;

    public Session(String sessionId, Map<String,Session> sessions) {
        this.sessionId = sessionId;
        this.sessions = sessions;
    }    

    public void selfDestruct() {
        destroy();        
        sessions.remove(sessionId);
    }

    public abstract BgpmonOuterClass.Session getProtobufSession();
    public abstract void destroy();
}
