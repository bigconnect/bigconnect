package com.mware.ge.cypher.builtin.proc.dbms;

import com.mware.ge.cypher.connection.NetworkConnectionTracker;
import com.mware.ge.cypher.connection.TrackedNetworkConnection;
import com.mware.ge.cypher.exception.AuthorizationViolationException;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.cypher.procedure.Context;
import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.cypher.procedure.Name;
import com.mware.ge.cypher.procedure.Procedure;
import com.mware.ge.cypher.security.SecurityContext;
import com.mware.ge.dependencies.DependencyResolver;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Stream;

import static com.mware.ge.cypher.procedure.Mode.DBMS;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class BuiltInDbmsProcedures {
    @Context
    public GeCypherQueryContext bcApi;

    @Context
    public DependencyResolver resolver;

    @Context
    public SecurityContext securityContext;

    @Description("List all accepted network connections at this instance that are visible to the user.")
    @Procedure(name = "dbms.listConnections", mode = DBMS)
    public Stream<ListConnectionResult> listConnections() {
        securityContext.assertCredentialsNotExpired();

        NetworkConnectionTracker connectionTracker = resolver.resolveDependency(NetworkConnectionTracker.class);
        ZoneId timeZone = ZoneOffset.UTC;

        return connectionTracker.activeConnections()
                .stream()
                .filter( connection -> isAdminOrSelf( connection.username() ) )
                .map(connection -> new ListConnectionResult(connection, timeZone));
    }

    @Description("Kill network connection with the given connection id.")
    @Procedure(name = "dbms.killConnection", mode = DBMS)
    public Stream<ConnectionTerminationResult> killConnection(@Name("id") String id) {
        return killConnections(singletonList(id));
    }

    @Description("Kill all network connections with the given connection ids.")
    @Procedure(name = "dbms.killConnections", mode = DBMS)
    public Stream<ConnectionTerminationResult> killConnections(@Name("ids") List<String> ids) {
        securityContext.assertCredentialsNotExpired();

        NetworkConnectionTracker connectionTracker = resolver.resolveDependency(NetworkConnectionTracker.class);
        return ids.stream().map(id -> killConnection(id, connectionTracker));
    }

    private ConnectionTerminationResult killConnection(String id, NetworkConnectionTracker connectionTracker) {
        TrackedNetworkConnection connection = connectionTracker.get(id);
        if (connection != null) {
            if (isAdminOrSelf(connection.username())) {
                connection.close();
                return new ConnectionTerminationResult(id, connection.username());
            }
            throw new AuthorizationViolationException(format("Executing admin procedure is not allowed for %s.", securityContext.description()));
        }
        return new ConnectionTerminationFailedResult(id);
    }

    private boolean isAdminOrSelf(String username) {
//        return securityContext.subject().hasUsername(username);
        return true;
    }

}
