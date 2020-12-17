package example;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerPoliciesPlugin;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.procedure.builtin.routing.SingleInstanceGetRoutingTableProcedure.ADDRESS_CONTEXT_KEY;
import static org.neo4j.values.storable.Values.NO_VALUE;

@ServiceProvider
public class ServerSideRoutingPlugin extends ServerPoliciesPlugin
{
    @ServiceProvider
    public static class ServerSideRoutingPluginConfiguration implements SettingsDeclaration
    {
        // TODO: make this a list of regex and fail on startup if they don't parse
        @Description( "List of addresses or address patterns that allow client side routing" )
        public static Setting<List<String>> allow_client_side_routing_addresses =
                newBuilder( "dbms.allow_client_side_routing_addresses", listOf( STRING ), emptyList() ).dynamic().build();
    }

    private Config config;
    private TopologyService topologyService;

    @Override
    public void init( TopologyService topologyService, LeaderService leaderService, LogProvider logProvider, Config config )
    {
        super.init( topologyService, leaderService, logProvider, config );
        this.config = config;
        this.topologyService = topologyService;
    }

    public boolean isKnownAdvertisedAddress( SocketAddress address )
    {
        return isKnownByConfig( address ) ||
               Stream.concat( topologyService.allCoreServers().values().stream(), topologyService.allReadReplicas().values().stream() )
                     .map( s -> s.connectors().clientBoltAddress() )
                     .anyMatch( s -> s.equals( address ) );
    }

    private boolean isKnownByConfig( SocketAddress address )
    {
        return config.get( ServerSideRoutingPluginConfiguration.allow_client_side_routing_addresses ).stream()
                     .map( p -> p.replace( ".", "\\." ).replace("?", ".?").replace("*", ".*?") )
                     .anyMatch( pattern -> address.getHostname().matches( pattern ) || address.toString().matches( pattern ));
    }

    private long configuredRoutingTableTtl()
    {
        return config.get( GraphDatabaseSettings.routing_ttl ).toMillis();
    }

    private Optional<SocketAddress> findClientProvidedAddress( MapValue routingContext ) throws ProcedureException
    {
        var address = routingContext.get( ADDRESS_CONTEXT_KEY );
        if ( address == null  || address == NO_VALUE )
        {
            return Optional.empty();
        }

        if ( address instanceof TextValue )
        {
            try
            {
                return Optional.of( SocketAddressParser.socketAddress( ((TextValue) address).stringValue(), SocketAddress::new ) );
            }
            catch ( Exception e )
            { // Do nothing but warn
            }
        }

        throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "An address key is included in the query string provided to the " +
                                                                            "GetRoutingTableProcedure, but its value could not be parsed." );
    }

    @Override
    public String pluginName()
    {
        return "ssr";
    }

    @Override
    public RoutingResult run( NamedDatabaseId namedDatabaseId, MapValue routingContext ) throws ProcedureException
    {

        var clientAddress = findClientProvidedAddress( routingContext );
        if ( clientAddress.isEmpty() || isKnownAdvertisedAddress( clientAddress.get() ) )
        {
            return super.run(namedDatabaseId, routingContext);
        }
        else
        {
            var addresses = Collections.singletonList( clientAddress.get() );
            return new RoutingResult( addresses, addresses, addresses, configuredRoutingTableTtl() );
        }
    }
}
