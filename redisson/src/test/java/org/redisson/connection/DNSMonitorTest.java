package org.redisson.connection;

import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.NodeType;
import org.redisson.client.FailedNodeDetector;
import org.redisson.client.RedisClient;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.config.ReadMode;
import org.redisson.config.SubscriptionMode;
import org.redisson.misc.RedisURI;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DNSMonitorTest {

    private TestMasterSlaveConnectionManager connectionManager;
    private ClientConnectionsEntry currentMasterEntry;
    private RedisClient masterClient;

    @AfterEach
    public void cleanup() {
        if (currentMasterEntry != null) {
            currentMasterEntry.shutdownAsync().toCompletableFuture().join();
        } else if (masterClient != null) {
            masterClient.shutdown();
        }
        if (connectionManager != null) {
            connectionManager.shutdown(0, 0, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSwitchSkippedForHealthyMasterWhenSwitchOnFailureEnabled() throws Exception {
        setup(false);

        AtomicInteger switches = new AtomicInteger();
        TestMasterSlaveEntry masterSlaveEntry = new TestMasterSlaveEntry(connectionManager, connectionManager.config,
                currentMasterEntry, switches);
        connectionManager.setOverrideEntry(masterSlaveEntry);

        mockResolveAll("redis://2.2.2.2:6379");
        DNSMonitor monitor = new DNSMonitor(connectionManager, masterClient, Collections.emptySet(), 5000, true,
                connectionManager.getServiceManager().getResolverGroup());
        invokeMonitorMasters(monitor);

        Assertions.assertThat(switches.get()).isZero();
    }

    @Test
    public void testSwitchPerformedForFailedMasterWhenSwitchOnFailureEnabled() throws Exception {
        setup(true);

        AtomicInteger switches = new AtomicInteger();
        TestMasterSlaveEntry masterSlaveEntry = new TestMasterSlaveEntry(connectionManager, connectionManager.config,
                currentMasterEntry, switches);
        connectionManager.setOverrideEntry(masterSlaveEntry);

        mockResolveAll("redis://2.2.2.2:6379");
        DNSMonitor monitor = new DNSMonitor(connectionManager, masterClient, Collections.emptySet(), 5000, true,
                connectionManager.getServiceManager().getResolverGroup());
        invokeMonitorMasters(monitor);

        Assertions.assertThat(switches.get()).isEqualTo(1);
    }

    @Test
    public void testSwitchPerformedForHealthyMasterWhenSwitchOnFailureDisabled() throws Exception {
        setup(false);

        AtomicInteger switches = new AtomicInteger();
        TestMasterSlaveEntry masterSlaveEntry = new TestMasterSlaveEntry(connectionManager, connectionManager.config,
                currentMasterEntry, switches);
        connectionManager.setOverrideEntry(masterSlaveEntry);

        mockResolveAll("redis://2.2.2.2:6379");
        DNSMonitor monitor = new DNSMonitor(connectionManager, masterClient, Collections.emptySet(), 5000, false,
                connectionManager.getServiceManager().getResolverGroup());
        invokeMonitorMasters(monitor);

        Assertions.assertThat(switches.get()).isEqualTo(1);
    }

    @Test
    public void testSwitchPerformedForFailedMasterWhenSwitchOnFailureDisabled() throws Exception {
        setup(true);

        AtomicInteger switches = new AtomicInteger();
        TestMasterSlaveEntry masterSlaveEntry = new TestMasterSlaveEntry(connectionManager, connectionManager.config,
                currentMasterEntry, switches);
        connectionManager.setOverrideEntry(masterSlaveEntry);

        mockResolveAll("redis://2.2.2.2:6379");
        DNSMonitor monitor = new DNSMonitor(connectionManager, masterClient, Collections.emptySet(), 5000, false,
                connectionManager.getServiceManager().getResolverGroup());
        invokeMonitorMasters(monitor);

        Assertions.assertThat(switches.get()).isEqualTo(1);
    }

    @Test
    public void testNoSwitchWhenResolvedIpMatchesCurrentMasterAndSwitchOnFailureDisabled() throws Exception {
        setup(true);

        AtomicInteger switches = new AtomicInteger();
        TestMasterSlaveEntry masterSlaveEntry = new TestMasterSlaveEntry(connectionManager, connectionManager.config,
                currentMasterEntry, switches);
        connectionManager.setOverrideEntry(masterSlaveEntry);

        mockResolveAll("redis://1.1.1.1:6379");
        DNSMonitor monitor = new DNSMonitor(connectionManager, masterClient, Collections.emptySet(), 5000, false,
                connectionManager.getServiceManager().getResolverGroup());
        invokeMonitorMasters(monitor);

        Assertions.assertThat(switches.get()).isZero();
    }

    private void setup(boolean failed) throws Exception {
        MasterSlaveServersConfig cfg = new MasterSlaveServersConfig();
        cfg.setMasterAddress("redis://127.0.0.1:6379");
        cfg.setMasterConnectionMinimumIdleSize(0);
        cfg.setMasterConnectionPoolSize(1);
        cfg.setSubscriptionConnectionMinimumIdleSize(0);
        cfg.setSubscriptionConnectionPoolSize(1);
        cfg.setReadMode(ReadMode.MASTER);
        cfg.setSubscriptionMode(SubscriptionMode.MASTER);

        connectionManager = new TestMasterSlaveConnectionManager(cfg, new Config());

        RedisURI uri = new RedisURI("redis://master.test:6379");
        InetSocketAddress addr = new InetSocketAddress(InetAddress.getByAddress("master.test",
                new byte[]{1, 1, 1, 1}), 6379);
        masterClient = connectionManager.createClient(NodeType.MASTER, addr, uri, null);
        masterClient.getConfig().setFailedNodeDetector(new StaticFailedNodeDetector(failed));

        currentMasterEntry = new ClientConnectionsEntry(masterClient, 0, 1, connectionManager, NodeType.MASTER, cfg);
    }

    private MockUp<ServiceManager> mockResolveAll(String resolvedAddress) {
        return new MockUp<ServiceManager>() {
            @Mock
            CompletableFuture<List<RedisURI>> resolveAll(Invocation inv, RedisURI uri) {
                return CompletableFuture.completedFuture(Collections.singletonList(new RedisURI(resolvedAddress)));
            }
        };
    }

    private void invokeMonitorMasters(DNSMonitor monitor) throws Exception {
        Method method = DNSMonitor.class.getDeclaredMethod("monitorMasters");
        method.setAccessible(true);
        CompletableFuture<?> future = (CompletableFuture<?>) method.invoke(monitor);
        future.join();
    }

    private static final class TestMasterSlaveConnectionManager extends MasterSlaveConnectionManager {

        private MasterSlaveEntry overrideEntry;

        private TestMasterSlaveConnectionManager(MasterSlaveServersConfig cfg, Config configCopy) {
            super(cfg, configCopy);
        }

        void setOverrideEntry(MasterSlaveEntry overrideEntry) {
            this.overrideEntry = overrideEntry;
        }

        @Override
        public MasterSlaveEntry getEntry(InetSocketAddress address) {
            if (overrideEntry != null) {
                return overrideEntry;
            }
            return super.getEntry(address);
        }
    }

    private static final class TestMasterSlaveEntry extends MasterSlaveEntry {

        private final ClientConnectionsEntry currentMasterEntry;
        private final AtomicInteger switches;

        private TestMasterSlaveEntry(ConnectionManager connectionManager, MasterSlaveServersConfig config,
                                     ClientConnectionsEntry currentMasterEntry, AtomicInteger switches) {
            super(connectionManager, config);
            this.currentMasterEntry = currentMasterEntry;
            this.switches = switches;
        }

        @Override
        public ClientConnectionsEntry getEntry() {
            return currentMasterEntry;
        }

        @Override
        public CompletableFuture<RedisClient> changeMaster(InetSocketAddress address, RedisURI uri) {
            switches.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class StaticFailedNodeDetector implements FailedNodeDetector {

        private final boolean failed;

        private StaticFailedNodeDetector(boolean failed) {
            this.failed = failed;
        }

        @Override
        public void onConnectSuccessful() {
        }

        @Override
        public void onConnectFailed() {
        }

        @Override
        public void onPingSuccessful() {
        }

        @Override
        public void onPingFailed() {
        }

        @Override
        public void onCommandSuccessful() {
        }

        @Override
        public void onCommandFailed(Throwable cause) {
        }

        @Override
        public boolean isNodeFailed() {
            return failed;
        }
    }
}
