package org.redisson;


import java.time.Duration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class ReplicatedConnectionManagerFallbackTest extends RedisDockerTest {
    @Test
    public void testReadModeSlaveFallBackToMasterWhenReplicaDown() throws Exception {
        Network network = Network.newNetwork();

        GenericContainer<?> master = new GenericContainer<>("redis:7.2.3")
                .withNetwork(network)
                .withNetworkAliases("master")
                .withCommand("redis-server",
                        "--bind", "0.0.0.0",
                        "--protected-mode", "no",
                        "--save", "",
                        "--appendonly", "no")
                .withExposedPorts(6379);

        GenericContainer<?> replica = new GenericContainer<>("redis:7.2.3")
                .withNetwork(network)
                .withNetworkAliases("replica")
                .withCommand("redis-server",
                        "--bind", "0.0.0.0",
                        "--protected-mode", "no",
                        "--replicaof", "master", "6379",
                        "--save", "",
                        "--appendonly", "no")
                .withExposedPorts(6379);

        master.start();
        replica.start();

        RedissonClient client = null;
        try {
            waitUntilReplicaUp(replica, Duration.ofSeconds(10));
            String masterAddr = "redis://127.0.0.1:" + master.getFirstMappedPort();
            String replicaAddr = "redis://127.0.0.1:" + replica.getFirstMappedPort();
            Config config = new Config();
            config.setProtocol(protocol);
            config.useReplicatedServers()
                    .addNodeAddress(masterAddr, replicaAddr)
                    .setReadMode(ReadMode.SLAVE)
                    .setScanInterval(200)
                    .setTimeout(1000)
                    .setRetryAttempts(1);

            client = Redisson.create(config);
            RBucket<String> bucket = client.getBucket("k");
            bucket.set("v");
            Assertions.assertThat(bucket.get()).isEqualTo("v");
            replica.stop();

            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            Throwable last = null;

            while (System.nanoTime() < deadline) {
                try {
                    String v = bucket.get();
                    if ("v".equals(v)) {
                        return;
                    }
                } catch (Throwable t) {
                    last = t;
                }
                Thread.sleep(100);
            }

            Assertions.fail("GET did not succeed via master fallback within timeout. last=" + last);

        } finally {
            if (client != null) {
                client.shutdown();
            }
            try { replica.stop(); } catch (Exception ignored) {}
            try { master.stop(); } catch (Exception ignored) {}
            try { network.close(); } catch (Exception ignored) {}
        }
    }

    private static void waitUntilReplicaUp(GenericContainer<?> replica, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            String out = replica.execInContainer("redis-cli", "info", "replication").getStdout();

            if (out.contains("role:slave") && out.contains("master_link_status:up")) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Replica did not become online within " + timeout);
    }
}
