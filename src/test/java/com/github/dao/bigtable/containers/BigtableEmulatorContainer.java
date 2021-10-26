package com.github.dao.bigtable.containers;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class BigtableEmulatorContainer extends GenericContainer<BigtableEmulatorContainer> {

    public static final String PROJECT_ID = "PROJECT_ID";
    public static final String INSTANCE_ID = "INSTANCE_ID";
    public static final int PORT = 8086;

    public BigtableEmulatorContainer() {
        super(DockerImageName.parse("google/cloud-sdk:latest"));
        withLogConsumer(new Slf4jLogConsumer(log));
        withExposedPorts(PORT);
        withCommand("gcloud beta emulators bigtable start --host-port=0.0.0.0:" + PORT);
        waitingFor(Wait.forListeningPort());
    }

    public Integer getMappedPort() {
        return getMappedPort(PORT);
    }

    @SneakyThrows
    public BigtableTableAdminClient bigtableTableAdminClient() {
        return BigtableTableAdminClient.create(
                BigtableTableAdminSettings
                        .newBuilderForEmulator(getMappedPort())
                        .setProjectId(PROJECT_ID)
                        .setInstanceId(INSTANCE_ID)
                        .build()
        );
    }
}
