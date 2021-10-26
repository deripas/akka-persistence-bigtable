package com.github.dao.bigtable;

import akka.actor.*;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.typesafe.config.Config;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BigtableExtension implements Extension {

    public static final int BATCH_LIMIT = 100_000;

    private static final Provider EXTENSION_PROVIDER = new Provider();
    private final BigtableDataClient dataClient;

    public static BigtableExtension get(ActorSystem system) {
        return EXTENSION_PROVIDER.get(system);
    }

    public BigtableDataClient dataClient() {
        return dataClient;
    }

    static class Provider extends AbstractExtensionId<BigtableExtension> implements ExtensionIdProvider {

        public static final String EMULATOR_PORT = "emulator-port";
        public static final String PROJECT_ID = "project-id";
        public static final String INSTANCE_ID = "instance-id";

        @SneakyThrows
        @Override
        public BigtableExtension createExtension(ExtendedActorSystem system) {
            Config config = system.settings().config().getConfig("bigtable");

            BigtableDataSettings.Builder builder = config.hasPath(EMULATOR_PORT)
                    ? BigtableDataSettings.newBuilderForEmulator(config.getInt(EMULATOR_PORT))
                    : BigtableDataSettings.newBuilder();

            BigtableDataSettings settings = builder
                    .setProjectId(config.getString(PROJECT_ID))
                    .setInstanceId(config.getString(INSTANCE_ID))
                    .build();

            log.info("event=BigtableExtension config={}", config);
            BigtableDataClient dataClient = BigtableDataClient.create(settings);
            system.registerOnTermination(dataClient::close);
            return new BigtableExtension(dataClient);
        }

        @Override
        public ExtensionId<? extends Extension> lookup() {
            return EXTENSION_PROVIDER;
        }
    }
}
