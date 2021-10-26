package com.github.dao.bigtable.persistence.journal;

import akka.persistence.japi.journal.JavaJournalSpec;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.github.dao.bigtable.containers.BigtableEmulatorContainer;
import org.junit.runner.RunWith;
import org.scalatestplus.junit.JUnitRunner;

@RunWith(JUnitRunner.class)
public class BigtableWriteJournalTestCase extends JavaJournalSpec {

    private static final BigtableEmulatorContainer BIGTABLE = new BigtableEmulatorContainer();
    private static final String TABLE_ID = "t1";
    private static final String FAMILY_ID = "f1";

    static {
        BIGTABLE.start();
        try (BigtableTableAdminClient adminClient = BIGTABLE.bigtableTableAdminClient()) {
            adminClient.createTable(CreateTableRequest.of(TABLE_ID).addFamily(FAMILY_ID));
        }
    }

    private static Config bigtableConfig() {
        return ConfigFactory.parseString("""                              
                bigtable {
                  project-id = "%s"
                  instance-id = "%s"
                  emulator-port = %d
                }
                """
                .formatted(BigtableEmulatorContainer.PROJECT_ID, BigtableEmulatorContainer.INSTANCE_ID, BIGTABLE.getMappedPort())
        );
    }

    private static Config journalConfig() {
        return ConfigFactory.parseString("""                              
                bigtable-journal {
                  table = "%s"
                  family = "%s"
                }
                """
                .formatted(TABLE_ID, FAMILY_ID)
        );
    }

    public BigtableWriteJournalTestCase() {
        super(bigtableConfig().withFallback(journalConfig()));
    }

    @Override
    public void afterAll() {
        super.afterAll();
        BIGTABLE.stop();
    }
}
