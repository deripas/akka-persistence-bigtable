package com.github.dao.bigtable.persistence.snapshot;

import akka.persistence.SelectedSnapshot;

public interface SnapshotSerializer {

    SnapshotItem toBinary(SelectedSnapshot repr);

    SelectedSnapshot fromBinary(SnapshotItem item);
}
