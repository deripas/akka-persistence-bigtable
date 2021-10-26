package com.github.dao.bigtable.persistence.snapshot;

import lombok.Data;

@Data
public final class SnapshotItem {
    private final String persistenceId;
    private final long sequenceNr;
    private final long timestamp;
    private final byte[] bytes;
}
