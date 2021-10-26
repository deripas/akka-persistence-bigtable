package com.github.dao.bigtable.persistence.journal;

import lombok.Data;

@Data
public class JournalItem {
    private final String persistenceId;
    private final long sequenceNr;
    private final byte[] bytes;
}
