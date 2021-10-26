package com.github.dao.bigtable.persistence.journal;

import akka.persistence.PersistentRepr;

public interface JournalSerializer {

    JournalItem toBinary(PersistentRepr repr);

    PersistentRepr fromBinary(JournalItem item);
}
