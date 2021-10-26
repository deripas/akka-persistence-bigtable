package com.github.dao.bigtable.persistence.journal;

import akka.persistence.PersistentRepr;
import akka.serialization.Serialization;
import akka.serialization.Serializer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

//todo: create specific Serializer
@RequiredArgsConstructor
public class JournalSerializerSimple implements JournalSerializer {

    private final Serializer serializer;

    @SneakyThrows
    public static JournalSerializer create(Serialization serialization) {
        Serializer simpleAkkaSerializer = serialization.serializerFor(PersistentRepr.class);
        return new JournalSerializerSimple(simpleAkkaSerializer);
    }

    @Override
    public JournalItem toBinary(PersistentRepr repr) {
        // remove original persistenceId to reduce the number of bytes
        PersistentRepr withoutPersistenceId = updatePersistenceId(repr, PersistentRepr.Undefined());
        return new JournalItem(repr.persistenceId(), repr.sequenceNr(), serializer.toBinary(withoutPersistenceId));
    }

    @Override
    public PersistentRepr fromBinary(JournalItem item) {
        PersistentRepr withoutPersistenceId = (PersistentRepr) serializer.fromBinary(item.getBytes());
        //restore original persistenceId
        return updatePersistenceId(withoutPersistenceId, item.getPersistenceId());
    }

    private static PersistentRepr updatePersistenceId(PersistentRepr repr, String persistenceId) {
        return repr.update(repr.sequenceNr(), persistenceId, repr.deleted(), repr.sender(), repr.writerUuid());
    }
}
