package com.github.dao.bigtable.persistence.snapshot;

import akka.persistence.SelectedSnapshot;
import akka.persistence.SnapshotMetadata;
import akka.persistence.serialization.Snapshot;
import akka.serialization.Serialization;
import akka.serialization.Serializer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class SnapshotSerializerSimple implements SnapshotSerializer {

    private final Serializer serializer;

    @SneakyThrows
    public static SnapshotSerializer create(Serialization serialization) {
        Serializer simpleAkkaSerializer = serialization.serializerFor(Snapshot.class);
        return new SnapshotSerializerSimple(simpleAkkaSerializer);
    }

    @Override
    public SnapshotItem toBinary(SelectedSnapshot repr) {
        byte[] bytes = serializer.toBinary(new Snapshot(repr.snapshot()));
        SnapshotMetadata metadata = repr.metadata();
        return new SnapshotItem(metadata.persistenceId(), metadata.sequenceNr(), metadata.timestamp(), bytes);
    }

    @Override
    public SelectedSnapshot fromBinary(SnapshotItem item) {
        Snapshot snapshot = (Snapshot) serializer.fromBinary(item.getBytes());
        SnapshotMetadata metadata = SnapshotMetadata.apply(item.getPersistenceId(), item.getSequenceNr(), item.getTimestamp());
        return SelectedSnapshot.create(metadata, snapshot.data());
    }
}
