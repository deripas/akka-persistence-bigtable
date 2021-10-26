package com.github.dao.bigtable.persistence.snapshot;

import akka.actor.ActorSystem;
import akka.persistence.SelectedSnapshot;
import akka.persistence.SnapshotMetadata;
import akka.persistence.SnapshotSelectionCriteria;
import akka.persistence.snapshot.japi.SnapshotStore;
import akka.serialization.SerializationExtension;
import com.github.dao.bigtable.BigtableExtension;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.typesafe.config.Config;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import scala.concurrent.Future;

import java.util.Optional;

import static scala.compat.java8.FutureConverters.toScala;

@Slf4j
public class BigtableSnapshotStore extends SnapshotStore {

    public static final String TABLE = "table";
    public static final String FAMILY = "family";

    private final SnapshotSerializer serializer;
    private final SnapshotDao dao;

    @SneakyThrows
    public BigtableSnapshotStore(Config config) {
        ActorSystem actorSystem = context().system();
        BigtableDataClient dataClient = BigtableExtension.get(actorSystem).dataClient();
        dao = new SnapshotDao(config.getString(TABLE), config.getString(FAMILY), dataClient);
        serializer = SnapshotSerializerSimple.create(SerializationExtension.get(actorSystem));
    }

    @Override
    public Future<Optional<SelectedSnapshot>> doLoadAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        Mono<Optional<SelectedSnapshot>> result = dao.find(persistenceId, criteria)
                .takeLast(1)
                .map(serializer::fromBinary)
                .map(Optional::of)
                .last(Optional.empty())
                .doOnSuccess(selectedSnapshot -> {
                    log.debug("event=doLoadAsync persistenceId={} criteria={} OK {}",
                            persistenceId, criteria, selectedSnapshot);
                })
                .doOnError(throwable -> {
                    log.debug("event=doLoadAsync persistenceId={} criteria={} FAIL",
                            persistenceId, criteria, throwable);
                });
        return toScala(result.toFuture());
    }

    @Override
    public Future<Void> doSaveAsync(SnapshotMetadata metadata, Object snapshot) {
        Mono<Void> result = Mono.defer(() -> {
                    SnapshotItem item = serializer.toBinary(SelectedSnapshot.create(metadata, snapshot));
                    return dao.save(item);
                })
                .doOnSuccess(unused -> {
                    log.debug("event=doSaveAsync metadata={} snapshot={} OK",
                            metadata, snapshot);
                })
                .doOnError(throwable -> {
                    log.debug("event=doSaveAsync metadata={} snapshot={} FAIL",
                            metadata, snapshot, throwable);
                });
        return toScala(result.toFuture());
    }

    @Override
    public Future<Void> doDeleteAsync(SnapshotMetadata metadata) {
        Mono<Void> result = dao.delete(metadata)
                .doOnSuccess(unused -> {
                    log.debug("event=doDeleteAsync metadata={} OK",
                            metadata);
                })
                .doOnError(throwable -> {
                    log.debug("event=doDeleteAsync metadata={} FAIL",
                            metadata, throwable);
                });
        return toScala(result.toFuture());
    }

    @Override
    public Future<Void> doDeleteAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        Mono<Void> result = dao.delete(persistenceId, criteria)
                .doOnSuccess(unused -> {
                    log.debug("event=doDeleteAsync persistenceId={} criteria={} OK",
                            persistenceId, criteria);
                })
                .doOnError(throwable -> {
                    log.debug("event=doDeleteAsync persistenceId={} criteria={} FAIL",
                            persistenceId, criteria, throwable);
                });
        return toScala(result.toFuture());
    }
}
