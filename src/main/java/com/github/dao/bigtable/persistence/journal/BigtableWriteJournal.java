package com.github.dao.bigtable.persistence.journal;

import akka.actor.ActorSystem;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentRepr;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.SerializationExtension;
import com.github.dao.bigtable.BigtableExtension;
import com.google.api.client.util.Lists;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.typesafe.config.Config;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import scala.compat.java8.ScalaStreamSupport;
import scala.concurrent.Future;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static scala.compat.java8.FutureConverters.toScala;

@Slf4j
public class BigtableWriteJournal extends AsyncWriteJournal {

    public static final String TABLE = "table";
    public static final String FAMILY = "family";

    private final JournalSerializer serializer;
    private final JournalDao dao;

    @SneakyThrows
    public BigtableWriteJournal(Config config) {
        ActorSystem actorSystem = context().system();
        BigtableDataClient dataClient = BigtableExtension.get(actorSystem).dataClient();
        dao = new JournalDao(config.getString(TABLE), config.getString(FAMILY), dataClient);
        serializer = JournalSerializerSimple.create(SerializationExtension.get(actorSystem));
    }

    @Override
    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max,
                                              Consumer<PersistentRepr> replayCallback) {
        Consumer<JournalItem> callback = journalItem -> {
            PersistentRepr persistentRepr = serializer.fromBinary(journalItem);
            log.debug("event=replayMessage msg={}", persistentRepr);
            replayCallback.accept(persistentRepr);
        };
        Mono<Void> result = dao.replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, callback)
                .doOnSuccess(unused -> {
                    log.debug("event=doAsyncReplayMessages persistenceId={} fromSequenceNr={} toSequenceNr={} max={} OK",
                            persistenceId, fromSequenceNr, toSequenceNr, max);
                })
                .doOnError(throwable -> {
                    log.debug("event=doAsyncReplayMessages persistenceId={} fromSequenceNr={} toSequenceNr={} max={} FAIL",
                            persistenceId, fromSequenceNr, toSequenceNr, max, throwable);
                });
        return toScala(result.toFuture());
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        Mono<Long> result = dao.readHighestSequenceNr(persistenceId, fromSequenceNr)
                .doOnSuccess(sequenceNr -> {
                    log.debug("event=doAsyncReadHighestSequenceNr persistenceId={} fromSequenceNr={} OK {}",
                            persistenceId, fromSequenceNr, sequenceNr);
                })
                .doOnError(throwable -> {
                    log.debug("event=doAsyncReadHighestSequenceNr persistenceId={} fromSequenceNr={} FAIL",
                            persistenceId, fromSequenceNr, throwable);
                });
        return toScala(result.toFuture());
    }

    @Override
    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {
        log.debug("event=doAsyncWriteMessages messages={}", messages);
        List<JournalItem> batchWriteMessages = Lists.newArrayList();
        CompletableFuture<Void> batchWriteFuture = new CompletableFuture<>();

        List<CompletableFuture<Void>> futures = StreamSupport.stream(messages.spliterator(), false)
                .map(atomicWrite -> {
                    try {
                        List<JournalItem> journalItems = ScalaStreamSupport.stream(atomicWrite.payload())
                                .map(serializer::toBinary)
                                .collect(Collectors.toList());
                        batchWriteMessages.addAll(journalItems);
                        return batchWriteFuture;
                    } catch (Exception exception) {
                        return CompletableFuture.<Void>failedFuture(exception);
                    }
                })
                .collect(Collectors.toList());

        dao.writeMessages(batchWriteMessages)
                .doOnSuccess(batchWriteFuture::complete)
                .doOnError(batchWriteFuture::completeExceptionally)
                .subscribe();

        Mono<Iterable<Optional<Exception>>> result = Flux.fromIterable(futures)
                .concatMap(future -> Mono.fromCompletionStage(future)
                        .thenReturn(Optional.<Exception>empty())
                        .onErrorResume(BigtableWriteJournal::toOptionalException))
                .collectList()
                .map(optionals -> optionals);
        return toScala(result.toFuture());
    }

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
        Mono<Void> result = dao.deleteMessages(persistenceId, toSequenceNr)
                .doOnSuccess(unused -> {
                    log.debug("event=doAsyncDeleteMessagesTo persistenceId={} toSequenceNr={} OK",
                            persistenceId, toSequenceNr);
                })
                .doOnError(throwable -> {
                    log.debug("event=doAsyncDeleteMessagesTo persistenceId={} toSequenceNr={} FAIL",
                            persistenceId, toSequenceNr, throwable);
                });
        return toScala(result.toFuture());
    }

    private static Mono<? extends Optional<Exception>> toOptionalException(Throwable throwable) {
        if (throwable instanceof Exception) {
            return Mono.just(Optional.of((Exception) throwable));
        } else {
            return Mono.error(throwable);
        }
    }
}
