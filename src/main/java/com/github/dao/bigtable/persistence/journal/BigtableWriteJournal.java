package com.github.dao.bigtable.persistence.journal;

import akka.actor.ActorSystem;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentRepr;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.SerializationExtension;
import com.github.dao.bigtable.BigtableExtension;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.typesafe.config.Config;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import scala.compat.java8.ScalaStreamSupport;
import scala.concurrent.Future;

import java.util.List;
import java.util.Optional;
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
        return dao.replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, callback)
                .doOnComplete(() -> {
                    log.debug("event=doAsyncReplayMessages persistenceId={} fromSequenceNr={} toSequenceNr={} max={} OK",
                            persistenceId, fromSequenceNr, toSequenceNr, max);
                })
                .doOnError(throwable -> {
                    log.debug("event=doAsyncReplayMessages persistenceId={} fromSequenceNr={} toSequenceNr={} max={} FAIL",
                            persistenceId, fromSequenceNr, toSequenceNr, max, throwable);
                })
                .to(upstream -> toScala(upstream.toCompletionStage(null)));
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        return dao.readHighestSequenceNr(persistenceId, fromSequenceNr)
                .doOnSuccess(sequenceNr -> {
                    log.debug("event=doAsyncReadHighestSequenceNr persistenceId={} fromSequenceNr={} OK {}",
                            persistenceId, fromSequenceNr, sequenceNr);
                })
                .doOnError(throwable -> {
                    log.debug("event=doAsyncReadHighestSequenceNr persistenceId={} fromSequenceNr={} FAIL",
                            persistenceId, fromSequenceNr, throwable);
                })
                .to(upstream -> toScala(upstream.toCompletionStage()));
    }

    @Override
    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {
        log.debug("event=doAsyncWriteMessages messages={}", messages);
        List<Flowable<JournalItem>> itemsList = toJava(messages);

        Single<List<JournalItem>> batch = Flowable.fromIterable(itemsList)
                .flatMap(Flowable::onErrorComplete)
                .toList();

        return batch
                .flatMapCompletable(dao::writeMessages)
                .toSingleDefault(Optional.empty())
                .flatMap(unused -> Flowable.fromIterable(itemsList)
                        .flatMapSingle(items -> items.toList()
                                .map(journalItems -> Optional.<Exception>empty())
                                .onErrorReturn(throwable -> Optional.of(toException(throwable))))
                        .toList())
                .map(optionals -> (Iterable<Optional<Exception>>) optionals)
                .to(upstream -> toScala(upstream.toCompletionStage()));
    }

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
        return dao.deleteMessages(persistenceId, toSequenceNr)
                .doOnComplete(() -> {
                    log.debug("event=doAsyncDeleteMessagesTo persistenceId={} toSequenceNr={} OK",
                            persistenceId, toSequenceNr);
                })
                .doOnError(throwable -> {
                    log.debug("event=doAsyncDeleteMessagesTo persistenceId={} toSequenceNr={} FAIL",
                            persistenceId, toSequenceNr, throwable);
                })
                .to(upstream -> toScala(upstream.toCompletionStage(null)));
    }

    private List<Flowable<JournalItem>> toJava(Iterable<AtomicWrite> messages) {
        return StreamSupport.stream(messages.spliterator(), false)
                .map(atomicWrite -> {
                    try {
                        List<JournalItem> journalItems = ScalaStreamSupport.stream(atomicWrite.payload())
                                .map(serializer::toBinary)
                                .collect(Collectors.toList());
                        return Flowable.fromIterable(journalItems);
                    } catch (Exception exception) {
                        return Flowable.<JournalItem>error(exception);
                    }
                })
                .collect(Collectors.toList());
    }


    @SneakyThrows
    private static Exception toException(Throwable throwable) {
        if (throwable instanceof Exception) {
            return (Exception) throwable;
        } else {
            throw throwable;
        }
    }
}
