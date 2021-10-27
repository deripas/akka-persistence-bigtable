package com.github.dao.bigtable.persistence.snapshot;

import akka.persistence.SnapshotMetadata;
import akka.persistence.SnapshotSelectionCriteria;
import com.github.dao.bigtable.util.RX;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

import static com.github.dao.bigtable.BigtableExtension.BATCH_LIMIT;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static com.google.protobuf.ByteStringUtil.*;

@Slf4j
@RequiredArgsConstructor
public class SnapshotDao {

    public static final byte DELIMITER = '|';
    public static final ByteString BYTES_QUALIFIER = ByteString.copyFromUtf8("b");

    private final String table;
    private final String family;
    private final BigtableDataClient client;

    public Flowable<SnapshotItem> find(String persistenceId, SnapshotSelectionCriteria criteria) {
        Query query = Query.create(table)
                .range(range(persistenceId, DELIMITER, criteria.minSequenceNr(), criteria.maxSequenceNr()))
                .filter(FILTERS.chain()
                        .filter(toTimestampRange(criteria))
                        .filter(FILTERS.limit().cellsPerColumn(1))
                        .filter(FILTERS.family().exactMatch(family))
                        .filter(FILTERS.qualifier().exactMatch(BYTES_QUALIFIER))
                );

        return RX.readRows(client, query)
                .map(row -> {
                    RowCell cell = getRowCell(row);
                    long sequenceNr = sequenceNr(row.getKey());
                    long timestampMillis = toMillis(cell.getTimestamp());
                    byte[] bytes = cell.getValue().toByteArray();
                    return new SnapshotItem(persistenceId, sequenceNr, timestampMillis, bytes);
                });
    }

    public Completable save(SnapshotItem item) {
        ByteString key = key(item.getPersistenceId(), DELIMITER, item.getSequenceNr());
        long timestampMicros = toMicros(item.getTimestamp());
        RowMutation insert = RowMutation.create(table, key, Mutation.createUnsafe()
                .setCell(family, BYTES_QUALIFIER, timestampMicros, wrap(item.getBytes())));
        return RX.mutateRow(client, insert);
    }

    public Completable delete(SnapshotMetadata metadata) {
        ByteString key = key(metadata.persistenceId(), DELIMITER, metadata.sequenceNr());
        return RX.mutateRow(client, RowMutation.create(table, key).deleteRow());
    }

    public Completable delete(String persistenceId, SnapshotSelectionCriteria criteria) {
        Query query = Query.create(table)
                .range(range(persistenceId, DELIMITER, criteria.minSequenceNr(), criteria.maxSequenceNr()))
                .filter(FILTERS.chain()
                        .filter(toTimestampRange(criteria))
                        .filter(FILTERS.value().strip())
                        .filter(FILTERS.limit().cellsPerColumn(1))
                        .filter(FILTERS.family().exactMatch(family))
                        .filter(FILTERS.qualifier().exactMatch(BYTES_QUALIFIER))
                );

        return RX.readRows(client, query)
                .map(row -> sequenceNr(row.getKey()))
                .toList()
                .flatMapPublisher(sequenceNrs -> Flowable.fromIterable(sequenceNrs)
                        .map(sequenceNr -> {
                            log.debug("event=deleteRow persistenceId={} sequenceNr={}", persistenceId, sequenceNr);
                            ByteString key = key(persistenceId, DELIMITER, sequenceNr);
                            return RowMutationEntry.create(key).deleteRow();
                        }))
                .buffer(BATCH_LIMIT)
                .flatMapCompletable(bulk -> RX.bulkMutateRows(client, table, bulk));
    }

    private RowCell getRowCell(Row row) {
        return row.getCells(family, BYTES_QUALIFIER).get(0);
    }

    private static Filters.TimestampRangeFilter toTimestampRange(SnapshotSelectionCriteria criteria) {
        long from = toMicros(criteria.minTimestamp());
        long to = toMicros(criteria.maxTimestamp());
        return FILTERS.timestamp().range().of(from, next(to));
    }

    private static long toMicros(long millis) {
        return TimeUnit.MILLISECONDS.toMicros(millis);
    }

    private static long toMillis(long micros) {
        return TimeUnit.MICROSECONDS.toMillis(micros);
    }

    private static long next(long value) {
        if (value < Long.MAX_VALUE) {
            return value + 1;
        } else {
            return Long.MAX_VALUE;
        }
    }
}
