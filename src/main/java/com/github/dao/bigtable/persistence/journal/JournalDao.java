package com.github.dao.bigtable.persistence.journal;

import com.github.dao.bigtable.util.RX;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringUtil;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Consumer;

import static com.github.dao.bigtable.BigtableExtension.BATCH_LIMIT;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static com.google.protobuf.ByteStringUtil.*;

@Slf4j
@RequiredArgsConstructor
class JournalDao {

    public static final byte DELIMITER = '|';
    public static final ByteString BYTES_QUALIFIER = ByteString.copyFromUtf8("b");

    private final String table;
    private final String family;
    private final BigtableDataClient client;

    public Completable replayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max,
                                      Consumer<JournalItem> callback) {
        Query query = Query.create(table)
                .range(range(persistenceId, DELIMITER, fromSequenceNr, toSequenceNr))
                .filter(FILTERS.chain()
                        .filter(FILTERS.limit().cellsPerColumn(1))
                        .filter(FILTERS.family().exactMatch(family))
                        .filter(FILTERS.qualifier().exactMatch(BYTES_QUALIFIER))
                );

        return RX.readRows(client, query)
                .filter(this::notEmpty)
                .take(max)
                .flatMapCompletable(row -> {
                    long sequenceNr = sequenceNr(row.getKey());
                    byte[] bytes = getBytes(row);
                    JournalItem item = new JournalItem(persistenceId, sequenceNr, bytes);

                    return Completable.fromAction(() -> callback.accept(item));
                });
    }

    public Single<Long> readHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        Query query = Query.create(table)
                .range(range(persistenceId, DELIMITER, fromSequenceNr, Long.MAX_VALUE))
                .filter(FILTERS.chain()
                        .filter(FILTERS.value().strip())
                        .filter(FILTERS.limit().cellsPerColumn(1))
                        .filter(FILTERS.family().exactMatch(family))
                        .filter(FILTERS.qualifier().exactMatch(BYTES_QUALIFIER))
                );

        return RX.readRows(client, query)
                .map(row -> sequenceNr(row.getKey()))
                .last(0L);
    }

    public Completable deleteMessages(String persistenceId, long toSequenceNr) {
        Query query = Query.create(table)
                .range(range(persistenceId, DELIMITER, 0, toSequenceNr))
                .filter(FILTERS.chain()
                        .filter(FILTERS.value().strip())
                        .filter(FILTERS.limit().cellsPerColumn(1))
                        .filter(FILTERS.family().exactMatch(family))
                        .filter(FILTERS.qualifier().exactMatch(BYTES_QUALIFIER))
                );
        return RX.readRows(client, query)
                .map(row -> sequenceNr(row.getKey()))
                .toList()
                .flatMapPublisher(sequenceNrs -> Flowable.zip(
                        Flowable.range(0, sequenceNrs.size()), // index
                        Flowable.fromIterable(sequenceNrs),         // sequenceNr
                        (index, sequenceNr) -> {
                            boolean hasNext = index < sequenceNrs.size() - 1;
                            if (hasNext) {
                                return deleteRow(persistenceId, sequenceNr);
                            } else {
                                return tombstoneRow(persistenceId, sequenceNr);
                            }
                        }))
                .buffer(BATCH_LIMIT)
                .flatMapCompletable(bulk -> RX.bulkMutateRows(client, table, bulk));
    }

    public Completable writeMessages(List<JournalItem> messages) {
        return Flowable.fromIterable(messages)
                .map(this::insertRow)
                .buffer(BATCH_LIMIT)
                .flatMapCompletable(bulk -> RX.bulkMutateRows(client, table, bulk));
    }

    private byte[] getBytes(Row row) {
        return row.getCells(family, BYTES_QUALIFIER).get(0).getValue().toByteArray();
    }

    private boolean notEmpty(Row row) {
        List<RowCell> cells = row.getCells(family, BYTES_QUALIFIER);
        if (cells.isEmpty()) return false;

        ByteString value = cells.get(0).getValue();
        return !value.isEmpty();
    }

    private RowMutationEntry deleteRow(String persistenceId, Long sequenceNr) {
        ByteString key = key(persistenceId, DELIMITER, sequenceNr);
        return RowMutationEntry
                .create(key)
                .deleteRow();
    }

    private RowMutationEntry tombstoneRow(String persistenceId, Long sequenceNr) {
        ByteString key = key(persistenceId, DELIMITER, sequenceNr);
        return RowMutationEntry
                .create(key)
                .setCell(family, BYTES_QUALIFIER, ByteString.EMPTY);
    }

    private RowMutationEntry insertRow(JournalItem item) {
        String persistenceId = item.getPersistenceId();
        long sequenceNr = item.getSequenceNr();
        byte[] bytes = item.getBytes();
        ByteString key = key(persistenceId, DELIMITER, sequenceNr);
        return RowMutationEntry
                .create(key)
                .setCell(family, BYTES_QUALIFIER, ByteStringUtil.wrap(bytes));
    }
}
