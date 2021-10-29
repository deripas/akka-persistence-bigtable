package com.github.dao.bigtable.util;

import com.github.dao.bigtable.rpc.LazySubscription;
import com.github.dao.bigtable.rpc.RxResponseObserver;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableEmitter;
import io.reactivex.rxjava3.core.Flowable;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@UtilityClass
public class RX {

    public static Flowable<Row> readRows(BigtableDataClient client, Query query) {
        return Flowable.fromPublisher(subscriber -> {
            subscriber.onSubscribe(LazySubscription.create(() -> {
                log.debug("event=rx action=crate");
                RxResponseObserver<Row> observer = new RxResponseObserver<>(subscriber);
                client.readRowsCallable().call(query, observer);
                return observer.subscription();
            }));
        });
    }

    public static Completable bulkMutateRows(BigtableDataClient client, String table, List<RowMutationEntry> mutations) {
        BulkMutation bulkMutation = BulkMutation.create(table);
        for (RowMutationEntry mutation : mutations) {
            bulkMutation.add(mutation);
        }
        return Completable.create(emitter -> {
            ApiFuture<Void> apiFuture = client.bulkMutateRowsAsync(bulkMutation);
            ApiFutures.addCallback(apiFuture, callback(emitter), MoreExecutors.directExecutor());
        });
    }

    public static Completable mutateRow(BigtableDataClient client, RowMutation rowMutation) {
        return Completable.create(emitter -> {
            ApiFuture<Void> apiFuture = client.mutateRowAsync(rowMutation);
            ApiFutures.addCallback(apiFuture, callback(emitter), MoreExecutors.directExecutor());
        });
    }

    private static ApiFutureCallback<? super Void> callback(CompletableEmitter emitter) {
        return new ApiFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                emitter.onError(throwable);
            }

            @Override
            public void onSuccess(Void unused) {
                emitter.onComplete();
            }
        };
    }
}
