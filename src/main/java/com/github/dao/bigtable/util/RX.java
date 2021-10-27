package com.github.dao.bigtable.util;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.*;
import lombok.experimental.UtilityClass;

import java.util.List;

@UtilityClass
public class RX {

    public static Flowable<Row> readRows(BigtableDataClient client, Query query) {
        return Flowable.create(emitter -> {
            client.readRowsCallable().call(query, observer(emitter));
        }, BackpressureStrategy.BUFFER);
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

    private static <T> ResponseObserver<T> observer(FlowableEmitter<? super T> emitter) {
        return new ResponseObserver<>() {
            @Override
            public void onStart(StreamController streamController) {
                //todo try disable auto flow control
                //streamController.disableAutoInboundFlowControl();
            }

            @Override
            public void onResponse(T next) {
                emitter.onNext(next);
            }

            @Override
            public void onError(Throwable throwable) {
                emitter.onError(throwable);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        };
    }
}
