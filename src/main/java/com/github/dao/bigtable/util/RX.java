package com.github.dao.bigtable.util;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.experimental.UtilityClass;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.List;

@UtilityClass
public class RX {

    public static Flux<Row> readRows(BigtableDataClient client, Query query) {
        return Flux.create(fluxSink -> client.readRowsCallable().call(query, observer(fluxSink)));
    }

    public static Mono<Void> bulkMutateRows(BigtableDataClient client, String table, List<RowMutationEntry> mutations) {
        BulkMutation bulkMutation = BulkMutation.create(table);
        for (RowMutationEntry mutation : mutations) {
            bulkMutation.add(mutation);
        }

        return Mono.create(monoSink -> {
            ApiFuture<Void> apiFuture = client.bulkMutateRowsAsync(bulkMutation);
            ApiFutures.addCallback(apiFuture, callback(monoSink), MoreExecutors.directExecutor());
        });
    }

    public static Mono<Void> mutateRow(BigtableDataClient client, RowMutation rowMutation) {
        return Mono.create(monoSink -> {
            ApiFuture<Void> apiFuture = client.mutateRowAsync(rowMutation);
            ApiFutures.addCallback(apiFuture, callback(monoSink), MoreExecutors.directExecutor());
        });
    }

    private static ApiFutureCallback<? super Void> callback(MonoSink<Void> voidMonoSink) {
        return new ApiFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                voidMonoSink.error(throwable);
            }

            @Override
            public void onSuccess(Void unused) {
                voidMonoSink.success();
            }
        };
    }

    private static <T> ResponseObserver<T> observer(FluxSink<T> fluxSink) {
        return new ResponseObserver<>() {
            @Override
            public void onStart(StreamController streamController) {
                //todo try disable auto flow control
                //streamController.disableAutoInboundFlowControl();
            }

            @Override
            public void onResponse(T next) {
                fluxSink.next(next);
            }

            @Override
            public void onError(Throwable throwable) {
                fluxSink.error(throwable);
            }

            @Override
            public void onComplete() {
                fluxSink.complete();
            }
        };
    }
}
