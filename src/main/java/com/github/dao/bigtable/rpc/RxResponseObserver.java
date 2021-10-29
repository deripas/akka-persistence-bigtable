package com.github.dao.bigtable.rpc;

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RequiredArgsConstructor
public class RxResponseObserver<T> implements ResponseObserver<T> {

    private final AtomicReference<StreamController> controller = new AtomicReference<>(null);
    private final AtomicInteger requested = new AtomicInteger();
    private final Subscriber<? super T> subscriber;

    @Override
    public void onStart(StreamController controller) {
        controller.disableAutoInboundFlowControl();
        this.controller.set(controller);
    }

    @Override
    public void onResponse(T response) {
        subscriber.onNext(response);
        requested.decrementAndGet();
    }

    @Override
    public void onError(Throwable t) {
        subscriber.onError(t);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }

    public Subscription subscription() {
        StreamController controller = Objects.requireNonNull(this.controller.get(), "not started");
        return new StreamControllerSubscription(controller, requested);
    }

    @RequiredArgsConstructor
    private static class StreamControllerSubscription implements Subscription {

        private final StreamController controller;
        private final AtomicInteger requested;

        @Override
        public void request(long n) {
            int diff = incrementAndGetDiff(requested, n);
            if (diff > 0) {
                controller.request(diff);
            }
        }

        @Override
        public void cancel() {
            controller.cancel();
        }

        private static int incrementAndGetDiff(AtomicInteger requested, long n) {
            for (; ; ) {
                int r = requested.get();
                if (r == Integer.MAX_VALUE) {
                    return 0;
                }
                int u = add(r, n);
                if (requested.compareAndSet(r, u)) {
                    return u - r;
                }
            }
        }

        private static int add(int a, long n) {
            int b = (int) n;
            int u = a + b;
            if (u < 0 || b < 0) {
                return Integer.MAX_VALUE;
            }
            return u;
        }
    }
}
