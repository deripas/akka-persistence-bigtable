package com.github.dao.bigtable.rpc;

import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class LazySubscription implements Subscription {

    private final AtomicReference<Subscription> upstream = new AtomicReference<>();
    private final Supplier<Subscription> supplier;

    public static Subscription create(Supplier<Subscription> supplier) {
        return new LazySubscription(supplier);
    }

    @Override
    public void request(long n) {
        Subscription subscription = upstream.get();
        if (subscription != null) {
            subscription.request(n);
        } else {
            SubscriptionHelper.setOnce(upstream, supplier.get(), n);
        }
    }

    @Override
    public void cancel() {
        SubscriptionHelper.cancel(upstream);
    }
}
