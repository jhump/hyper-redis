package com.bluegosling.redis.channel;

import io.netty.util.concurrent.Future;

public interface Lifecycle {
   Future<?> start();
   Future<?> shutdown();
}
