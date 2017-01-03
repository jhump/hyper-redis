package com.bluegosling.redis.concurrent;

import com.bluegosling.redis.concurrent.Observable;
import com.google.common.util.concurrent.MoreExecutors;

public class ObservableAsIteratorTest extends StreamingIteratorTest {
   @Override public void setup() {
      Observable<String> observable = new Observable<>(MoreExecutors.directExecutor());
      this.observer = observable.getDriver();
      this.iter = observable.asIterator();
   }
}
