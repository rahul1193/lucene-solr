/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;


import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Java's builtin ThreadLocal has a serious flaw:
 *  it can take an arbitrarily long amount of time to
 *  dereference the things you had stored in it, even once the
 *  ThreadLocal instance itself is no longer referenced.
 *  This is because there is single, master map stored for
 *  each thread, which all ThreadLocals share, and that
 *  master map only periodically purges "stale" entries.
 *
 *  While not technically a memory leak, because eventually
 *  the memory will be reclaimed, it can take a long time
 *  and you can easily hit OutOfMemoryError because from the
 *  GC's standpoint the stale entries are not reclaimable.
 * 
 *  This class works around that, by only enrolling
 *  WeakReference values into the ThreadLocal, and
 *  separately holding a hard reference to each stored
 *  value.  When you call {@link #close}, these hard
 *  references are cleared and then GC is freely able to
 *  reclaim space by objects stored in it.
 *
 *  We can not rely on {@link ThreadLocal#remove()} as it
 *  only removes the value for the caller thread, whereas
 *  {@link #close} takes care of all
 *  threads.  You should not call {@link #close} until all
 *  threads are done using the instance.
 *
 * @lucene.internal
 */

public class CloseableThreadLocal<T> implements Closeable {

  private final ReentrantReadWriteLock.WriteLock writeLock;
  private final ReentrantReadWriteLock.ReadLock readLock;


  // Use a WeakHashMap so that if a Thread exits and is
  // GC'able, its entry may be removed:
  private Map<Thread, T> hardRefs = new WeakHashMap<>();

  // Increase this to decrease frequency of purging in get:
  private static int PURGE_MULTIPLIER = 20;

  // On each get or set we decrement this; when it hits 0 we
  // purge.  After purge, we set this to
  // PURGE_MULTIPLIER * stillAliveCount.  This keeps
  // amortized cost of purging linear.
  private final AtomicInteger countUntilPurge = new AtomicInteger(PURGE_MULTIPLIER);

  public CloseableThreadLocal() {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    writeLock = readWriteLock.writeLock();
    readLock = readWriteLock.readLock();
  }

  protected T initialValue() {
    return null;
  }

  public T get() {
    T value = null;
    readLock.lock();
    try {
      value = hardRefs.get(Thread.currentThread());
    } finally {
      readLock.unlock();
    }
    if (value == null) {
      T iv = initialValue();
      if (iv != null) {
        set(iv);
        return iv;
      } else {
        return null;
      }
    } else {
      maybePurge();
      return value;
    }
  }

  public void set(T object) {
    writeLock.lock();
    try {
      hardRefs.put(Thread.currentThread(), object);
    } finally {
      writeLock.unlock();
    }
    maybePurge();
  }

  private void maybePurge() {
    if (countUntilPurge.getAndDecrement() == 0) {
      purge();
    }
  }

  // Purge dead threads
  private void purge() {
    writeLock.lock();
    try {
      int stillAliveCount = 0;
      for (Iterator<Thread> it = hardRefs.keySet().iterator(); it.hasNext(); ) {
        final Thread t = it.next();
        if (!t.isAlive()) {
          it.remove();
        } else {
          stillAliveCount++;
        }
      }
      int nextCount = (1 + stillAliveCount) * PURGE_MULTIPLIER;
      if (nextCount <= 0) {
        // defensive: int overflow!
        nextCount = 1000000;
      }
      countUntilPurge.set(nextCount);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void close() {
    // Clear the hard refs; then, the only remaining refs to
    // all values we were storing are weak (unless somewhere
    // else is still using them) and so GC may reclaim them:
    hardRefs = null;
  }
}
