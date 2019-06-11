package com.ntent.configuration

import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}

import rx.lang.scala.{Observable, Subscriber}

/**
  * When you subscribe to the Observable returned by: [[com.ntent.configuration.Dconfig]].liveUpdateEffectiveSettings(),
  * wrap your Subscriber with an instance of this class.  This class will detect/remove the endInitPair emitted by
  * liveUpdateEffectiveSettings().  Call waitUntilInitialized(), to wait until the wrapped Subscriber has received
  * all of the "initialization phase" values (or an error) from liveUpdateEffectiveSettings().  For example:
  * <pre>
  * Observable[KeyValuePair] pairs = Dconfig().liveUpdateEffectiveSettings()
  * Subscriber[KeyValuePair] initSub = InitSubscriber(new Subscriber() { ... })
  * pairs.subscribe(initSub)
  * initSub.waitUntilInitialized(20, TimeUnit.SECONDS)
  * </pre>
  * NOTE: Observables from other methods won't emit endInitPair, and will cause waitForInit() to block until timeout.
  * So only use this class with Observables returned by liveUpdateEffectiveSettings().
  */
class InitSubscriber(subscriber: Subscriber[KeyValuePair]) extends Subscriber[KeyValuePair] {
  /**
    * Wait until subscriber has received all onNext() calls from the "initialization phase".
    */
  @throws(classOf[InterruptedException])
  @throws(classOf[TimeoutException])
  def waitUntilInitialized(timeout: Long, unit: TimeUnit): Unit = {
    if (!initLatch.await(timeout, unit)) {
      throw new TimeoutException(s"initLatch.await() limit reached: $timeout ${String.valueOf(unit).toLowerCase}")
    }
  }

  /** 1 while in "initialization phase".  Zero after initialization is over. */
  private val initLatch = new CountDownLatch(1)

  override def onNext(pair: KeyValuePair): Unit = {
    // If this is the "end of initialization marker",
    if (pair eq InitSubscriber.endInitPair) {
      // Signal "initialization complete".
      initLatch.countDown()
    } else {
      // Tell subscriber about this pair.
      subscriber.onNext(pair)
    }
  }

  override def onError(error: Throwable): Unit = {
    subscriber.onError(error)
    initLatch.countDown()
  }

  override def onCompleted(): Unit = {
    subscriber.onCompleted()
    initLatch.countDown()
  }
}

object InitSubscriber {
  def apply(subscriber: Subscriber[KeyValuePair]): InitSubscriber = new InitSubscriber(subscriber)

  private val endInitKey = "SubscriberWithInit_EndInit_BDCA"
  private val endInitPair: KeyValuePair = KeyValuePair(endInitKey, endInitKey, endInitKey)

  /** Signals: "end initialization". */
  private[configuration] def endInit: Observable[KeyValuePair] = Observable.just(endInitPair)
}
