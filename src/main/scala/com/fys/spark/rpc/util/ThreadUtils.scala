package com.fys.spark.rpc.util

import java.util.concurrent._

import com.fys.spark.rpc.exception.RpcException

import scala.concurrent.{Await, Awaitable, ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration.Duration
import scala.concurrent.forkjoin.{ForkJoinPool => SForkJoinPool, ForkJoinWorkerThread => SForkJoinWorkerThread}
import scala.util.control.NonFatal
import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}

object ThreadUtils {

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(MoreExecutors.sameThreadExecutor())

  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonCachedThreadPool(prefix: String,
                                maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber,
      maxThreadNumber,
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingDeque[Runnable],
      threadFactory
    )
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  /**
    * Wrapper over ScheduledThreadPoolExecutor.
    */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
    * Run a piece of code in a new thread and return the result. Exception in the new thread is
    * thrown in the caller thread with an adjusted stack trace that removes references to this
    * method for clarity. The exception stack traces will be like the following
    *
    * SomeException: exception-message
    * at CallerClass.body-method (sourcefile.scala)
    * at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
    * at CallerClass.caller-method (sourcefile.scala)
    * ...
    */
  def runInNewThread[T](
                         threadName: String,
                         isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()
    thread.join()

    exception match {
      case Some(realException) =>
        // Remove the part of the stack that shows method calls into this helper method
        // This means drop everything from the top until the stack element
        // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
        val baseStackTrace = Thread.currentThread().getStackTrace().dropWhile(
          !_.getClassName.contains(this.getClass.getSimpleName)).drop(1)

        // Remove the part of the new thread stack that shows methods call from this helper method
        val extraStackTrace = realException.getStackTrace.takeWhile(
          !_.getClassName.contains(this.getClass.getSimpleName))

        // Combine the two stack traces, with a place holder just specifying that there
        // was a helper method used, without any further details of the helper
        val placeHolderStackElem = new StackTraceElement(
          s"... run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")} ..",
          " ", "", -1)
        val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

        // Update the stack trace and rethrow the exception in the caller thread
        realException.setStackTrace(finalStackTrace)
        throw realException
      case None =>
        result
    }
  }

  /**
    * Construct a new Scala ForkJoinPool with a specified max parallelism and name prefix.
    */
  def newForkJoinPool(prefix: String, maxThreadNumber: Int): SForkJoinPool = {
    // Custom factory to set thread names
    val factory = new SForkJoinPool.ForkJoinWorkerThreadFactory {
      override def newThread(pool: SForkJoinPool) =
        new SForkJoinWorkerThread(pool) {
          setName(prefix + "-" + super.getName)
        }
    }
    new SForkJoinPool(maxThreadNumber, factory,
      null, // handler
      false // asyncMode
    )
  }

  // scalastyle:off awaitresult
  /**
    * Preferred alternative to `Await.result()`. This method wraps and re-throws any exceptions
    * thrown by the underlying `Await` call, ensuring that this thread's stack trace appears in
    * logs.
    */
  @throws(classOf[RpcException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      Await.result(awaitable, atMost)
      // scalastyle:on awaitresult
    } catch {
      case NonFatal(t) =>
        throw new RpcException("Exception thrown in awaitResult: ", t)
    }
  }

  /**
    * Calls `Awaitable.result` directly to avoid using `ForkJoinPool`'s `BlockingContext`, wraps
    * and re-throws any exceptions with nice stack track.
    *
    * Codes running in the user's thread may be in a thread of Scala ForkJoinPool. As concurrent
    * executions in ForkJoinPool may see some [[ThreadLocal]] value unexpectedly, this method
    * basically prevents ForkJoinPool from running other tasks in the current waiting thread.
    */
  @throws(classOf[RpcException])
  def awaitResultInForkJoinSafely[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.result(atMost)(awaitPermission)
    } catch {
      case NonFatal(t) =>
        throw new RpcException("Exception thrown in awaitResult: ", t)
    }
  }

}