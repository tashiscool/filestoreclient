package com.filestore.core

import java.util.concurrent.{ SynchronousQueue, ThreadPoolExecutor, TimeUnit }

import scala.concurrent.ExecutionContext

object Contexts {

  def cryptoContext: ExecutionContext = ExecutionContext.fromExecutor(new ThreadPoolExecutor(4, 4, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable]))

  object Implicits {
    implicit def cryptoContext = Contexts.cryptoContext
  }
}
