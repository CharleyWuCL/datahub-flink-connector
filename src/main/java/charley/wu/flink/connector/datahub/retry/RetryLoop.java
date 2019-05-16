/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package charley.wu.flink.connector.datahub.retry;

import java.util.concurrent.TimeUnit;


/**
 * <p> Mechanism to perform an operation on Zookeeper that is safe against disconnections and
 * "recoverable" errors. </p> <p> <p> If an exception occurs during the operation, the RetryLoop
 * will process it, check with the current retry policy and either attempt to reconnect or re-throw
 * the exception </p> <p> Canonical usage:<br> <p>
 * <pre>
 * RetryLoop retryLoop = client.newRetryLoop();
 * while (retryLoop.shouldContinue()) {
 *     try {
 *         // do your work
 *         ZooKeeper zk = client.getZooKeeper(); // it's important to re-get the ZK instance in case
 * there was an error
 *                                               // and the instance was re-created
 *
 *         retryLoop.markComplete();
 *     }
 *     catch (Exception e) {
 *         retryLoop.takeException(e);
 *     }
 * }
 * </pre>
 */
public class RetryLoop {

  private static final RetrySleeper sleeper = new RetrySleeper() {
    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException {
      unit.sleep(time);
    }
  };
  private final long startTimeMs = System.currentTimeMillis();
  private final RetryPolicy retryPolicy;
  private int retryCount = 0;
  private boolean isDone = false;


  public RetryLoop(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }

  /**
   * Returns the default retry sleeper
   *
   * @return sleeper
   */
  public static RetrySleeper getDefaultRetrySleeper() {
    return sleeper;
  }

  /**
   * If true is returned, make an attempt at the operation
   *
   * @return true/false
   */
  public boolean shouldContinue() {
    return !isDone;
  }


  /**
   * Call this when your operation has successfully completed
   */
  public void markComplete() {
    isDone = true;
  }


  /**
   * Pass any caught exceptions here
   *
   * @param exception the exception
   * @throws Exception if not retry-able or the retry policy returned negative
   */
  public void takeException(Exception exception) throws Exception {
    boolean rethrow = true;

    if (retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startTimeMs, sleeper)) {
      rethrow = false;
    }

    if (rethrow) {
      throw exception;
    }
  }

  /**
   * Pass any caught RuntimeException here
   *
   * @param exception the RuntimeException
   */
  public void takeRuntimeException(RuntimeException exception) {
    boolean rethrow = true;

    if (retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startTimeMs, sleeper)) {
      rethrow = false;
    }

    if (rethrow) {
      throw exception;
    }
  }

}
