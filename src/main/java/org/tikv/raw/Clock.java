package org.tikv.raw;

class Clock {
  private long rate = 0;
  private volatile long now = 0;
  private boolean outdated = true;

  private Clock(long rate) {
    this.rate = rate;
    this.now = System.nanoTime();
    // start();
  }

  private void start() {
    if (outdated) {
      new Thread(
              () -> {
                outdated = false;
                try {
                  Thread.sleep(rate);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                now = System.nanoTime();
                outdated = true;
              })
          .start();
    }
  }

  long now() {
    // start();
    // return now;
    return System.nanoTime();
  }

  static final Clock CLOCK = new Clock(10);
}
