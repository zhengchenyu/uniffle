package org.apache.tez.runtime.library.common.shuffle.rss;

public class RssInputAttemptIdentifier {

  private final int inputIdentifier;
  private final int attemptNumber;

  public RssInputAttemptIdentifier(int inputIdentifier, int attemptNumber) {
    this.inputIdentifier = inputIdentifier;     // source vertex task id
    this.attemptNumber = attemptNumber;         // source vertex task attempt id
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + attemptNumber;
    result = prime * result + inputIdentifier;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RssInputAttemptIdentifier other = (RssInputAttemptIdentifier) obj;
    if (attemptNumber != other.attemptNumber)
      return false;
    if (inputIdentifier != other.inputIdentifier)
      return false;
    return true;
  }


  public int getAttemptNumber() {
    return attemptNumber;
  }
}
