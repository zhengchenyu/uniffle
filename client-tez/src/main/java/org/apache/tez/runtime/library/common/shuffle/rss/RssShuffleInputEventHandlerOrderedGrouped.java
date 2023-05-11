package org.apache.tez.runtime.library.common.shuffle.rss;

import com.google.protobuf.ByteString;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.CompositeRoutedDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Inflater;

public class RssShuffleInputEventHandlerOrderedGrouped implements ShuffleEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleInputEventHandlerOrderedGrouped.class);

  private RssShuffleScheduler scheduler;
  private InputContext inputContext;
  private boolean compositeFetch;
  private Inflater inflater;

  private final AtomicInteger nextToLogEventCount = new AtomicInteger(0);
  private final AtomicInteger numDmeEvents = new AtomicInteger(0);
  private final AtomicInteger numDmeEventsNoData = new AtomicInteger(0);
  private final AtomicInteger numObsoletionEvents = new AtomicInteger(0);

  public RssShuffleInputEventHandlerOrderedGrouped(InputContext inputContext,
                                                   RssShuffleScheduler scheduler, boolean compositeFetch) {
    this.inputContext = inputContext;
    this.scheduler = scheduler;
    this.compositeFetch = compositeFetch;
    this.inflater = TezCommonUtils.newInflater();
  }

  @Override
  public void handleEvents(List<Event> events) throws IOException {
    for (Event event : events) {
      handleEvent(event);
    }
  }

  private void handleEvent(Event event) throws IOException {
    if (event instanceof DataMovementEvent) {
      numDmeEvents.incrementAndGet();
      DataMovementEvent dmEvent = (DataMovementEvent)event;
      Map<String, Object> shufflePayload;
      try {
        shufflePayload = ShuffleUtils.parseDataMovementEventPayload(dmEvent.getUserPayload(), inflater);
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = (BitSet) shufflePayload.get("empty_partitions");
      processDataMovementEvent(dmEvent, shufflePayload, emptyPartitionsBitSet);
      scheduler.updateEventReceivedTime();
    } else if (event instanceof CompositeRoutedDataMovementEvent) {
      CompositeRoutedDataMovementEvent crdme = (CompositeRoutedDataMovementEvent)event;
      Map<String, Object> shufflePayload;
      try {
        shufflePayload = ShuffleUtils.parseDataMovementEventPayload(crdme.getUserPayload(), inflater);
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to parse DataMovementEvent payload", e);
      }
      BitSet emptyPartitionsBitSet = (BitSet) shufflePayload.get("empty_partitions");
      for (int offset = 0; offset < crdme.getCount(); offset++) {
        numDmeEvents.incrementAndGet();
        processDataMovementEvent(crdme.expand(offset), shufflePayload, emptyPartitionsBitSet);
      }
    } else if (event instanceof InputFailedEvent) {
      numObsoletionEvents.incrementAndGet();
      processTaskFailedEvent((InputFailedEvent) event);
    }

    if (numDmeEvents.get() + numObsoletionEvents.get() > nextToLogEventCount.get()) {
      logProgress(false);
      // Log every 50 events seen.
      nextToLogEventCount.addAndGet(50);
    }
  }

  private void processDataMovementEvent(DataMovementEvent dmEvent, Map<String, Object> shufflePayload,
                                        BitSet emptyPartitionsBitSet) throws IOException {
    int partitionId = dmEvent.getSourceIndex();
    RssInputAttemptIdentifier srcAttemptIdentifier = new RssInputAttemptIdentifier(dmEvent.getTargetIndex(), dmEvent.getVersion());

    if (LOG.isDebugEnabled()) {
      LOG.debug("DME srcIdx: " + partitionId + ", targetIdx: " + dmEvent.getTargetIndex()
        + ", attemptNum: " + dmEvent.getVersion() + ", payload: " + shufflePayload);
    }

    if (shufflePayload.containsKey("empty_partitions")) {
      try {
        if (emptyPartitionsBitSet.get(partitionId)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source partition: " + partitionId + " did not generate any data. SrcAttempt: ["
                + srcAttemptIdentifier + "]. Not fetching.");
          }
          numDmeEventsNoData.getAndIncrement();
          // TODO: ignore empty partition
          scheduler.copySucceeded(srcAttemptIdentifier);
          return;
        }
      } catch (IOException e) {
        throw new TezUncheckedException("Unable to set the empty partition to succeeded", e);
      }
    }

    long taskAttemptId = Long.parseLong((String) shufflePayload.get("path_component"));
    scheduler.addKnownInput(partitionId, srcAttemptIdentifier, taskAttemptId);
  }

  private void processTaskFailedEvent(InputFailedEvent ifEvent) {
    RssInputAttemptIdentifier taIdentifier = new RssInputAttemptIdentifier(ifEvent.getTargetIndex(), ifEvent.getVersion());
    scheduler.obsoleteInput(taIdentifier);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Obsoleting output of src-task: " + taIdentifier);
    }
  }

  @Override
  public void logProgress(boolean updateOnClose) {
    LOG.info(inputContext.getSourceVertexName() + ": "
      + "numDmeEventsSeen=" + numDmeEvents.get()
      + ", numDmeEventsSeenWithNoData=" + numDmeEventsNoData.get()
      + ", numObsoletionEventsSeen=" + numObsoletionEvents.get()
      + (updateOnClose == true ? ", updateOnClose" : ""));
  }
}
