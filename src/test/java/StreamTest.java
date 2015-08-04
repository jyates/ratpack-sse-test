import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import ratpack.exec.ExecController;
import ratpack.exec.Execution;
import ratpack.exec.internal.DefaultExecController;
import ratpack.func.Action;
import ratpack.sse.Event;
import ratpack.sse.ServerSentEventStreamClient;
import ratpack.sse.ServerSentEvents;
import ratpack.stream.Streams;
import ratpack.test.embed.EmbeddedApp;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static ratpack.sse.ServerSentEvents.serverSentEvents;

public class StreamTest {
  @Test
  public void run() throws Exception {
    EmbeddedApp app = EmbeddedApp.fromHandler(context -> {
      // infinite stream of strings
      Iterable<String> infinite =
        () -> Stream.iterate(0, i -> i + 1).limit(5).map(i -> Integer.toString(i)).iterator();
      Publisher<String> stream = Streams.publish(infinite);

      ServerSentEvents events = serverSentEvents(stream, e -> {
          e.id(Objects::toString);
          e.event("counter");
          e.data(i -> "event " + i);
        }
      );

      context.render(events);
    });


    CountDownLatch latch = new CountDownLatch(1);
    List<String> events = new ArrayList<>();

    Action<Execution> sseAction = execution -> {
      ServerSentEventStreamClient sseClient = ServerSentEventStreamClient.sseStreamClient(execution.getController(), ByteBufAllocator.DEFAULT);
      sseClient.request(app.getAddress()).then(eventTransformablePublisher -> eventTransformablePublisher.subscribe(new Subscriber<Event<?>>() {
        private Subscription subscription;

        @Override
        public void onSubscribe(Subscription subscription) {
          this.subscription = subscription;
          subscription.request(1);
        }

        @Override
        public void onNext(Event<?> event) {
          events.add(event.getData());
          subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
          System.out.println(throwable.getLocalizedMessage());
          throwable.printStackTrace();
          latch.countDown();
        }

        @Override
        public void onComplete() {
          latch.countDown();
        }
      }));
    };

    try (ExecController execController = new DefaultExecController()) {
      execController.getControl().fork()
          .onError(throwable -> {
            System.out.println(throwable.getLocalizedMessage());
            throwable.printStackTrace();
            latch.countDown();
          })
          .start(sseAction::execute);

      latch.await();
    }

    assertEquals(5, events.size());
    assertArrayEquals(new String[]{"event 0", "event 1", "event 2", "event 3", "event 4"}, events.toArray());

  }
}