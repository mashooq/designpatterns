package mashooq.spring.dispatcherworker;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.MessageChannel;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static com.jayway.awaitility.Awaitility.to;
import static com.jayway.awaitility.Awaitility.waitAtMost;
import static com.jayway.awaitility.Duration.FIVE_SECONDS;
import static mashooq.spring.dispatcherworker.MessageDispatcher.CONSUMER_SUFFIX;
import static org.hamcrest.CoreMatchers.is;
import static org.springframework.integration.support.MessageBuilder.withPayload;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class MessageDispatcherTest {

    public static final String ID_ONE = "id-1";
    public static final String ID_TWO = "id-2";
    public static final String ID_ONE_CONSUMER = ID_ONE + CONSUMER_SUFFIX;
    public static final String MESSAGE_PAYLOAD = "sample message payload";
    @Autowired
    MessageChannel inputChannel;

    @Autowired
    private MessageDispatcher messageDispatcher;


    @Test
    public void allMessagesWithSameIdAreProcessedByTheSameConsumer() throws Exception {
        inputChannel.send(withPayload(createMessage(ID_ONE, 1)).build());
        inputChannel.send(withPayload(createMessage(ID_ONE, 5)).build());
        inputChannel.send(withPayload(createMessage(ID_TWO, 1)).build());
        inputChannel.send(withPayload(createMessage(ID_ONE, 2)).build());

        final int expectedNumberOfMessages = 3;
        waitAtMost(FIVE_SECONDS).untilCall(
                to(messageDispatcher).getNumberOfMessagesProcessedBy(ID_ONE_CONSUMER),
                is(expectedNumberOfMessages));
    }


    private CustomMessage createMessage(String id, Integer version) {
        CustomMessage message = new CustomMessage();
        message.setId(id);
        message.setVersion(version);
        message.setPayload(MESSAGE_PAYLOAD);
        return message;
    }
}
