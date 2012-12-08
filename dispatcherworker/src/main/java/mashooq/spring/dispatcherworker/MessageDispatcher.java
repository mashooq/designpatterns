package mashooq.spring.dispatcherworker;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.Message;
import org.springframework.integration.MessagingException;
import org.springframework.integration.annotation.Router;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessageHandler;
import org.springframework.integration.endpoint.PollingConsumer;

import java.util.concurrent.ConcurrentHashMap;

public class MessageDispatcher implements ApplicationContextAware {
    public static final String CHANNEL_SUFFIX = "-channel";
    public static final String CONSUMER_SUFFIX = "-consumer";

    private ConcurrentHashMap<String, QueueChannel> activeChannels = new ConcurrentHashMap<String, QueueChannel>();
    private GenericApplicationContext applicationContext;
    private final ConcurrentHashMap<String, Integer> receivedMessageCounter = new ConcurrentHashMap<String, Integer>();

    @Router
    public String dispatch(CustomMessage inputMessage) {
        String channelName = inputMessage.getId() + CHANNEL_SUFFIX;
        if (activeChannels.get(channelName) == null) {
            QueueChannel activeChannel = createNewChannel(channelName);
            PollingConsumer activeConsumer = createAssociatedConsumer(inputMessage, activeChannel);
            activeConsumer.start();
        }

        return channelName;
    }

    public Integer getNumberOfVersionsReceivedForMessage(String messageId) {
        Integer versionCount = receivedMessageCounter.get(messageId);
        return versionCount == null ? 0 : versionCount;
    }

    private PollingConsumer createAssociatedConsumer(final CustomMessage inputMessage, final QueueChannel activeChannel) {
        final String consumerName = inputMessage.getId() + CONSUMER_SUFFIX;
        PollingConsumer activeConsumer = createPollingConsumerFor(activeChannel);

        startConsumingFromChannel(consumerName, activeConsumer);

        return activeConsumer;
    }

    private void startConsumingFromChannel(final String consumerName, final PollingConsumer activeConsumer) {
        activeConsumer.setBeanName(consumerName);
        activeConsumer.setAutoStartup(true);
        activeConsumer.setBeanFactory(applicationContext.getBeanFactory());
        activeConsumer.setTaskExecutor((TaskExecutor) applicationContext.getBean("consumerPool"));
        applicationContext.getBeanFactory().registerSingleton(consumerName, activeConsumer);
    }

    private PollingConsumer createPollingConsumerFor(final QueueChannel activeChannel) {
        return new PollingConsumer(activeChannel, new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                maintainMessageCounter(message);
            }

            private void maintainMessageCounter(final Message<?> message) {
                CustomMessage payload = (CustomMessage) message.getPayload();
                synchronized (receivedMessageCounter) {
                    Integer counter = receivedMessageCounter.get(payload.getId());
                    if (counter == null) {
                        receivedMessageCounter.put(payload.getId(), 1);
                    } else {
                        receivedMessageCounter.put(payload.getId(), ++counter);
                    }
                }
            }
        });
    }


    private QueueChannel createNewChannel(String channelName) {
        QueueChannel activeChannel = new QueueChannel();
        activeChannel.setBeanName(channelName);
        activeChannels.put(channelName, activeChannel);
        applicationContext.getBeanFactory().registerSingleton(channelName, activeChannel);
        return activeChannel;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (GenericApplicationContext) applicationContext;
    }
}
