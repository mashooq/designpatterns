package mashooq.spring.dispatcherworker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    private static final Log logger = LogFactory.getLog(MessageDispatcher.class);
    private ConcurrentHashMap<String, QueueChannel> activeChannels = new ConcurrentHashMap<String, QueueChannel>();
    private ConcurrentHashMap<String, PollingConsumer> activeConsumers = new ConcurrentHashMap<String, PollingConsumer>();
    private GenericApplicationContext applicationContext;
    private final ConcurrentHashMap<String, Integer> receivedMessageCounter = new ConcurrentHashMap<String, Integer>();

    @Router
    public String dispatch(CustomMessage inputMessage) {
        String channelName = inputMessage.getId() + "-channel";
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
        final String consumerName = inputMessage.getId() + "-consumer";
        PollingConsumer activeConsumer = new PollingConsumer(activeChannel, new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                logger.info("Message received: " + message.getPayload());
                maintainMessageCounter(message);
            }

            private void maintainMessageCounter(final Message<?> message) {
                CustomMessage payload = (CustomMessage) message.getPayload();
                synchronized (receivedMessageCounter) {
                    Integer counter = receivedMessageCounter.get(payload.getId());
                    if (counter == null) {
                        receivedMessageCounter.put(payload.getId(), new Integer(1));
                    } else {
                        receivedMessageCounter.put(payload.getId(), new Integer(++counter));
                    }
                }
            }
        });

        activeConsumer.setBeanName(consumerName);
        activeConsumer.setAutoStartup(true);
        activeConsumer.setBeanFactory(applicationContext.getBeanFactory());
        activeConsumer.setTaskExecutor((TaskExecutor) applicationContext.getBean("consumerPool"));



        applicationContext.getBeanFactory().registerSingleton(consumerName, activeConsumer);
        return activeConsumer;
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
