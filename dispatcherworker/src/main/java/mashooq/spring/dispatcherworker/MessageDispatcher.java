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

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

public class MessageDispatcher implements ApplicationContextAware {
    public static final String CHANNEL_SUFFIX = "-channel";
    public static final String CONSUMER_SUFFIX = "-consumer";
    public static final String WORKER_SUFFIX = "-worker";

    private ConcurrentHashMap<String, QueueChannel> activeChannels = new ConcurrentHashMap<String, QueueChannel>();
    private GenericApplicationContext applicationContext;
    private final ConcurrentHashMap<String, MessageWorker> messageWorkers =
            new ConcurrentHashMap<String, MessageWorker>();

    @Resource(name = "consumerExecutorPool")
    TaskExecutor consumerExecutorPool;

    @Router
    public String dispatch(CustomMessage inputMessage) {
        String channelName = inputMessage.getId() + CHANNEL_SUFFIX;

        synchronized (channelName.intern()) {
            if (activeChannels.get(channelName) == null) {
                QueueChannel activeChannel = createNewChannel(channelName);
                PollingConsumer activeConsumer = createConsumerWithWorker(inputMessage, activeChannel);
                activeConsumer.start();
            }
        }

        return channelName;
    }

    public Integer getNumberOfMessagesProcessedBy(String worker) {
        return messageWorkers.get(worker).numberOfMessageReceived;
    }

    private PollingConsumer createConsumerWithWorker(final CustomMessage inputMessage,
                                                     final QueueChannel activeChannel) {
        final String workerName = inputMessage.getId() + WORKER_SUFFIX;
        MessageWorker messageWorker = createMessageWorker(workerName);

        PollingConsumer activeConsumer = new PollingConsumer(activeChannel, messageWorker);
        final String consumerName = inputMessage.getId() + CONSUMER_SUFFIX;
        startConsumingFromChannel(consumerName, activeConsumer);

        return activeConsumer;
    }

    private void startConsumingFromChannel(final String consumerName, final PollingConsumer activeConsumer) {
        activeConsumer.setBeanName(consumerName);
        activeConsumer.setAutoStartup(true);
        activeConsumer.setBeanFactory(applicationContext.getBeanFactory());
        activeConsumer.setTaskExecutor(consumerExecutorPool);
        applicationContext.getBeanFactory().registerSingleton(consumerName, activeConsumer);
    }

    private MessageWorker createMessageWorker(final String workerName) {
        final MessageWorker messageWorker = new MessageWorker();
        messageWorkers.put(workerName, messageWorker);
        return messageWorker;
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


    class MessageWorker implements MessageHandler {
        private int numberOfMessageReceived = 0;

        @Override
        public void handleMessage(Message<?> message) throws MessagingException {
            incrementNumberOfMessagesReceivedByThisConsumer();
        }

        private void incrementNumberOfMessagesReceivedByThisConsumer() {
            numberOfMessageReceived++;
        }
    }

}
