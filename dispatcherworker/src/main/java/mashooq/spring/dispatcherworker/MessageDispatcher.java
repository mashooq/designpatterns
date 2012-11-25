package mashooq.spring.dispatcherworker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.task.TaskExecutor;
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

    private PollingConsumer createAssociatedConsumer(CustomMessage inputMessage, QueueChannel activeChannel) {
        String consumerName = inputMessage.getId() + "-consumer";
        PollingConsumer activeConsumer = new PollingConsumer(activeChannel, new MessageHandler() {
            @Override
            public void handleMessage(org.springframework.integration.Message<?> message) throws MessagingException {
                logger.info("Message received: " + message.getPayload());
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
