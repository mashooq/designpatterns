package mashooq.spring.dispatcherworker;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.PollingConsumer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.springframework.integration.support.MessageBuilder.withPayload;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class MessageDispatcherTest implements ApplicationContextAware {
	
	@Autowired
	MessageChannel inputChannel;
    private ApplicationContext applicationContext;


    @Test
	public void testCat() throws InterruptedException {
		inputChannel.send(withPayload(createMessage("id-1", 1)).build());
		inputChannel.send(withPayload(createMessage("id-1", 5)).build());
		inputChannel.send(withPayload(createMessage("id-2", 1)).build());
		inputChannel.send(withPayload(createMessage("id-1", 2)).build());

        MessageDispatcher messageDispatcher = (MessageDispatcher) applicationContext.getBean("messageDispatcher");
        assertNotNull(messageDispatcher);
        Thread.sleep(1000);
        assertThat(messageDispatcher.getNumberOfVersionsReceivedForMessage("id-1"), is(3));
        assertThat(messageDispatcher.getNumberOfVersionsReceivedForMessage("id-2"), is(1));
	}

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    private CustomMessage createMessage(String id, Integer version) {
        CustomMessage message = new CustomMessage();
        message.setId(id);
        message.setVersion(version);
        message.setPayload("message body");
        return message;
    }
}
