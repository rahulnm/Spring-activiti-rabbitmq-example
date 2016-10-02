package com.example;

import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.bpmn.behavior.ReceiveTaskActivityBehavior;
import org.activiti.engine.impl.pvm.delegate.ActivityBehavior;
import org.activiti.engine.impl.pvm.delegate.ActivityExecution;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.spring.integration.Activiti;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	private Log log = LogFactory.getLog(DemoApplication.class);

	@Bean
	ActivityBehavior gateway(MessageChannels channels) {
		return new ReceiveTaskActivityBehavior() {

			@Override
			public void execute(ActivityExecution execution) throws Exception {

				Map<String, Object> vars = execution.getVariables();

				Message<?> executionMessage = MessageBuilder
						.withPayload(vars)
						.setHeader("executionId", execution.getId())
						.setHeader("taskName", execution.getCurrentActivityName())
						.build();

				channels.requests().send(executionMessage);
			}
		};
	}

	@Bean
	IntegrationFlow requestsFlow(MessageChannels channels) {
		return IntegrationFlows.from(channels.requests())
				.handle(msg -> log.info(msg.toString()))
				.get();
	}

	@Bean
	IntegrationFlow repliesFlow(MessageChannels channels,
								ProcessEngine engine) {
		return IntegrationFlows.from(channels.replies())
				.handle(msg -> {
							log.info("PAYLOAD: " + msg.getPayload().toString());
							engine.getRuntimeService()
									.signal(String.class.cast(msg.getHeaders().get("executionId")),
											(Map<String, Object>) (msg.getPayload()));
						}
				)
				.get();
	}
}


@Configuration
class MessageChannels {

	@Bean
	DirectChannel requests() {
		return new DirectChannel();
	}

	@Bean
	DirectChannel replies() {
		return new DirectChannel();
	}
}

@RestController
class ProcessStartingRestController {

	@Autowired
	private ProcessEngine processEngine;

	@RequestMapping(method = RequestMethod.GET, value = "/start")
	Map<String, String> launch() {
		Map<String, Object> HOTING_VAR = new HashMap<>();
		HOTING_VAR.put("1", "linode.com");
		HOTING_VAR.put("2", "heroku.com");
		HOTING_VAR.put("3", "digitalocean.com");
		HOTING_VAR.put("4", "aws.amazon.com");

		ProcessInstance asyncProcess = this.processEngine.getRuntimeService()
				.startProcessInstanceByKey("asyncProcess", HOTING_VAR);
		return Collections.singletonMap("executionId", asyncProcess.getId());
	}
}

@RestController
class ProcessResumingRestController {

	@Autowired
	private MessageChannels messageChannels;

	@RequestMapping(method = RequestMethod.GET, value = "/resume/{executionId}")
	void resume(@PathVariable String executionId) {
			Map<String, Object> HOTING_VAR = new HashMap<>();
			HOTING_VAR.put("1", "google.com");
			HOTING_VAR.put("2", "heroku.com");
			HOTING_VAR.put("3", "digitalocean.com");
			HOTING_VAR.put("4", "aws.amazon.com");

		Message<?> build = MessageBuilder
				.withPayload(HOTING_VAR)
				.setHeader("executionId", executionId)
				.build();
		this.messageChannels.replies().send(build);
	}
}
