package br.com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ServiceHttpEcommerceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ServiceHttpEcommerceApplication.class, args);
	}


	@Bean
	public KafkaDispatcher dispatcher(){
		return new KafkaDispatcher();
	}


}
