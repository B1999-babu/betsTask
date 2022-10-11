package com.Json.Project.Service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.Json.Project.Entity.Bets;
import com.Json.Project.Repository.BetsRepository;

@Service
public class Producer {

	@Autowired
	private BetsRepository betsRepository;
	
	public static final String topic="bet_detail";
	
	
	private KafkaTemplate<String, String> kafkatemp;
	
	public void publishToTopic(String message) {
		System.out.println("Publishing the topic "+topic);
		List<Bets> returns=betsRepository.getAllBetsFromReturns();
		if(!returns.isEmpty()) {
		for(int i=0; i< returns.size(); i++) {
		this.kafkatemp.send(topic,message);
		}
		}
	}
	
	
	
	
}
