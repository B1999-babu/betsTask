package com.Json.Project.Service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.Json.Project.Entity.Bets;
import com.Json.Project.Repository.BetsRepository;

@Service
public class BetsService {

	@Autowired
	private BetsRepository betsRepository;
	
	
	public Iterable<Bets> save(List<Bets> bets){
		return betsRepository.saveAll(bets);
	}
	public Iterable<Bets> getList() {
		// TODO Auto-generated method stub
		return betsRepository.findAll();
	}

	public List<Bets> getBetsfrom(String game,int clientId,String date){
		return betsRepository.getAllBetsFrom(game, clientId, date);
	}
}
