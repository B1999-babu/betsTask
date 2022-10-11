package com.Json.Project.Repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.Json.Project.Entity.Bets;

@Repository
public interface BetsRepository extends CrudRepository<Bets,Integer>{
	
	
	@Query("from Bets where returns >= 1500.00")
	public List<Bets> getAllBetsFromReturns();
	
	@Query("from Bets where game=:game and clientId=:clientId and date=:date")
	public List<Bets> getAllBetsFrom(@Param("game")String game,@Param("clientId")int clientId,@Param("date")String date);

}
