package com.Json.Project.Controller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.Json.Project.Entity.Bets;
import com.Json.Project.Service.BetsService;
import com.Json.Project.Service.Producer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/bets")
public class BetsContoller {
	
	@Autowired
	private BetsService betsService;
	
	@Autowired
	private Producer producer;

	@Value("${file.upload-dir}")
	String FILE_DIRECTORY;
	
	
	//fetch the all the records from the database
	@GetMapping("/bets/list")
	public Iterable<Bets> list(){
		return betsService.getList();
	}
	

	//insert data into database from the json file
	@PostMapping("/uploadJsonFile")
	public Iterable<Bets> save(@RequestParam("file") MultipartFile file) throws IOException{
		File myFile=new File(FILE_DIRECTORY+file.getOriginalFilename());
		myFile.createNewFile();
		FileOutputStream fos=new FileOutputStream(myFile);
		fos.write(file.getBytes());
		fos.close();

		ObjectMapper mapper=new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		TypeReference<List<Bets>> typereference=new TypeReference<List<Bets>>() {};
		InputStream inputStream=typereference.getClass().getResourceAsStream("/Json/"+file.getOriginalFilename());
		Iterable<Bets> service = null;
		try {
			List<Bets> bet=mapper.readValue(inputStream, typereference);
			service=betsService.save(bet);	
			System.out.println("bets saved!");
		}catch(IOException ex) {
			System.out.println("unable to save "+ex.getMessage());
		}
		return service;
	}
	
	//publish the kafka message 
	@PostMapping("/post/message/{msg}")
	public void sendMessage(@RequestParam("msg")String msg) {
		producer.publishToTopic(msg);
	}
	
	
	//fetch the records from database using game,clientId and date parameters
	@GetMapping("/get/{game}/{clientid}/{date}")
	public List<Bets> getBetsFrom(@PathVariable("game")String game,
			@PathVariable("clientid")int clientid,@PathVariable("date")String date){
		return betsService.getBetsfrom(game, clientid, date);
	}
	
}
