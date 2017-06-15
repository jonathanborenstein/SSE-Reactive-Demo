package com.example.demo;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

}

@Controller
class SSEController {
	
	int i = 0;
	int j = 0;
	int k = 0;

	
	private Map<Integer, String> quotes = new ConcurrentHashMap<Integer, String>() {
		{
			put(0, "Life is what happens to you while you’re busy making other plans. –John Lennon");
			put(1, "An unexamined life is not worth living. –Socrates");
			put(2, "The most common way people give up their power is by thinking they don’t have any. –Alice Walker");
			put(3, "Every child is an artist.  The problem is how to remain an artist once he grows up. –Pablo Picasso");
			put(4, "Whatever you can do, or dream you can, begin it.  Boldness has genius, power and magic in it. –Johann Wolfgang von Goethe");

		}
	};
	
	private Map<Integer, String> books = new ConcurrentHashMap<Integer, String>() {
		{
			put(0, "To Kill A Mockingbird");
			put(1, "The Stranger");
			put(2, "Catcher in the Rye");
			put(3, "Moby Dick");
			put(4, "The Great Gatsby");

		}
	};
	
	@GetMapping(value = "/numbers",  produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	@ResponseBody
	public Flux<Integer> getCounting(){
		return Flux.<Integer>generate(sink -> sink.next(i++))
			.delayElements(Duration.ofMillis(500));
	}
	
	@GetMapping(value = "/quotes",  produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	@ResponseBody
	public Flux<String> getQuotes(){
		return Flux.<String>generate(book -> book.next(quotes.get(++j % quotes.size())))
			.delayElements(Duration.ofSeconds(2));
	}
	
	@GetMapping(value = "/books",  produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	@ResponseBody
	public Flux<String> getBooks(){
		return Flux.<String>generate(book -> book.next(books.get(++k % books.size())))
			.delayElements(Duration.ofSeconds(1));
	}
	
	@GetMapping(value = "/date", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	@ResponseBody
	public Flux<LocalDateTime> getLocalDateTime(){
		return Flux.<LocalDateTime>generate(sink -> sink.next(LocalDateTime.now()))
				.delayElements(Duration.ofSeconds(1));
	}
	
	@GetMapping(value = "/random", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	@ResponseBody
	public Flux<Double> getRandomNumber(){
		return Flux.<Double>generate(sink -> sink.next(Math.random() * 100))
				.delayElements(Duration.ofMillis(300));
	}
	
	@GetMapping(value = "/")
	public String all(){
		return "index";
	}
	
}
