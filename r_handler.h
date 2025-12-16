#pragma once

#include <string>
#include <SFML/System.hpp>
#include <iostream>

class ResponseHandler {
	std::string ResponseMessage;
	sf::Clock timer;
	bool error;

	void commit(const std::string& Message) {
		if (!Message.empty()) {
			ResponseMessage = std::string(error ? ErrorMessage : SuccessMessage) + ": " + Message;
			timer.restart();
		}
	}

public:
	// in seconds
	static constexpr unsigned int message_time = 5; 
	static constexpr const char* SuccessMessage = "Success";
	static constexpr const char* ErrorMessage = "Error";

	ResponseHandler() : error(false), ResponseMessage("Hi user!"), timer() {
		timer.start();
	}

	const std::string getResponse() {
		if (timer.getElapsedTime().asSeconds() < message_time)
			return ResponseMessage;
		return {};
	}

	void commitErrorMessage(const std::string& Message) {
		error = true;
		commit(Message);
	}

	void commitSuccessMessage(const std::string& Message) {
		error = false;
		commit(Message);	
	}

	bool isLastCommitWasAnError() {
		return error;
	}
};