#pragma once

#include <iostream>

#include <imgui.h>
#include <imgui-SFML.h>
#include <SFML/Graphics.hpp>
#include <drogon/drogon.h>

#include "r_handler.h"

namespace mvc {

	static unsigned int WINDOW_SIZEX = 1280, WINDOW_SIZEY = 720;
	class Controller;

	class View {
	private:
		sf::RenderWindow window;
		ResponseHandler& response;
		mvc::Controller* controller;

		std::vector<std::string> columnsNames = {};
		std::vector<std::string> tableNames = {};

		View(ResponseHandler& r);

		void Draw();
		void HandleInput();
		void Update(sf::Clock& delta);

		void table(const std::string& name);
		void insert();
		void misc(const std::string& tableName);
		void tabs();
		void mainWindow();
		void searchWindow();

		static View& getInstance(ResponseHandler& r) {
			static View instance(r);
			return instance;
		}

	public:
		~View();

		View(const View&) = delete;
		void operator=(const View&) = delete;

		class Spawner {
			static View& instance(ResponseHandler& r) {
				return getInstance(r);
			}

			friend class Controller;
		};

		void setController(mvc::Controller* Controller) {
			controller = Controller;
		}

		void Start();
	};

}