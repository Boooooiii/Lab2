#pragma once

#include "Model.h"
#include "r_handler.h"

namespace mvc {
	class View;

	class Controller {
		ResponseHandler response;
		View& view;
		Model& model;

		static const size_t BUFFER_SIZE = 256;

		int current_row_index = 0;

		int selected_row = -1, selected_col = -1;
		int editing_row = -1, editing_col = -1;
		char text_buffer[BUFFER_SIZE];

		int randomDataCount = 0;
		int deleteRowsFrom = 0, deleteRowsTo = 0;
		bool isSegmentToDelete = false;

		bool isSortWindowOpen = false;

		std::map<std::string, std::array<char, BUFFER_SIZE>> insert_buffers;
		std::map<std::string, std::array<char, BUFFER_SIZE>> custom_quert_buffers;

	public:
		Controller();

		drogon::orm::Result res;
		pqxx::result search_res;
		std::string current_table_name;

        void onCellEditConfirmed();
        void onDeleteRowsClicked();

        std::vector<std::string> getTableNames();
		drogon::orm::Result& getTableData(const std::string& name);
		std::vector<std::string> getTableColumnsNames(const std::string& name);

		bool isCellEditing(int r, int c);
		bool isCellSelected(int r, int c);
		bool isSearchWindowOpen() const;

		char* getEditBuffer();

		void onCellSelected(int row, int col);
		void onCellDoubleClicked(int row, int col, const std::string& current_text);
		void onCellEditCanceled();
		void onAddRandomDataClicked();
		void onClearAllClicked();
		void onInsertClicked();
		void onOpenSearchWindowClicked();
		void onCloseSearchWindowClicked();

		int* getRandomDataCountPtr();
		int* getDeleteRowsFromPtr();
		int* getDeleteRowsToPtr();
		bool* getIsSegmentToDeletePtr();

		void clearInsertBuffers();
		char* getInsertBuffer(const std::string& colName);;

		size_t getBufferSize() const;

		void on1SearchRequestClicked();
		void on2SearchRequestClicked();
		void on3SearchRequestClicked();
		
		char* getSubjectBuffer();
		char* getFirstNameBuffer();
		char* getLastNameBuffer();
		char* getGroupBuffer();
		char* getAttendanceDateFromBuffer();
		char* getAttendanceDateToBuffer();
		char* getFacultyBuffer();
	};
}

