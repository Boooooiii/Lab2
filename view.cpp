#include "View.h"
#include "Controller.h"

using namespace mvc;

View::View(ResponseHandler& r) : window(sf::VideoMode({ WINDOW_SIZEX, WINDOW_SIZEY }), "PostgreSQL manager"), response(r) {
	window.setFramerateLimit(120);
	ImGui::SFML::Init(window);

	ImGuiIO& io = ImGui::GetIO();
	io.Fonts->Clear();
	io.Fonts->AddFontFromFileTTF("Arial.ttf", 16.0f, nullptr, io.Fonts->GetGlyphRangesCyrillic());

	if (!ImGui::SFML::UpdateFontTexture()) {
		std::cerr << "View: font update error!" << std::endl;
	}

}

View::~View() {
	if (ImGui::GetCurrentContext() != nullptr)
	{
		ImGui::SFML::Shutdown();
	}
}

void View::Draw() {
	window.clear();

	ImGui::SFML::Render(window);

	window.display();
}

void View::HandleInput() {
	while (const std::optional event = window.pollEvent()) {
		ImGui::SFML::ProcessEvent(window, *event);

		if (event->is<sf::Event::Closed>()) {
			window.close();
		}

		if (event->is<sf::Event::Resized>()) {
			const auto& newSize = event->getIf<sf::Event::Resized>()->size;
			WINDOW_SIZEX = newSize.x;
			WINDOW_SIZEY = newSize.y;
		}
	}
}

void View::table(const std::string& name) {
	controller->current_table_name = name;
	columnsNames = controller->getTableColumnsNames(name);
	
	const drogon::orm::Result& res = controller->getTableData(name); 

	if (ImGui::BeginTable("activeTable", columnsNames.size() + 1, ImGuiTableFlags_Borders |
		ImGuiTableFlags_ScrollY | ImGuiTableFlags_RowBg, ImVec2(0, WINDOW_SIZEY * 0.78))) {

		ImGui::TableSetupScrollFreeze(0, 1);
		ImGui::TableSetupColumn("Nu", ImGuiTableColumnFlags_WidthFixed);
		for (const auto& columnName : columnsNames)
			ImGui::TableSetupColumn(columnName.c_str());
		ImGui::TableHeadersRow();

		char label_id[256];

		ImGuiListClipper clipper;
		clipper.Begin(res.size());

		while (clipper.Step()) {
			for (int current_row_index = clipper.DisplayStart; current_row_index < clipper.DisplayEnd; ++current_row_index) {
				const auto& row = res[current_row_index];
				ImGui::TableNextRow();

				ImGui::TableNextColumn();
				sprintf_s(label_id, "%d##nu_%d", current_row_index + 1, current_row_index);
				bool is_row_selected = controller->isCellEditing(current_row_index, -1);
				if (ImGui::Selectable(label_id, is_row_selected, ImGuiSelectableFlags_AllowDoubleClick)) {
					controller->onCellSelected(current_row_index, -1);
				}


				for (int i = 0; i < columnsNames.size(); i++) {
					ImGui::TableNextColumn();

					std::string cell_content = row[i].isNull() ? "" : row[i].as<std::string>();
					sprintf_s(label_id, "%s##%d_%d", cell_content.c_str(), current_row_index, i);

					if (controller->isCellEditing(current_row_index, i)) {
						ImGui::SetKeyboardFocusHere();

						if (ImGui::InputText("Enter/ESC", controller->getEditBuffer(), controller->getBufferSize(), ImGuiInputTextFlags_EnterReturnsTrue |
							ImGuiInputTextFlags_AutoSelectAll | ImGuiInputTextFlags_EscapeClearsAll))
						{
							controller->onCellEditConfirmed();
						}
						else if (ImGui::IsItemFocused() && ImGui::IsKeyPressed(ImGuiKey_Escape))
						{
							controller->onCellEditCanceled();
						}
					}
					else {
						if (ImGui::Selectable(label_id, controller->isCellSelected(current_row_index, i), ImGuiSelectableFlags_AllowDoubleClick))
						{
							controller->onCellSelected(current_row_index, i);

							if (ImGui::IsMouseDoubleClicked(0)) {
								controller->onCellDoubleClicked(current_row_index, i, cell_content);
							}
						}
					}
				}
			}
		}
		ImGui::EndTable();
	}
}

void View::insert() {
	if (ImGui::BeginTable("additionalTable", columnsNames.size()+1, ImGuiTableFlags_RowBg)) {
		ImGui::TableSetupColumn("Actions", ImGuiTableColumnFlags_WidthFixed);
		ImGui::TableNextRow();
		ImGui::TableNextColumn();		
		if (ImGui::SmallButton("Clear")) { controller->clearInsertBuffers(); }
		if (ImGui::SmallButton("Instert")) { controller->onInsertClicked(); }
		for (const auto& colName : columnsNames) {
			ImGui::TableNextColumn();
			ImGui::PushItemWidth(-1);
			ImGui::InputText(std::string("Enter/ECS##insert_" + colName).c_str(), controller->getInsertBuffer(colName), controller->getBufferSize(),
				ImGuiInputTextFlags_EscapeClearsAll);
			ImGui::PopItemWidth();
		}

		ImGui::EndTable();
	}
}

void View::misc(const std::string& tableName) {

	insert();

	if (ImGui::Button("Add Random Data")) {
		controller->onAddRandomDataClicked();
	}

	ImGui::SetNextItemWidth(WINDOW_SIZEX / 7);
	ImGui::SameLine();
	ImGui::InputInt("Amount of Random Data", controller->getRandomDataCountPtr());

	ImGui::SameLine(0.0f, 30.0f);
	ImGui::TextColored(response.isLastCommitWasAnError() ? ImVec4(0.9, 0.1, 0.1, 1) : ImVec4(0.1, 0.9, 0.1, 1), "%s", response.getResponse().c_str());

	bool* isSegment = controller->getIsSegmentToDeletePtr();

	if (ImGui::Button((*isSegment ? "Delete rows" : "Delete row"))) {
		controller->onDeleteRowsClicked();
	}

	ImGui::SameLine();
	ImGui::Checkbox("segment", isSegment);

	ImGui::SameLine();
	ImGui::SetNextItemWidth(WINDOW_SIZEX / 8);
	if (*isSegment) {
		ImGui::InputInt("From", controller->getDeleteRowsFromPtr());
		ImGui::SameLine();
		ImGui::SetNextItemWidth(WINDOW_SIZEX / 8);
		ImGui::InputInt("To", controller->getDeleteRowsToPtr());
	}
	else {
		ImGui::InputInt("Number (Nu)", controller->getDeleteRowsFromPtr());
	}

	ImGui::SameLine();
	ImGui::SetCursorPosX( WINDOW_SIZEX - ImGui::CalcTextSize("Clear all tables").x - 15);
	if (ImGui::Button("Clear all tables"))
		controller->onClearAllClicked();

	ImGui::SetCursorPos({ WINDOW_SIZEX - (ImGui::CalcTextSize("Search").x + 13), 5 });
	if (ImGui::SmallButton("Search"))
		controller->onOpenSearchWindowClicked();
}

void View::tabs() {
	if (ImGui::BeginTabBar("TablesBar")) {

		tableNames = controller->getTableNames();

		for (const std::string& name : tableNames) {
			if (ImGui::BeginTabItem(name.c_str())) {

				table(name);
				misc(name);

				ImGui::EndTabItem();
			}
		}

		ImGui::EndTabBar();
	}
}

void View::mainWindow() {
	ImGui::SetNextWindowPos(ImVec2(0, 0), ImGuiCond_Always);
	ImGui::SetNextWindowSize(ImVec2(window.getSize().x, window.getSize().y), ImGuiCond_Always);

	if (ImGui::Begin("Main begin", nullptr, ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoCollapse | ImGuiWindowFlags_NoResize |
		ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoBringToFrontOnFocus | ImGuiWindowFlags_NoNavFocus)) {

		tabs();

		ImGui::End();
	}
}

void View::searchWindow() {
	if (!controller->isSearchWindowOpen())
		return;

	ImGui::OpenPopup("Search 6_6");
	ImGui::SetNextWindowPos(ImVec2( WINDOW_SIZEX / 2,WINDOW_SIZEY / 2 ), ImGuiCond_Appearing, ImVec2(0.5f, 0.5f));

	if (ImGui::BeginPopupModal("Search 6_6", NULL, ImGuiWindowFlags_AlwaysAutoResize)) {

		ImGui::Text("Advanced search");
		ImGui::Separator();
		ImGui::Text("List of teachers that leads the subject: ");
		ImGui::Text("Enter subject name " );
		ImGui::SameLine();
		ImGui::InputText("##subject_input", controller->getSubjectBuffer(), controller->getBufferSize());
		ImGui::Text("Enter teachers first/last name: ");
		ImGui::SameLine();
		ImGui::InputText("##first_name_input", controller->getFirstNameBuffer(), controller->getBufferSize());
		ImGui::SameLine();
		ImGui::InputText("##last_name_input", controller->getLastNameBuffer(), controller->getBufferSize());
		ImGui::TextDisabled("Empty teachers fields means that it will search for teacher with any first/last name");
		if (ImGui::Button("Search##1")) 
			controller->on1SearchRequestClicked();

		ImGui::Separator();
		ImGui::Text("Student attendance in groups by dates(YYYY-MM-DD): ");
		ImGui::Text("Enter group name ");
		ImGui::SameLine();
		ImGui::InputText("##group_input", controller->getGroupBuffer(), controller->getBufferSize());
		ImGui::Text("Enter dates from "); 
		ImGui::SameLine();
		ImGui::InputText("##attendance_date_input_from", controller->getAttendanceDateFromBuffer(), controller->getBufferSize());
		ImGui::SameLine();
		ImGui::Text(" and to ");
		ImGui::SameLine();
		ImGui::InputText("##attendance_date_input_to", controller->getAttendanceDateToBuffer(), controller->getBufferSize());
		ImGui::TextDisabled("Empty date fields means that it will search throughout the entire time");
		if (ImGui::Button("Search##2"))
			controller->on2SearchRequestClicked();

		ImGui::Separator();
		ImGui::Text("Number of students who missed classes by faculty: ");
		ImGui::Text("Enter faculty: ");
		ImGui::SameLine();
		ImGui::InputText("##faculty_name", controller->getFacultyBuffer(), controller->getBufferSize());
		if (ImGui::Button("Search##3"))
			controller->on3SearchRequestClicked();

		ImGui::Separator();

		const pqxx::result& res = controller->search_res;
		if (res.size() <= 0)
			goto A;

		if (ImGui::BeginTable("Search result", res.columns(), ImGuiTableFlags_ScrollY | ImGuiTableFlags_RowBg, { 0, WINDOW_SIZEX * 0.15f })) {

			for (int i = 0; i < res.columns(); ++i) {
				ImGui::TableSetupColumn(res.column_name(i));
			}
			ImGui::TableHeadersRow();

			ImGuiListClipper clipper;
			clipper.Begin(res.size());

			while (clipper.Step()) {
				for (int row_idx = clipper.DisplayStart; row_idx < clipper.DisplayEnd; ++row_idx) {
					ImGui::TableNextRow();
					for (int col_idx = 0; col_idx < res.columns(); ++col_idx) {
						ImGui::TableNextColumn();

						// NULl перевірка
						const auto& field = res[row_idx][col_idx];
						const char* cell_text = field.is_null() ? "[NULL]" : field.c_str();

						ImGui::TextUnformatted(cell_text);
					}
				}
			}


			ImGui::EndTable();
		}

		A:
		ImGui::TextColored(response.isLastCommitWasAnError() ? ImVec4(0.9f, 0.1f, 0.1f, 1.0f) : ImVec4(0.1f, 0.9f, 0.1f, 1.0f), "%s", response.getResponse().c_str());

		ImGui::Separator();
		if (ImGui::Button("Close")) {
			controller->onCloseSearchWindowClicked();
			ImGui::CloseCurrentPopup(); 
		}

		ImGui::EndPopup();
	}
}

void View::Update(sf::Clock& delta) {
	ImGui::SFML::Update(window, delta.restart());

	mainWindow();
	searchWindow();
}

void View::Start() {
	sf::Clock delta;

	while (window.isOpen())
	{
		HandleInput();
		Update(delta);
		Draw();
	}
}