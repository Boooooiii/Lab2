#include "Controller.h"
#include "View.h"

#include <chrono>

using namespace mvc;

Controller::Controller() : view(View::Spawner::instance(response)), model(Model::Spawner::instance())
{
    view.setController(this); 
    view.Start();             
}

void Controller::onCellEditConfirmed() {
    try {
        std::string s(text_buffer);

        drogon::orm::Row row_to_update = res[editing_row];

        model.updateDataCell(current_table_name, row_to_update, model.getTableColumnsNames(current_table_name)[editing_col], s);
        response.commitSuccessMessage("Cell updated!");
    }
    catch (const std::exception& e) {
        response.commitErrorMessage(e.what());
    }
    editing_row = -1;
    editing_col = -1;
}

void Controller::onDeleteRowsClicked() {
    try {
        if (deleteRowsFrom <= 0 || deleteRowsFrom > res.size() || (isSegmentToDelete && (deleteRowsTo <= 0 || deleteRowsTo > res.size())))
        {
            throw std::runtime_error("Wrong row number to delete");
        }

        if (isSegmentToDelete) {
            model.deleteDataRange(current_table_name, res, deleteRowsFrom - 1, deleteRowsTo - 1);
        }
        else {
            model.deleteDataRange(current_table_name, res, deleteRowsFrom - 1, deleteRowsFrom - 1);
        }
        response.commitSuccessMessage("Rows deleted!");
    }
    catch (const std::exception& e) {
        response.commitErrorMessage(e.what());
    }
}

std::vector<std::string> Controller::getTableNames() {
    return model.getTableNames();
}
drogon::orm::Result& Controller::getTableData(const std::string& name) {
    this->res = model.getTableData(name);
    return this->res;
}

bool Controller::isCellEditing(int r, int c) {
    return editing_row == r && editing_col == c;
}

bool Controller::isCellSelected(int r, int c) {
    return selected_row == r && selected_col == c;
}

char* Controller::getEditBuffer() {
    return text_buffer;
}

void Controller::onCellSelected(int row, int col) {
    selected_row = row;
    selected_col = col;
}

void Controller::onCellDoubleClicked(int row, int col, const std::string& current_text) {
    editing_row = row;
    editing_col = col;
    strncpy_s(text_buffer, sizeof(text_buffer), current_text.c_str(), current_text.size());
    text_buffer[sizeof(text_buffer) - 1] = '\0';
}

void Controller::onCellEditCanceled() {
    editing_row = -1;
    editing_col = -1;
}

void Controller::onAddRandomDataClicked() {
    try {
        model.generateRandomDataForTable(current_table_name, randomDataCount);
        response.commitSuccessMessage("Random data added! " + std::to_string(randomDataCount));
    }
    catch (const std::exception& e) {
        response.commitErrorMessage(e.what());
    }
}

void Controller::onClearAllClicked() {
    try {
        model.clearAllTables(current_table_name);
        response.commitSuccessMessage("Nothing left!");
    }
    catch (const std::exception& e) {
        response.commitErrorMessage(e.what());
    }
}

size_t Controller::getBufferSize() const {
    return BUFFER_SIZE;
}

std::vector<std::string> Controller::getTableColumnsNames(const std::string& name) {
    return model.getTableColumnsNames(name);
}

void Controller::clearInsertBuffers()
{
    for (auto& buf : insert_buffers) {
        buf.second.fill('\0');
    }
}

char* Controller::getInsertBuffer(const std::string& colName)
{
    return insert_buffers[colName].data();
}

void Controller::onInsertClicked()
{
    try {
        std::map<std::string, std::string> dataToInsert;
        for (const auto& pair : insert_buffers)
        {
            std::string value(pair.second.data());

            if (!value.empty()) {
                dataToInsert[pair.first] = value;
            }
        }

        if (dataToInsert.empty()) {
            throw std::runtime_error("No data to insert. All fields are empty.");
        }

        model.insertData(current_table_name, dataToInsert);

        response.commitSuccessMessage("Row inserted successfully!");
        clearInsertBuffers();
    }
    catch (const std::exception& e)
    {
        response.commitErrorMessage(e.what());
    }
}

void Controller::onOpenSearchWindowClicked() {
    isSortWindowOpen = true;
}

void Controller::onCloseSearchWindowClicked() {
    isSortWindowOpen = false;
}

bool Controller::isSearchWindowOpen() const {
    return isSortWindowOpen;
}

void Controller::on1SearchRequestClicked() {

    if (strlen(getSubjectBuffer()) <= 0) {
        response.commitErrorMessage("no subject name");
        return;
    }

    std::string request = "SELECT teacher.first_name, teacher.last_name, subject.subject_name"
        " FROM teacher JOIN teacher_subject ON teacher.teacher_id = teacher_subject.teacher_id "
        "JOIN Subject ON teacher_subject.subject_id = subject.subject_id "
        "WHERE  subject.subject_name LIKE $1 AND teacher.first_name LIKE $2 AND teacher.last_name LIKE $3 "
        "ORDER BY subject.subject_name, teacher.last_name";

    std::string subject = getSubjectBuffer();
    std::string first_name = (strlen(getFirstNameBuffer()) > 0) ? getFirstNameBuffer() : "%";
    std::string last_name = (strlen(getLastNameBuffer()) > 0) ? getLastNameBuffer() : "%";

    try {
        auto start = std::chrono::high_resolution_clock::now();
        search_res = model.executeCustomQuery(request, { subject, first_name, last_name });
        auto end = std::chrono::high_resolution_clock::now();
        response.commitSuccessMessage(std::string(search_res.size() > 0 ? "" : "nothing found, ") + " search time in ms "
            + std::to_string(duration_cast<std::chrono::milliseconds>(end - start).count()));
        custom_quert_buffers["Subject"].fill('\0');
        custom_quert_buffers["FirstName"].fill('\0');
        custom_quert_buffers["LastName"].fill('\0');
    }
    catch (const std::exception& e) {
        response.commitErrorMessage(e.what());
    }
}

void Controller::on2SearchRequestClicked() {
    if (strlen(getGroupBuffer()) <= 0) {
        response.commitErrorMessage("no group name");
        return;
    }

    std::string request = reinterpret_cast < const char*>(u8"SELECT student.first_name, student.last_name, student_group.group_name, "
        "COUNT(attendance.attendance_id) FILTER(WHERE attendance.status = 'Присутній') AS present_count "
        "FROM Student_group JOIN Student ON student_group.group_id = student.group_id "
        "JOIN Attendance ON student.student_id = attendance.student_id JOIN Class ON attendance.class_id = class.class_id "
        "WHERE student_group.group_name LIKE $1 AND class.class_date BETWEEN $2 AND COALESCE($3, CURRENT_DATE) "
        "GROUP BY student.student_id, student_group.group_name "
        "ORDER BY present_count DESC, student.last_name");

    std::string group = getGroupBuffer();
    std::string dateFrom = (strlen(getAttendanceDateFromBuffer()) > 0) ? getAttendanceDateFromBuffer() : "1970-01-01";
    std::optional<std::string> dateTo; // за замовчуванням він (std::nullopt)
    if (strlen(getAttendanceDateToBuffer()) > 0) {
        dateTo = getAttendanceDateToBuffer(); 
    }

    try {
        auto start = std::chrono::high_resolution_clock::now();
        search_res = model.executeCustomQuery(request, { group, dateFrom, dateTo });
        auto end = std::chrono::high_resolution_clock::now();
        response.commitSuccessMessage(std::string(search_res.size() > 0 ? "" : "nothing found, ") + " search time in ms "
            + std::to_string(duration_cast<std::chrono::milliseconds>(end - start).count()));
        custom_quert_buffers["Group"].fill('\0');
        custom_quert_buffers["AttendanceDateFrom"].fill('\0');
        custom_quert_buffers["AttendanceDateTo"].fill('\0');
    }
    catch (const std::exception& e) {
        response.commitErrorMessage(e.what());
    }

}

void Controller::on3SearchRequestClicked() {
    std::string request = reinterpret_cast < const char*>(u8"SELECT student_group.faculty, COUNT(DISTINCT student.student_id) AS absent_student_count "
        "FROM student_group JOIN student ON student_group.group_id = student.group_id JOIN Attendance ON student.student_id = attendance.student_id "
        "WHERE attendance.status = 'Відсутній' AND student_group.faculty LIKE $1 "
        "GROUP BY student_group.faculty ORDER BY absent_student_count DESC");

    std::string faculty = (strlen(getFacultyBuffer()) > 0) ? getFacultyBuffer() : "%";

    try {
        auto start = std::chrono::high_resolution_clock::now();
        search_res = model.executeCustomQuery(request, { faculty });
        auto end = std::chrono::high_resolution_clock::now();
        response.commitSuccessMessage(std::string(search_res.size() > 0 ? "" : "nothing found, ") + " search time in ms "
            + std::to_string(duration_cast<std::chrono::milliseconds>(end - start).count()));
        custom_quert_buffers["Faculty"].fill('\0');
    }
    catch (const std::exception& e) {
        response.commitErrorMessage(e.what());
    }
}

char* Controller::getSubjectBuffer() { return custom_quert_buffers["Subject"].data(); }
char* Controller::getFirstNameBuffer() { return custom_quert_buffers["FirstName"].data(); }
char* Controller::getLastNameBuffer() { return custom_quert_buffers["LastName"].data(); }
char* Controller::getGroupBuffer() { return custom_quert_buffers["Group"].data(); }
char* Controller::getAttendanceDateFromBuffer() { return custom_quert_buffers["AttendanceDateFrom"].data(); }
char* Controller::getAttendanceDateToBuffer() { return custom_quert_buffers["AttendanceDateTo"].data(); }
char* Controller::getFacultyBuffer() { return custom_quert_buffers["Faculty"].data(); }

int* Controller::getRandomDataCountPtr() { return &randomDataCount; }
int* Controller::getDeleteRowsFromPtr() { return &deleteRowsFrom; }
int* Controller::getDeleteRowsToPtr() { return &deleteRowsTo; }
bool* Controller::getIsSegmentToDeletePtr() { return &isSegmentToDelete; }