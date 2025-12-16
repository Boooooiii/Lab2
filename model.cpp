#include "Model.h"

#include "Class.h"
#include "Attendance.h"
#include "Student.h"
#include "StudentGroup.h"
#include "Subject.h"
#include "Teacher.h"
#include "TeacherSubject.h"

using namespace mvc;

using namespace drogon_model::Lab1;

int32_t safeStoll(const std::string& val, const std::string& fieldName) {
    try {
        return std::stoll(val);
    }
    catch (const std::exception& e) {
        throw std::runtime_error("Conversion error: Value '" + val + "' for field '" + fieldName + "' must be an integer.");
    }
}

std::string unquoteIdentifier(const std::string& s) {
    std::string result = s;

    result.erase(std::remove(result.begin(), result.end(), '"'), result.end());

    return result;
}

Model::Model() {
    try {
        std::string cS = "host=localhost port=8040 dbname=Lab1 user=postgres password=4242";
        conn = std::make_unique<pqxx::connection>(cS);

        dbClient = drogon::orm::DbClient::newPgClient("host=localhost port=8040 dbname=Lab1 connect_timeout=10 user=postgres password=4242",1);

        pqxx::work worker(*conn);
        pqxx::result res = worker.exec("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;");

        tableNames.clear();
        for (auto row : res) {
            tableNames.push_back(row[0].as<std::string>());
        }
    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }

}

bool Model::isForeignKey(const std::string& tableName, const std::string& columnName)
{
    try {
        pqxx::nontransaction worker(*conn);

        const std::string sql =
            "SELECT 1 FROM information_schema.table_constraints AS tc "
            "JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
            "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = " + worker.quote_name("public") +
            "  AND tc.table_name = " + worker.quote_name(tableName) + " AND kcu.column_name = " + worker.quote_name(columnName) + " LIMIT 1;";

        pqxx::result res = worker.exec(sql);

        return !res.empty();
    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
        return false;
    }
}

const drogon::orm::Result& Model::requestTableData(const std::string& TableName) {
    if (!conn)
        reconnect();

    try {
        drogon::orm::Result res = dbClient->execSqlSync("SELECT * FROM " + TableName);
        tableData = { TableName, res };
        return tableData.second;
    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}


std::map<std::string, std::string> Model::getPrimaryKeyInfo(pqxx::work& worker, const std::string& tableName) {
    std::string pk_query =
        "SELECT kcu.column_name, c.data_type "
        "FROM information_schema.table_constraints AS tc "
        "JOIN information_schema.key_column_usage AS kcu "
        "  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
        "JOIN information_schema.columns AS c "
        "  ON kcu.table_name = c.table_name AND kcu.table_schema = c.table_schema AND kcu.column_name = c.column_name "
        "WHERE tc.constraint_type = 'PRIMARY KEY' "
        "  AND tc.table_name = " + worker.quote(tableName) +
        " ORDER BY kcu.ordinal_position;";

    pqxx::result pk_res = worker.exec(pk_query);

    std::map<std::string, std::string> pkInfo;
    for (const auto& row : pk_res) {
        pkInfo[row[0].as<std::string>()] = row[1].as<std::string>();
    }
    return pkInfo; 
}

bool Model::isConnectionReady() {
    if (!conn || !dbClient)
        return false;
    return true;
}

void Model::reconnect() {
    try {
        if (!conn) {
            std::string cS = "host=localhost port=8040 dbname=Lab1 user=postgres password=4242";
            conn = std::make_unique<pqxx::connection>(cS);
        }
        else {
            dbClient = drogon::orm::DbClient::newPgClient("host=localhost port=8040 dbname=Lab1 connect_timeout=10 user=postgres password=4242", 1);
        }
    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}
    
const std::vector<std::string>& Model::getTableNames() {
    return tableNames;
}

const std::vector<std::string>& Model::getTableColumnsNames(const std::string& TableName) {

    if (!conn)
        reconnect();

    auto it = columnNames.find(TableName);
    if (it != columnNames.end())
        return it->second;

    std::vector<std::string> names;
    try {
        pqxx::work worker(*conn);
        pqxx::result res = worker.exec("SELECT column_name FROM information_schema.columns WHERE table_name = " + worker.quote(TableName) +
            " AND table_schema = 'public' ORDER BY ordinal_position;");

        for (const auto& row : res)
            names.push_back(row[0].as<std::string>());

    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
        return names;
    }

    columnNames[TableName] = names;

    return columnNames.at(TableName);
}

const drogon::orm::Result& Model::getTableData(const std::string& TableName) {
    if (tableData.first != TableName)
        return requestTableData(TableName);
    return tableData.second;
}

void Model::generateRandomDataForTable(const std::string& tableName, int rowsCount) {
    try {
        if (!conn)
            reconnect();

        pqxx::work worker(*conn);

        if (tableName.find("teacher_subject") != std::string::npos)
        {
            // Перевіряємо, чи є вхідні дані
            pqxx::result t_count = worker.exec("SELECT COUNT(*) FROM Teacher;");
            pqxx::result s_count = worker.exec("SELECT COUNT(*) FROM Subject;");
            if (t_count[0][0].as<int>() == 0 || s_count[0][0].as<int>() == 0) {
                throw std::runtime_error("cannot generate links - Teacher or Subject table is empty.");
            }
            
            std::string insertSQL =
                "INSERT INTO teacher_subject (teacher_id, subject_id) "
                "SELECT t.teacher_id, s.subject_id "
                "FROM Teacher t "
                "CROSS JOIN Subject s "
                "LEFT JOIN teacher_subject ts "
                "  ON t.teacher_id = ts.teacher_id AND s.subject_id = ts.subject_id "
                "WHERE ts.teacher_id IS NULL " 
                "ORDER BY random() "
                "LIMIT $1"; 

            worker.exec(insertSQL, rowsCount);
            worker.commit();
            requestTableData(tableName);
            return;
        }

        pqxx::result fks = worker.exec("(SELECT kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column "
            "FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name "
            "JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name "
            "WHERE tc.table_name = " + worker.quote(tableName) + " AND tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public')");

        std::vector<std::string> missingTables;
        for (auto const& row : fks) {
            std::string refTable = row["foreign_table"].c_str();
            pqxx::result count = worker.exec("SELECT COUNT(*) FROM " + worker.quote_name(refTable) + ";");
            if (count[0][0].as<int>() == 0)
                missingTables.push_back(refTable);
        }

        if (!missingTables.empty()) {
            std::string msg = "missing data in related tables: ";
            for (size_t i = 0; i < missingTables.size(); ++i) {
                msg += missingTables[i] + (i + 1 < missingTables.size() ? ", " : "");
            }
            throw std::runtime_error(msg);
        }

        pqxx::result all_cols = worker.exec("(SELECT column_name, data_type, character_maximum_length, column_default FROM information_schema.columns "
            "WHERE table_name = " + worker.quote(tableName) + " AND table_schema = 'public' ORDER BY ordinal_position)");

        std::string colsList, valsList;
        std::vector<pqxx::row> colsToInsert;

        // def val
        for (auto const& row : all_cols) {
            if (row["column_default"].is_null()) {
                colsToInsert.push_back(row);
            }
        }

        if (colsToInsert.empty()) {
            throw std::runtime_error("No columns to insert");
        }

        for (size_t i = 0; i < colsToInsert.size(); ++i) {
            colsList += worker.quote_name(colsToInsert[i]["column_name"].c_str());
            if (i + 1 < colsToInsert.size()) colsList += ", ";
        }


        for (size_t i = 0; i < colsToInsert.size(); ++i) {
            std::string colName = colsToInsert[i]["column_name"].c_str();
            std::string colType = colsToInsert[i]["data_type"].c_str();
            auto colLength = colsToInsert[i]["character_maximum_length"];
            bool isFK = false;
            std::string fkSelect;

            for (auto const& fk : fks) {
                if (fk["column_name"].c_str() == colName) {
                    isFK = true;
                    fkSelect = "(SELECT " + worker.quote_name(fk["foreign_column"].c_str()) +
                        " FROM " + worker.quote_name(fk["foreign_table"].c_str()) +
                        " ORDER BY random() LIMIT 1 OFFSET (s.i * 0))";

                    break;
                }
            }

            if (isFK) {
                valsList += fkSelect;
            }
            else if (colType.find("char") != std::string::npos || colType == "text") {

                std::string generator_sql;
                bool apply_substr = true;

                if (colName.find("first_name") != std::string::npos) {
                    generator_sql = reinterpret_cast<const char*>(u8"(ARRAY['Іван', 'Петро', 'Марія', 'Ярослав', 'Анна', 'Володимир', "
                        u8"'Василь', 'Ганна', 'Вікторія', 'Анастасія', 'Денис', 'Артем', 'Руслан', 'Михайло', 'Микита'])[floor(random() * 15 + 1)]");
                }
                else if (colName.find("last_name") != std::string::npos) {
                    generator_sql = reinterpret_cast<const char*>(u8"(ARRAY['Шевченко', 'Коваленко', 'Петренко', 'Іваненко', 'Бойко', 'Мельник', "
                        u8"'Одарченко', 'Порошенко', 'Мироненко', 'Сектим', 'Гончаренко', 'Винник', 'Боженко', 'Файнберг'])[floor(random() * 14 + 1)]");
                }
                else if (colName.find("status") != std::string::npos) {
                    generator_sql = reinterpret_cast<const char*>(u8"(ARRAY['Присутній', 'Відсутній'])[floor(random() * 2 + 1)]");
                }
                else if (colName.find("class_type") != std::string::npos) {
                    generator_sql = reinterpret_cast<const char*>(u8"(ARRAY['Практика', 'Лекція', 'Лабораторна'])[floor(random() * 3 + 1)]");
                }
                else if (colName.find("email") != std::string::npos) {
                    generator_sql = "substr(md5(random()::text), 1, 5) || '.' || substr(md5(random()::text), 1, 7) || "
                        "(ARRAY['@gmail.com', '@ukr.net', '@kpi.ua'])[floor(random() * 3 + 1)]";
                }
                else if (colName.find("group_name") != std::string::npos) {
                    generator_sql = reinterpret_cast<const char*>(u8"(ARRAY['КВ', 'КМ', 'КП'])[floor(random() * 3 + 1)] || '-' || "
                        "floor(random() * 90000 + 10000)::int || '-' || gen_random_uuid()::text");
                }
                else if (colName.find("subject_name") != std::string::npos) {
                    generator_sql = reinterpret_cast<const char*>(u8"(ARRAY['СОНС', 'АМО', 'КЕ', 'ЗІКС', 'БДЗУ', 'КС'])[floor(random() * 6 + 1)] "
                        "|| '-' || gen_random_uuid()::text");
                }
                else if (colName.find("faculty") != std::string::npos) {
                    generator_sql = reinterpret_cast<const char*>(u8"(ARRAY['ФПМ', 'ФМФ', 'ФІОТ'])[floor(random() * 3 + 1)]");
                }
                else {
                    std::string len = "8";
                    if (!colLength.is_null()) {
                        int dbLen = colLength.as<int>();
                        len = (std::to_string)((std::min)(dbLen, 32)); 
                    }
                    generator_sql = "substr(md5(random()::text), 1, " + len + ")";
                    apply_substr = false; 
                }


                if (apply_substr && !colLength.is_null()) {
                    valsList += "substr(" + generator_sql + ", 1, " + colLength.as<std::string>() + ")";
                }
                else {
                    valsList += generator_sql;
                }
            }
            else if (colType.find("int") != std::string::npos) {
                valsList += "floor(random()*1000)::int";
            }
            else if (colType.find("date") != std::string::npos || colType.find("timestamp") != std::string::npos) {
                valsList += "now() - (random() * interval '10 years')";
            }
            else if (colType == "boolean") {
                valsList += "random() > 0.5";
            }
            else {
                valsList += "NULL";
            }

            if (i + 1 < colsToInsert.size()) valsList += ", ";
        }

        std::string insertSQL = "INSERT INTO " + worker.quote_name(tableName) +
            " (" + colsList + ") " +
            " SELECT " + valsList +
            " FROM generate_series(1, " + std::to_string(rowsCount) + ") AS s(i);";

        worker.exec(insertSQL.c_str()).no_rows();

        worker.commit();
        requestTableData(tableName);
    }
    catch (std::exception const& e) {
        throw std::runtime_error(e.what());
    }
}

void Model::deleteDataRange(const std::string& tableName, const drogon::orm::Result& dataToDelete, int rowFrom, int rowTo)
{
    if (tableName == unquoteIdentifier(Student::tableName)) {
        deleteDataRangeOrm<Student>(dataToDelete, rowFrom, rowTo);
    }
    else if (tableName == unquoteIdentifier(Teacher::tableName)) {
        deleteDataRangeOrm<Teacher>(dataToDelete, rowFrom, rowTo);
    }
    else if (tableName == unquoteIdentifier(Subject::tableName)) {
        deleteDataRangeOrm<Subject>(dataToDelete, rowFrom, rowTo);
    }
    else if (tableName == unquoteIdentifier(StudentGroup::tableName)) {
        deleteDataRangeOrm<StudentGroup>(dataToDelete, rowFrom, rowTo);
    }
    else if (tableName == unquoteIdentifier(Attendance::tableName)) {
        deleteDataRangeOrm<Attendance>(dataToDelete, rowFrom, rowTo);
    }
    else if (tableName == unquoteIdentifier(TeacherSubject::tableName)) {
        deleteDataRangeOrm<TeacherSubject>(dataToDelete, rowFrom, rowTo);
    }
    else if (tableName == unquoteIdentifier(Class::tableName)) {
        deleteDataRangeOrm<Class>(dataToDelete, rowFrom, rowTo);
    }
    else {
        throw std::runtime_error("Unknown table name for deletion: " + tableName);
    }

    requestTableData(tableName);
}

template <typename T>
void Model::deleteDataRangeOrm(const drogon::orm::Result& dataToDelete, int rowFrom, int rowTo)
{
    if (rowFrom > rowTo) {
        std::swap(rowFrom, rowTo);
    }
    if (dataToDelete.empty() || rowFrom < 0 || rowTo >= dataToDelete.size()) {
        throw std::runtime_error("Invalid row indices or empty data set.");
    }

    drogon::orm::Mapper<T> mapper(dbClient);
    constexpr size_t pk_column_index = 0;

    try {
        auto trans = dbClient->newTransaction();
        drogon::orm::Mapper<T> transMapper(trans); 

        for (int i = rowFrom; i <= rowTo; ++i)
        {
            const auto& row = dataToDelete[i];

            T tmpModel(row, pk_column_index);
            auto primaryKey = tmpModel.getPrimaryKey();

            transMapper.deleteByPrimaryKey(primaryKey);
        }
    }
    catch (const drogon::orm::DrogonDbException& e) {
        throw std::runtime_error("Deleting data error (ORM): " + std::string(e.base().what()));
    }
}

template <typename T>
void Model::updateDataCellOrm(const drogon::orm::Row& dataRow, const std::string& changedCellName, const std::string& dataToChange)
{
    auto trans = dbClient->newTransaction();
    drogon::orm::Mapper<T> mapper(trans);

    try {
        T tempModel(dataRow, 0); 
        auto primaryKey = tempModel.getPrimaryKey();

        T modelToUpdate = mapper.findByPrimaryKey(primaryKey);

        // constexpr тут потрібен через мою 'універсальну' реалізацію
        // без нього будуть помилки бо не будемо знати на етапі виконання, чи справді існують такі методи у класі Т
        if constexpr (std::is_same_v<T, StudentGroup>)
        {
            if (changedCellName == "group_name") {
                modelToUpdate.setGroupName(dataToChange);
            }
            else if (changedCellName == "faculty") {
                modelToUpdate.setFaculty(dataToChange);
            }
        }
        else if constexpr (std::is_same_v<T, Student>)
        {
            if (changedCellName == "first_name") {
                modelToUpdate.setFirstName(dataToChange);
            }
            else if (changedCellName == "last_name") {
                modelToUpdate.setLastName(dataToChange);
            }
            else if (changedCellName == "email") {
                modelToUpdate.setEmail(dataToChange);
            }
            else if (changedCellName == "group_id") {
                modelToUpdate.setGroupId(safeStoll(dataToChange, changedCellName));
            }
        }
        else if constexpr (std::is_same_v<T, Teacher>)
        {
            if (changedCellName == "first_name") {
                modelToUpdate.setFirstName(dataToChange);
            }
            else if (changedCellName == "last_name") {
                modelToUpdate.setLastName(dataToChange);
            }
        }
        else if constexpr (std::is_same_v<T, Subject>)
        {
            if (changedCellName == "subject_name") {
                modelToUpdate.setSubjectName(dataToChange);
            }
        }
        else if constexpr (std::is_same_v<T, Class>)
        {
            if (changedCellName == "subject_id") {
                modelToUpdate.setSubjectId(safeStoll(dataToChange, changedCellName));
            }
            else if (changedCellName == "teacher_id") {
                modelToUpdate.setTeacherId(safeStoll(dataToChange, changedCellName));
            }
            else if (changedCellName == "class_date") {
                modelToUpdate.setClassDate(trantor::Date::fromDbString(dataToChange));
            }
            else if (changedCellName == "class_type") {
                modelToUpdate.setClassType(dataToChange);
            }
        }
        else if constexpr (std::is_same_v<T, Attendance>)
        {
            if (changedCellName == "class_id") {
                modelToUpdate.setClassId(safeStoll(dataToChange, changedCellName));
            }
            else if (changedCellName == "student_id") {
                modelToUpdate.setStudentId(safeStoll(dataToChange, changedCellName));
            }
            else if (changedCellName == "status") {
                if (dataToChange != "Присутній" && dataToChange != "Відсутній") {
                    throw std::runtime_error("Wrong status! Should be 'Присутній' or 'Відсутній'");
                }
                modelToUpdate.setStatus(dataToChange);
            }
        }
        else if constexpr (std::is_same_v<T, TeacherSubject>) {
            if (changedCellName == "teacher_id") {
                modelToUpdate.setTeacherId(safeStoll(dataToChange, changedCellName));
            }
            else if (changedCellName == "subject_id") {
                modelToUpdate.setSubjectId(safeStoll(dataToChange, changedCellName));
            }
        }
        else {
            throw std::runtime_error("Unknown or unsupported column '" + changedCellName + "' for table/model.");
        }

        mapper.update(modelToUpdate);
        
    }
    catch (const drogon::orm::DrogonDbException& e) {
        throw std::runtime_error("ORM Update error: " + std::string(e.base().what()));
    }
}

void Model::updateDataCell(const std::string& tableName, const drogon::orm::Row& dataRow, const std::string& changedCellName, const std::string& dataToChange)
{
    if (dataRow.size() <= 0) {
        throw std::runtime_error("Nothing to update?");
    }
    else if (changedCellName.find("_id") != std::string::npos) {
        throw std::runtime_error("Please don't try to change ID's");
    }

    if (tableName == unquoteIdentifier(StudentGroup::tableName)) {
        updateDataCellOrm<StudentGroup>(dataRow, changedCellName, dataToChange);
    }
    else if (tableName == unquoteIdentifier(Student::tableName)) {
        updateDataCellOrm<Student>(dataRow, changedCellName, dataToChange);
    }
    else if (tableName == unquoteIdentifier(Teacher::tableName)) {
        updateDataCellOrm<Teacher>(dataRow, changedCellName, dataToChange);
    }
    else if (tableName == unquoteIdentifier(Subject::tableName)) {
        updateDataCellOrm<Subject>(dataRow, changedCellName, dataToChange);
    }
    else if (tableName == unquoteIdentifier(Class::tableName)) {
        updateDataCellOrm<Class>(dataRow, changedCellName, dataToChange);
    }
    else if (tableName == unquoteIdentifier(Attendance::tableName)) {
        updateDataCellOrm<Attendance>(dataRow, changedCellName, dataToChange);
    }
    else if (tableName == unquoteIdentifier(TeacherSubject::tableName)) {
        throw std::runtime_error("Cannot update individual cells in association table: " + tableName);
    }
    else {
        throw std::runtime_error("Unknown or unsupported table name for update: " + tableName);
    }
    requestTableData(tableName);
}

void Model::clearAllTables(const std::string currentTableName) {
    try {
        pqxx::work worker(*conn);
        std::string request = "TRUNCATE TABLE ";

        std::vector<std::string> names = getTableNames();
        for (const auto& name : names)
            request += name + ", ";
        request.pop_back();
        request.pop_back();
        request += " RESTART IDENTITY CASCADE;";

        worker.exec(request);
        worker.commit();
        requestTableData(currentTableName);
    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

template <typename T>
void Model::insertDataOrm(const std::map<std::string, std::string>& data)
{
    T newRecord;
    auto trans = dbClient->newTransaction();
    drogon::orm::Mapper<T> mapper(trans);

    try {
        for (const auto& pair : data)
        {
            const std::string& colName = pair.first;
            const std::string& colValue = pair.second;

            if (colValue.empty()) {
                continue;
            }

            if constexpr (std::is_same_v<T, StudentGroup>)
            {
                if (colName == "group_id") newRecord.setGroupId(safeStoll(colValue, colName)); 
                else if (colName == "group_name") newRecord.setGroupName(colValue);
                else if (colName == "faculty") newRecord.setFaculty(colValue);
            }
            else if constexpr (std::is_same_v<T, Student>)
            {
                if (colName == "student_id") newRecord.setStudentId(safeStoll(colValue, colName)); 
                else if (colName == "first_name") newRecord.setFirstName(colValue);
                else if (colName == "last_name") newRecord.setLastName(colValue);
                else if (colName == "email") newRecord.setEmail(colValue);
                else if (colName == "group_id") newRecord.setGroupId(safeStoll(colValue, colName)); 
            }
            else if constexpr (std::is_same_v<T, Teacher>)
            {
                if (colName == "teacher_id") newRecord.setTeacherId(safeStoll(colValue, colName)); // PK
                else if (colName == "first_name") newRecord.setFirstName(colValue);
                else if (colName == "last_name") newRecord.setLastName(colValue);
            }
            else if constexpr (std::is_same_v<T, Subject>)
            {
                if (colName == "subject_id") newRecord.setSubjectId(safeStoll(colValue, colName)); // PK
                else if (colName == "subject_name") newRecord.setSubjectName(colValue);
            }
            else if constexpr (std::is_same_v<T, TeacherSubject>)
            {
                if (colName == "teacher_id") newRecord.setTeacherId(safeStoll(colValue, colName)); // Компонент PK/FK
                else if (colName == "subject_id") newRecord.setSubjectId(safeStoll(colValue, colName)); // Компонент PK/FK
            }
            else if constexpr (std::is_same_v<T, Class>)
            {
                if (colName == "class_id") newRecord.setClassId(safeStoll(colValue, colName)); // PK
                else if (colName == "subject_id") newRecord.setSubjectId(safeStoll(colValue, colName)); // FK
                else if (colName == "teacher_id") newRecord.setTeacherId(safeStoll(colValue, colName)); // FK
                else if (colName == "class_date") newRecord.setClassDate(trantor::Date::fromDbString(colValue));
                else if (colName == "class_type") newRecord.setClassType(colValue);
            }
            else if constexpr (std::is_same_v<T, Attendance>)
            {
                if (colName == "attendance_id") newRecord.setAttendanceId(safeStoll(colValue, colName)); // PK
                else if (colName == "class_id") newRecord.setClassId(safeStoll(colValue, colName)); // FK
                else if (colName == "student_id") newRecord.setStudentId(safeStoll(colValue, colName)); // FK
                else if (colName == "status") {
                    if (colValue != "Присутній" && colValue != "Відсутній") {
                        throw std::runtime_error("Wrong status! Should be 'Присутній' or 'Відсутній'");
                    }
                    newRecord.setStatus(colValue);
                }
            }
            else {
                throw std::runtime_error("Unknown field '" + colName + "' in model for insertion.");
            }
        }

        mapper.insert(newRecord);
    }
    catch (const drogon::orm::DrogonDbException& e) {
        throw std::runtime_error("ORM Insert error: " + std::string(e.base().what()));
    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

void Model::Model::insertData(const std::string& tableName, const std::map<std::string, std::string>& data)
{
    if (data.empty())
        throw std::runtime_error("No data to insert");

    if (tableName == unquoteIdentifier(StudentGroup::tableName)) {
        insertDataOrm<StudentGroup>(data);
    }
    else if (tableName == unquoteIdentifier(Student::tableName)) {
        insertDataOrm<Student>(data);
    }
    else if (tableName == unquoteIdentifier(Teacher::tableName)) {
        insertDataOrm<Teacher>(data);
    }
    else if (tableName == unquoteIdentifier(Subject::tableName)) {
        insertDataOrm<Subject>(data);
    }
    else if (tableName == unquoteIdentifier(TeacherSubject::tableName)) {
        insertDataOrm<TeacherSubject>(data);
    }
    else if (tableName == unquoteIdentifier(Class::tableName)) {
        insertDataOrm<Class>(data);
    }
    else if (tableName == unquoteIdentifier(Attendance::tableName)) {
        insertDataOrm<Attendance>(data);
    }
    else {
        throw std::runtime_error("Unknown or unsupported table name for insertion: " + tableName);
    }
    requestTableData(tableName);
}

template <typename ParentT>
typename ParentT::PrimaryKeyType Model::findParentId(const std::string& parentColName, const std::string& searchValue)
{
    drogon::orm::Mapper<ParentT> mapper(dbClient);

    drogon::orm::Criteria criteria(parentColName, drogon::orm::CompareOperator::EQ, searchValue);

    try {
        ParentT parentRecord = mapper.findOne(criteria);

        return parentRecord.getPrimaryKey();

    }
    catch (const drogon::orm::DrogonDbException& e) {
        if (std::string(e.base().what()).find("No result found") != std::string::npos) {
            throw std::runtime_error("Parent record not found for value: " + searchValue);
        }
        throw e; // Прокидаємо інші помилки БД
    }
}

void Model::insertChildData( const std::string& childTableName, std::map<std::string, std::string> data, const std::string& parentTableName,
    const std::string& parentColName, const std::string& foreignKeyName)
{
    if (data.find(parentColName) == data.end()) {
        throw std::runtime_error("Error: Key field '" + parentColName + "' is missing in input data.");
    }
    std::string searchValue = data[parentColName];

    std::any foundPk;

    if (parentTableName == unquoteIdentifier(StudentGroup::tableName)) {
        foundPk = findParentId<StudentGroup>(parentColName, searchValue);
    }
    else if (parentTableName == unquoteIdentifier(Student::tableName)) {
        foundPk = findParentId<Student>(parentColName, searchValue);
    }
    else if (parentTableName == unquoteIdentifier(Teacher::tableName)) {
        foundPk = findParentId<Teacher>(parentColName, searchValue);
    }
    else if (parentTableName == unquoteIdentifier(Subject::tableName)) {
        foundPk = findParentId<Subject>(parentColName, searchValue);
    }
    else if (parentTableName == unquoteIdentifier(Class::tableName)) {
        foundPk = findParentId<Class>(parentColName, searchValue);
    }
    else {
        throw std::runtime_error("Unsupported parent table for PK lookup: " + parentTableName);
    }

    data.erase(parentColName);

    // Конвертація PK з std::any назад у рядок (припускаємо int64_t)
    if (foundPk.type() == typeid(int64_t)) {
        data[foreignKeyName] = std::to_string(std::any_cast<int64_t>(foundPk));
    }
    else {
        throw std::runtime_error("Unsupported PK type received. Expected int64_t.");
    }


    if (childTableName == unquoteIdentifier(StudentGroup::tableName)) {
        insertDataOrm<StudentGroup>(data);
    }
    else if (childTableName == unquoteIdentifier(Student::tableName)) {
        insertDataOrm<Student>(data);
    }
    else if (childTableName == unquoteIdentifier(Teacher::tableName)) {
        insertDataOrm<Teacher>(data);
    }
    else if (childTableName == unquoteIdentifier(Subject::tableName)) {
        insertDataOrm<Subject>(data);
    }
    else if (childTableName == unquoteIdentifier(TeacherSubject::tableName)) {
        insertDataOrm<TeacherSubject>(data);
    }
    else if (childTableName == unquoteIdentifier(Class::tableName)) {
        insertDataOrm<Class>(data);
    }
    else if (childTableName == unquoteIdentifier(Attendance::tableName)) {
        insertDataOrm<Attendance>(data);
    }
    else {
        throw std::runtime_error("Unsupported child table for insertion: " + childTableName);
    }
}

pqxx::result Model::executeCustomQuery(const std::string& request, pqxx::params params) {
    try {
        pqxx::work worker(*conn);
        return worker.exec(request, params);
    }
    catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}