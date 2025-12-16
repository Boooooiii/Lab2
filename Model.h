#pragma once

#include <pqxx/pqxx>
#include <drogon/drogon.h>

#include <algorithm>
#include <string>
#include <iostream>
#include <map>
#include <stdexcept>

namespace mvc {

    class Model {
    private:
        std::unique_ptr<pqxx::connection> conn;
        std::vector<std::string> tableNames;
        std::map<std::string, std::vector<std::string>> columnNames;
        std::pair<std::string, drogon::orm::Result> tableData;

        drogon::orm::DbClientPtr dbClient;

        Model();

        bool isForeignKey(const std::string& tableName, const std::string& columnName);

        const drogon::orm::Result& requestTableData(const std::string& TableName);

        std::map<std::string, std::string> getPrimaryKeyInfo(pqxx::work& worker, const std::string& tableName);

        template <typename T>
        void deleteDataRangeOrm(const drogon::orm::Result& dataToDelete, int rowFrom, int rowTo);

        template <typename T>
        void updateDataCellOrm(const drogon::orm::Row& dataRow, const std::string& changedCellName, const std::string& dataToChange);

        template <typename T>
        void insertDataOrm(const std::map<std::string, std::string>& data);

        template <typename ParentT>
        typename ParentT::PrimaryKeyType findParentId(const std::string& parentColName, const std::string& searchValue);

    public:

        static Model& getInstance() {
            static Model instance;
            return instance;
        }

        class Spawner {
            static Model& instance() {
                return getInstance();
            }

            friend class Controller;
        };

        Model(const Model&) = delete;
        void operator=(const Model&) = delete;

        bool isConnectionReady();

        void reconnect();

        const std::vector<std::string>& getTableColumnsNames(const std::string& TableName);

        const drogon::orm::Result& getTableData(const std::string& TableName);

        const std::vector<std::string>& getTableNames();

        void generateRandomDataForTable(const std::string& tableName, int rowsCount);

        void deleteDataRange(const std::string& tableName, const drogon::orm::Result& dataToDelete, int rowFrom, int rowTo);

        void updateDataCell(const std::string& tableName, const drogon::orm::Row& dataRow, const std::string& changedCellName, const std::string& dataToChange);

        void clearAllTables(const std::string currentTableName);

        void insertData(const std::string& tableName, const std::map<std::string, std::string>& data);

        void insertChildData(const std::string& childTable, std::map<std::string, std::string> data, const std::string& parentTable, 
            const std::string& parentColName, const std::string& foreignKeyName);

        pqxx::result executeCustomQuery(const std::string& request, pqxx::params params);

    };

}