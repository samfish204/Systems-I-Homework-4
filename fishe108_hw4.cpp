// Copyright 2020 Samuel Fisher
// lab8
// CSE278 - Section B
// Professor Vendome
// October 21, 2020
// This program creates a query and allows the user
// to supply sql commands to interact with a database.
#define MYSQLPP_MYSQL_HEADERS_BURIED
#include <mysql++/mysql++.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

void interactive() {
    // Connect to database with: database, server, userID, password
    mysqlpp::Connection myDB("cse278", "localhost",
         "cse278", "S3rul3z");

    // Variable to build query string
    std::string qString;

    // Get user input for query
    while (std::getline(std::cin, qString) && qString != std::string("quit")) {
        // Create a query
        mysqlpp::Query query = myDB.query();
        query << qString;

        try {
            // Check query is correct
            query.parse();
            // Execute query
            mysqlpp::StoreQueryResult result = query.store();
            // Results is a 2D vector of mysqlpp::String objects.
            // Print the results.
            std::cout << "-----Query Result-----" << std::endl;
            for (size_t row = 0; (row < result.size()); row++) {
                for (size_t col = 0; (col < result[row].size()); col++) {
                    std::cout << "| " << result[row][col] << " ";
                }
                std::cout << "|" << std::endl;
            }
            std::cout << "------End Result------" << std::endl;
        } catch(mysqlpp::BadQuery e) {
            std::cerr << "Query: " << qString <<std::endl;
            std::cerr << "Query is not correct SQL syntax" <<std::endl;
        }
    }
}

std::string generateLoadQuery(std::string& line) {
    // Create base insert query string
    std::string result = "INSERT INTO ";
    std::vector<std::string> strVec;

    // Split file on commas
    boost::split(strVec, line, boost::is_any_of(", "));

    // Start building query from split files (table name)
    std::string tableName = strVec[0];
    // Strings to hold attributes and values
    std::string att = "("; std::string val = "(";
    // Build attribute and value strings
    for (int i = 1; i < strVec.size(); i++) {
        att += strVec[i].substr(0, strVec[i].find(":")) + ", ";
        val+=strVec[i].substr(strVec[i].find(":")+1, strVec[i].size()-1)+", ";
    }
    att.pop_back(); att.pop_back();
    val.pop_back(); val.pop_back();
    att += ")";
    val += ");";
    // Form full query string
    result += tableName + att + " VALUES" + val;
    return result;
}

void loadData(std::string& path) {
    // Open file stream
    std::ifstream input(path);

    // Connect to database with: database, server, userID, password
    mysqlpp::Connection myDB("cse278", "localhost",
         "cse278", "S3rul3z");

    // Some necessary variables for the file IO
    std::string temp;
    std::string output;
    int count = 0;

    // Read file line-by-line
    while (getline(input, temp)) {
        // Create query string from current line
        output = generateLoadQuery(temp);
        count++;

        // Create mysql++ query
        mysqlpp::Query query = myDB.query();
        query << output;

        try {
            // Check query is correct
            query.parse();
            // Execute Query
            mysqlpp::StoreQueryResult result = query.store();

            std::cout << "Data Line " << count << " Loaded\n";
        } catch(mysqlpp::BadQuery e) {
            std::cerr << "Query: " << query << std::endl;
            std::cerr << "Query is not correct SQL syntax" <<std::endl;
        }
    }
}

void dropTable(std::string table) {
    // connect to database
    mysqlpp::Connection myDB("cse278", "localhost",
     "cse278", "S3rul3z");

    mysqlpp::Query query = myDB.query();

    // variables to store query string
    std::string output = "DROP TABLE ";
    output += table + ";";
    query << output;

    try {
        // Check query is correct
        query.parse();
        // Execute Query
        mysqlpp::StoreQueryResult result = query.store();

        std::cout << "Table " << table << " Dropped\n";
    } catch(mysqlpp::BadQuery e) {
        std::cerr << "Query: " << query << std::endl;
        std::cerr << "Query is not correct SQL syntax" <<std::endl;
    }
}

std::vector<std::string> generateCreateQuery(std::string& line) {
    std::string result = "CREATE TABLE ";
    std::vector<std::string> final;
    std::vector<std::string> strVec; std::vector<std::string> tempVec;

    // Split the file by commas
    boost::split(strVec, line, boost::is_any_of(","));

    // Start building query from split files (tableName first)
    // Variables to hold different values
    std::string tN; std::string key; bool isKey = false;
    // Loop through each string in strVec to seperate by ":"
    for (int i = 0; i < strVec.size(); i++) {
        if (i == 0) {  // take out table name
            tN = strVec[0].substr(strVec[0].find(":")+1, strVec[i].size() - 1);
            final.push_back(tN);
            result += tN + " (\n";
        } else {  // build query
            boost::split(tempVec, strVec[i], boost::is_any_of(":_"));
            for (int j = 0; j < tempVec.size(); j++) {
                std::string temp = "";
                if (tempVec[j] != std::string("key")) {
                    temp = tempVec[j] + " ";
                } else {  // format key correctly
                    key += "PRIMARY KEY(" + tempVec[0] + ")\n";
                    isKey = true;
                }
                result += temp;
            }
            result.pop_back();
            result += ",\n";
        }
    }
    if (isKey == false) {  // continue to format key correctly
        result.pop_back(); result.pop_back();
        result += "\n";
    }
    result += key + ");";
    final.push_back(result);
    return final;
}

void createTable(std::string filePath) {
    std::ifstream input(filePath);

    // connect to database
    mysqlpp::Connection myDB("cse278", "localhost",
     "cse278", "S3rul3z");

    // Some necessary variables for the file IO
    std::string temp;
    std::string output;
    std::vector<std::string> final;

    // Read file line-by-line
    while (getline(input, temp)) {
        // Create query string from current line
        final = generateCreateQuery(temp);
        output = final[1];

        // Create mysql++ query
        mysqlpp::Query query = myDB.query();
        query << output;

        try {
             // Check query is correct
             query.parse();
             // Execute Query
             mysqlpp::StoreQueryResult result = query.store();

             std::cout << "Table " << final[0] << " Created\n";
         } catch(mysqlpp::BadQuery e) {
             std::cerr << "Query: " << query << std::endl;
             std::cerr << "Query is not correct SQL syntax" <<std::endl;
         }
    }
}

// Print output of query to a specified output file
void printToFile(std::string contents, std::ostream& out) {
    out << contents;
}

std::string writeData(std::string filePath) {
    std::ifstream input(filePath); std::string output; std::string final;

    // connect to database
    mysqlpp::Connection myDB("cse278", "localhost", "cse278", "S3rul3z");


    while (getline(input, output)) {
        mysqlpp::Query query = myDB.query(); query << output;
        std::string writeOut = "-----Query Result-----\n";

        try {
            query.parse(); mysqlpp::StoreQueryResult result = query.store();

            for (size_t row = 0; (row < result.size()); row++) {
                for (size_t col = 0; (col < result[row].size()); col++) {
                    writeOut = writeOut + "| " + result[row][col].c_str() + " ";
                }
                writeOut = writeOut + "|" + "\n";
            }
            if (writeOut == std::string("-----Query Result-----\n")) {
                writeOut = "";
            } else { writeOut = writeOut + "------End Result------\n";
            }
            final = final + writeOut;
        } catch(mysqlpp::BadQuery e) {
            std::cerr << "Query: " << query << std::endl;
            std::cerr << "Query is not correct SQL syntax" << std::endl;
        }
    }
    return final;
}

int main(int argc, char *argv[]) {
    // Ensure arguments are specified
    if (argc < 2) {
        std::cerr << "Specify arguments" << std::endl;
        return 1;
    }

    std::string option = argv[1]; std::string fileIn; std::string tableName;
    if (argc > 2) {
        tableName = fileIn = argv[2];
    }

    std::ifstream input(argv[2]); std::ofstream output(argv[3]);

    if (option == std::string("-I")) {
        interactive();
    } else if (option == std::string("-L") && input.good()) {
        loadData(fileIn);
    } else if (option == std::string("-D")) {
        dropTable(tableName);
    } else if (option == std::string("-C") && input.good()) {
        createTable(tableName);
    } else if (option == std::string("-W") && input.good() && output.good()) {
        std::string result = writeData(tableName); printToFile(result, output);
    } else {
        std::cerr << "Invalid input" << std::endl;
        return 1;
    }
    // All done
    return 0;
}
