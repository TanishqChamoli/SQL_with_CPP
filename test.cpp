#include <iostream>
#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <chrono>
#include<bits/stdc++.h>

using namespace std;

#define URL "tcp://127.0.0.1:3306"
#define USER "root"
#define PASSWORD "password"

class data_fetch
{
public:
    struct out{
        unordered_map<string,int> all_sites;
        long int devices_today;
        long int devices_ndays;
        set<string> hosts;
        set<string> country;
        unordered_map<string,int> dead;
        unordered_map<string,int> country_count;
    };
    out objOut;
    void dump_out(){
        cout<<"All Sites"<<endl;
        for(auto &x: objOut.all_sites){
            cout<<x.first<<" "<<x.second;
            cout<<endl;
        }
        cout<<"Dead Tokens"<<endl;
        for(auto &x: objOut.dead){
            cout<<x.first<<" "<<x.second;
            cout<<endl;
        }
        cout<<"Country Count"<<endl;
        for(auto &x: objOut.country_count){
            cout<<x.first<<" "<<x.second;
            cout<<endl;
        }
        cout<<"Hosts"<<endl;
        for (auto itr = objOut.hosts.begin(); itr != objOut.hosts.end(); itr++)
        {
            cout << *itr<<" ";
            cout<<endl;
        }
        cout<<"Country"<<endl;
        for (auto itr = objOut.country.begin(); itr != objOut.country.end(); itr++)
        {
            cout << *itr<<" ";
            cout<<endl;
        }
        cout<<"Devices Today"<<endl;
        cout<<objOut.devices_today<<endl;
        cout<<"N Day Devices"<<endl;
        cout<<objOut.devices_ndays<<endl;
    }
    void devices_all(int rep)
    {
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("SELECT * FROM devices");
                while (res->next())
                {
                    //for (int i = 1; i < 12; i++)
                    //{
                        //cout << res->getString(1) << "\t";
                    //}
                    //cout << "\n";
                }
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
        }
        delete con;
    }
    int devices_today(int rep){
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("SELECT count(id) FROM devices WHERE token_date >= CURRENT_DATE");
                while (res->next())
                    objOut.devices_today = res->getInt(1);
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
            con->commit();
        }
        delete con;
        return objOut.devices_today;
    }
    long long int devices_nday(int rep,char days){
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("SELECT count(id) FROM devices WHERE token_date >= CURRENT_DATE - "+to_string(days));
                while (res->next())
                {
                    objOut.devices_ndays = res->getInt(1);
                    //for (int i = 1; i < 12; i++)
                    //{
                        //cout << res->getString(1) << "\t";
                    //}
                    //cout << "\n";
                }
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
            con->commit();
        }
        delete con;
        return objOut.devices_ndays;
    }
    set<string> country(int rep){
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("select distinct country from devices");
                while (res->next())
                {
                    objOut.country.insert(res->getString(1));
                    //cout<<res->getString(1)<<endl;
                    //for (int i = 1; i < 12; i++)
                    //{
                        //cout << res->getString(1) << "\t";
                    //}
                    //cout << "\n";
                }
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
            con->commit();
        }
        delete con;
        return objOut.country;
    }
    unordered_map<string,int> all_sites(int rep){
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("SELECT DISTINCT host,COUNT(*) as total FROM devices GROUP BY host ORDER BY total DESC ");
                while (res->next())
                {
                    // cout<<res->getString(1);
                    // cout<<res->getString(2);
                    objOut.all_sites[res->getString(1)]=res->getInt(2);
                    // cout<<endl;
                }
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
            con->commit();
        }
        delete con;
        return objOut.all_sites;
    }
    set<string> host(int rep){
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("select distinct host from devices");
                while (res->next())
                {
                    objOut.hosts.insert(res->getString(1));
                    //cout<<res->getString(1)<<endl;
                    //for (int i = 1; i < 12; i++)
                    //{
                        //cout << res->getString(1) << "\t";
                    //}
                    //cout << "\n";
                }
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
            con->commit();
        }
        delete con;
        return objOut.hosts;
    }
    unordered_map<string,int> dead(int rep){
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("SELECT DISTINCT token_status,COUNT(*) as total FROM `devices` GROUP BY token_status");
                while (res->next())
                    objOut.dead[res->getString(1)] = res->getInt(2);
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
            con->commit();
        }
        delete con;
        return objOut.dead;
    }
    unordered_map<string,int> country_count(int rep){
        sql::Connection *con;
        sql::Driver *driver;
        driver = get_driver_instance();
        con = driver->connect(URL, USER, PASSWORD);
        con->setSchema("admin_push");
        for (int i = 0; i < rep; i++)
        {
            try
            {
                sql::Statement *stmt;
                sql::ResultSet *res;
                if (!con->isValid())
                    con->reconnect();
                stmt = con->createStatement();
                res = stmt->executeQuery("SELECT DISTINCT country,COUNT(*) as total FROM `devices` GROUP BY country ORDER BY total DESC");
                while (res->next())
                    objOut.country_count[res->getString(1)] = res->getInt(2);
                delete res;
                delete stmt;
            }
            catch (sql::SQLException &e)
            {
                cout << "# ERR: SQLException in " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
                cout << "# ERR: " << e.what();
                cout << " (MySQL error code: " << e.getErrorCode();
                cout << ", SQLState: " << e.getSQLState() << " )" << endl;
            }
            con->commit();
        }
        delete con;
        return objOut.country_count;
    }
};
//sudo g++ -Wall -I/usr/include/cppconn -o test test.cpp -L/usr/lib -lmysqlcppconn -pthread
//dynamic joining on the basis of completion