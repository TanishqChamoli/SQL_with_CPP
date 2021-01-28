#include "test.cpp"
#include <chrono> 
#include <thread>
#include <future>

using namespace std;

int main(){
    auto start = chrono::high_resolution_clock::now();

    data_fetch foo;
    
    auto future1 = async(&data_fetch::all_sites,foo, 100);
    auto future2 = async(&data_fetch::devices_nday,foo, 100,100);
    auto future3 = async(&data_fetch::devices_today,foo, 100);
    auto future4 = async(&data_fetch::country,foo, 100);
    auto future5 = async(&data_fetch::host,foo, 100);
    auto future6 = async(&data_fetch::dead,foo, 100);
    auto future7 = async(&data_fetch::country_count,foo, 100);
    
    foo.objOut.all_sites = future1.get();
    foo.objOut.devices_ndays = future2.get();
    foo.objOut.devices_today = future3.get();
    foo.objOut.country = future4.get();
    foo.objOut.hosts = future5.get();
    foo.objOut.dead = future6.get();
    foo.objOut.country_count = future7.get();

    foo.dump_out();
    
    auto stop = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(stop - start);
    cout << "Time taken by function: "<< duration.count() << "seconds" << endl;
    return 0;
}