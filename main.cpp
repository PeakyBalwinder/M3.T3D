#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <mpi.h>

using namespace std;

// Define constants for clarity and reusability
const int MASTER_RANK = 0;
const int TOP_CONGESTED = 3;

// Define a struct to hold traffic data
struct TrafficData {
    string timestamp;
    int traffic_light_id;
    int cars_passed;
};

// Function to read traffic data from the input file
void read_traffic_data(ifstream& inputFile, vector<TrafficData>& traffic_data_buffer, int rank, int size) {
    string line;
    while (getline(inputFile, line)) {
        stringstream ss(line);
        string token;
        TrafficData data;

        // Parse the input line and extract relevant information
        getline(ss, token, ','); // Read timestamp
        data.timestamp = token.substr(11); // Extract timestamp without "Generated: "
        getline(ss, token, ':'); // Read "Traffic Light ID"
        ss >> data.traffic_light_id; // Read traffic light ID
        getline(ss, token, ':'); // Read "Cars Passed"
        ss >> data.cars_passed; // Read cars passed

        // Store data relevant to the current MPI rank
        if (data.traffic_light_id % size == rank) {
            traffic_data_buffer.push_back(data);
        }
    }
}

// Function to print the top congested traffic lights based on cars passed
void print_top_congested(const vector<TrafficData>& traffic_data_buffer, int rank) {
    map<int, int> congested_traffic_lights;

    // Calculate total cars passed for each traffic light
    for (const auto& data : traffic_data_buffer) {
        congested_traffic_lights[data.traffic_light_id] += data.cars_passed;
    }

    // Sort traffic lights based on cars passed and print top congested lights
    vector<pair<int, int>> sorted_congested(congested_traffic_lights.begin(), congested_traffic_lights.end());
    partial_sort(sorted_congested.begin(), sorted_congested.begin() + min(TOP_CONGESTED, (int)sorted_congested.size()), sorted_congested.end(),
                 [](const pair<int, int> &a, const pair<int, int> &b) { return a.second > b.second; });

    // Print congested traffic lights for the current process
    cout << "Process " << rank << " congested traffic lights:" << endl;
    for (int i = 0; i < min(TOP_CONGESTED, (int)sorted_congested.size()); ++i) {
        cout << "Traffic Light ID: " << sorted_congested[i].first << ", Total Cars Passed: " << sorted_congested[i].second << endl;
    }
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Open the input file for reading traffic data
    ifstream inputFile("data.txt");
    if (!inputFile) {
        cerr << "Error opening input file." << endl;
        MPI_Finalize();
        return 1;
    }

    // Buffer to store traffic data for each process
    vector<TrafficData> traffic_data_buffer;
    read_traffic_data(inputFile, traffic_data_buffer, rank, size);

    // Print congested traffic lights for verification
    print_top_congested(traffic_data_buffer, rank);

    // Finalize MPI environment
    MPI_Finalize();
    return 0;
}
