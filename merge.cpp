#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <string>
#include <sstream>
#include <filesystem>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace fs = std::filesystem;

// Struct to represent a single market data entry.
// This struct holds the details of market data such as timestamp, symbol, and data.
// The operator< is overridden to define a custom sorting order based on the timestamp
// (ascending) and symbol (alphabetically, if timestamps are equal).
struct MarketData {
    std::string timestamp;
    std::string symbol;
    std::string data;

    bool operator<(const MarketData& other) const {
        if (timestamp != other.timestamp)
            return timestamp > other.timestamp; // Min-heap for timestamps.
        return symbol > other.symbol;          // Sort alphabetically if timestamps are equal.
    }
};

// Function to parse a line from the input file into a MarketData object.
// Each line is assumed to have a timestamp followed by data, and the symbol is derived
// from the file name.
MarketData parseLine(const std::string& line, const std::string& symbol) {
    std::istringstream iss(line);
    std::string timestamp, price, size, exchange, type;

    // Parse the line using ',' as a delimiter
    std::getline(iss, timestamp, ','); 
    std::getline(iss, price, ',');
    std::getline(iss, size, ',');
    std::getline(iss, exchange, ',');
    std::getline(iss, type);

    // Reformat the data part for easier output
    std::ostringstream dataStream;
    dataStream << ", " << price << ", " << size << ", " << exchange << ", " << type;

    return {timestamp, symbol, dataStream.str()};
}

// Function to read, sort, and write chunks of data into temporary files.
// The input files are read in chunks, sorted, and written into smaller temporary files
// to handle large data that cannot fit into memory.
std::vector<std::string> sortAndWriteChunks(const std::string& inputDir, size_t chunkSize) {
    std::vector<std::string> tempFiles;

    for (const auto& entry : fs::directory_iterator(inputDir)) {
        if (entry.is_regular_file()) {
            std::ifstream inputFile(entry.path());
            if (!inputFile.is_open()) {
                throw std::runtime_error("Failed to open input file: " + entry.path().string());
            }

            std::string symbol = entry.path().stem().string();
            std::vector<MarketData> buffer;
            std::string line;

            while (std::getline(inputFile, line)) {
                buffer.push_back(parseLine(line, symbol));
                if (buffer.size() == chunkSize) {
                    // Sort the buffer and write it to a temporary file.
                    std::sort(buffer.begin(), buffer.end());

                    std::string tempFile = "temp_" + std::to_string(tempFiles.size()) + ".txt";
                    std::ofstream tempOutput(tempFile);
                    for (const auto& data : buffer) {
                        tempOutput << data.symbol << ", " << data.timestamp << data.data << "\n";
                    }
                    tempFiles.push_back(tempFile);
                    buffer.clear();
                }
            }

            if (!buffer.empty()) {
                // Sort and write remaining data to a temporary file.
                std::sort(buffer.begin(), buffer.end());
                std::string tempFile = "temp_" + std::to_string(tempFiles.size()) + ".txt";
                std::ofstream tempOutput(tempFile);
                for (const auto& data : buffer) {
                    tempOutput << data.symbol << ", " << data.timestamp << data.data << "\n";
                }
                tempFiles.push_back(tempFile);
            }
        }
    }

    return tempFiles;
}

// Function to merge sorted temporary files into a final output file.
// Uses a priority queue (min-heap) to perform a k-way merge on the sorted temporary files.
void mergeSortedChunks(const std::vector<std::string>& tempFiles, const std::string& outputFile) {
    using HeapNode = std::pair<MarketData, size_t>;

    // Custom comparator for the min-heap.
    auto compare = [](const HeapNode& a, const HeapNode& b) { return a.first < b.first; };
    std::priority_queue<HeapNode, std::vector<HeapNode>, decltype(compare)> minHeap(compare);

    std::vector<std::ifstream> fileStreams(tempFiles.size());

    // Open all temporary files and load the first entry from each.
    for (size_t i = 0; i < tempFiles.size(); ++i) {
        fileStreams[i].open(tempFiles[i]);
        if (!fileStreams[i].is_open()) {
            throw std::runtime_error("Failed to open temporary file: " + tempFiles[i]);
        }

        std::string line;
        if (std::getline(fileStreams[i], line)) {
            std::istringstream iss(line);
            std::string timestamp, symbol, data;
            iss >> timestamp >> symbol;
            std::getline(iss, data);
            minHeap.emplace(MarketData{timestamp, symbol, data}, i);
        }
    }

    std::ofstream output(outputFile);
    if (!output.is_open()) {
        throw std::runtime_error("Failed to open output file: " + outputFile);
    }

    // Perform k-way merge.
    while (!minHeap.empty()) {
        auto [smallest, index] = minHeap.top();
        minHeap.pop();

        output << smallest.timestamp << " " << smallest.symbol << smallest.data << "\n";

        std::string line;
        if (std::getline(fileStreams[index], line)) {
            std::istringstream iss(line);
            std::string timestamp, symbol, data;
            iss >> timestamp >> symbol;
            std::getline(iss, data);
            minHeap.emplace(MarketData{timestamp, symbol, data}, index);
        }
    }

    // Close all file streams.
    for (auto& stream : fileStreams) {
        stream.close();
    }
    output.close();
}

// Main function to handle input arguments and invoke the external sort process.
// Takes input directory, output file name, and chunk size as command-line arguments.
int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Error: expected four inputs \n";
        return 1;
    }

    std::string inputDir = argv[1];
    std::string outputFile = argv[2];
    size_t chunkSize = std::stoul(argv[3]);

    try {
        // Stage 1: Sort and write chunks to temporary files.
        auto tempFiles = sortAndWriteChunks(inputDir, chunkSize);

        // Stage 2: Merge sorted chunks into the final output file.
        mergeSortedChunks(tempFiles, outputFile);

        // Clean up temporary files.
        for (const auto& tempFile : tempFiles) {
            fs::remove(tempFile);
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
