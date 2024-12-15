#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <string>
#include <sstream>
#include <filesystem>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "algorithm"


namespace fs = std::filesystem;

// Struct representing a single market data entry.
struct MarketData {
    std::string timestamp;
    std::string symbol;
    std::string data;

    bool operator<(const MarketData& other) const {
        if (timestamp != other.timestamp)
            return timestamp > other.timestamp; // Min-heap for timestamp.
        return symbol > other.symbol;          // Sort alphabetically for same timestamp.
    }
};

// Function to read a single line from a file and parse it into a MarketData entry.
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

// Thread-safe queue to hold the smallest entries from each file.
class ThreadSafeQueue {
private:
    std::priority_queue<MarketData> queue;
    std::mutex mtx;
    std::condition_variable cv;
    bool finished = false;

public:
    void push(const MarketData& data) {
        std::lock_guard<std::mutex> lock(mtx);
        queue.push(data);
        cv.notify_one();
    }

    bool pop(MarketData& result) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this]() { return !queue.empty() || finished; });
        if (queue.empty()) return false;
        result = queue.top();
        queue.pop();
        return true;
    }

    void markFinished() {
        std::lock_guard<std::mutex> lock(mtx);
        finished = true;
        cv.notify_all();
    }
};

// function to process files in batch 
void processFilesBatch(const std::vector<fs::path>& files, size_t start, size_t end, ThreadSafeQueue& outputQueue) {
    std::priority_queue<MarketData> localHeap;
    std::vector<std::ifstream> fileStreams;
    std::unordered_map<std::string, size_t> symbolToIndex;

    for (size_t i = start; i < end; ++i) {
        fileStreams.emplace_back(files[i]);
        if (!fileStreams.back().is_open()) {
            throw std::runtime_error("Failed to open file: " + files[i].string());
        }

        // Extract symbol from the filename.
        std::string symbol = files[i].stem().string();

        // Read the first line from the file and push it to the local heap.
        std::string line;
        if (std::getline(fileStreams.back(), line)) {
            MarketData entry = parseLine(line, symbol);
            localHeap.push(entry);
            symbolToIndex[symbol] = fileStreams.size() - 1;
        }
    }

    // Merge entries from local heap and push to the shared queue.
    while (!localHeap.empty()) {
        MarketData smallest = localHeap.top();
        localHeap.pop();
        outputQueue.push(smallest);

        // Get the next entry from the corresponding file.
        size_t streamIndex = symbolToIndex[smallest.symbol];
        std::string line;
        if (std::getline(fileStreams[streamIndex], line)) {
            MarketData entry = parseLine(line, smallest.symbol);
            localHeap.push(entry);
        }
    }
}


// Function to merge market data from multiple files into a single output file using threads.
void mergeMarketData(const std::string& inputDir, const std::string& outputFile, size_t maxOpenFiles) {
    // Collect all files in the input directory.
    std::vector<fs::path> files;
    for (const auto& entry : fs::directory_iterator(inputDir)) {
        if (entry.is_regular_file()) {
            files.push_back(entry.path());
        }
    }

    size_t totalFiles = files.size();
    // Determine the number of threads based on the total number of files and hardware.
    size_t numThreads = std::min(totalFiles, (size_t)std::thread::hardware_concurrency());
    if (numThreads == 0) numThreads = 1; // Fallback to at least one thread if hardware concurrency is 0.

    size_t batchSize = (totalFiles + numThreads - 1) / numThreads;

    ThreadSafeQueue outputQueue;
    std::vector<std::thread> workers;

    // Start threads to process file batches.
    for (size_t i = 0; i < numThreads; ++i) {
        size_t start = i * batchSize;
        size_t end = std::min(start + batchSize, totalFiles);
        if (start < end) {
            workers.emplace_back(processFilesBatch, std::ref(files), start, end, std::ref(outputQueue));
        }
    }

    //Thread to write output from the shared queue.
    std::thread writerThread([&]() {
        std::ofstream output(outputFile);
        if (!output.is_open()) {
            throw std::runtime_error("Failed to open output file");
        }

        MarketData data;
        while (outputQueue.pop(data)) {
            output << data.symbol << ", " << data.timestamp << data.data << "\n";
        }

        output.close();
    });
    
    // Wait for all worker threads to finish the batch file processing.
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    // Mark the queue as finished and join the writer thread.
    outputQueue.markFinished();
    if (writerThread.joinable()) {
        writerThread.join();
    }
}

// Main function to handle input arguments and invoke the merge function.
int main(int argc, char* argv[]) {

    if (argc != 4) {
        std::cerr << "Invalid number of inputs \n";
        return 1;
    }

    std::string inputDir = argv[1];
    std::string outputFile = argv[2];
    size_t maxOpenFiles = std::stoul(argv[3]);

    try {
        mergeMarketData(inputDir, outputFile, maxOpenFiles);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
