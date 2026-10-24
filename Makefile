CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -MMD -MP
LDFLAGS = 
TARGET = titan-packet-filter

SRC_DIR = src
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmark

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(SRCS:$(SRC_DIR)/%.cpp=$(BUILD_DIR)/%.o)
DEPS = $(OBJS:.o=.d)

TEST_SRCS = $(wildcard $(TEST_DIR)/*.cpp)
TEST_TARGET = $(BUILD_DIR)/run_tests

BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_TARGET = $(BUILD_DIR)/run_benchmark

.PHONY: all build test clean benchmark directories

all: directories $(BUILD_DIR)/$(TARGET)

directories:
	@mkdir -p $(BUILD_DIR)

build: all

$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

-include $(DEPS)

test: directories
	@if [ -d "$(TEST_DIR)" ] && [ -n "$(TEST_SRCS)" ]; then \
		echo "Building and running tests..."; \
		$(CXX) $(CXXFLAGS) $(TEST_SRCS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(TEST_TARGET) $(LDFLAGS); \
		$(TEST_TARGET); \
	else \
		echo "No tests found in '$(TEST_DIR)'. Skipping."; \
	fi

benchmark: directories
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$(BENCH_SRCS)" ]; then \
		echo "Building and running benchmarks..."; \
		$(CXX) $(CXXFLAGS) $(BENCH_SRCS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BENCH_TARGET) $(LDFLAGS); \
		$(BENCH_TARGET); \
	else \
		echo "No benchmarks found in '$(BENCH_DIR)'. Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Cleaned build artifacts."