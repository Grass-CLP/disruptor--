// Copyright (c) 2011-2015, Francois Saint-Jacques
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of the disruptor-- nor the
//       names of its contributors may be used to endorse or promote products
//       derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL FRANCOIS SAINT-JACQUES BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef CACHE_LINE_SIZE_IN_BYTES     // NOLINT
#define CACHE_LINE_SIZE_IN_BYTES 64  // NOLINT
#endif                               // NOLINT
#define ATOMIC_SEQUENCE_PADDING_LENGTH \
  (CACHE_LINE_SIZE_IN_BYTES - sizeof(std::atomic<int64_t>)) / 8
#define SEQUENCE_PADDING_LENGTH                              \
  (CACHE_LINE_SIZE_IN_BYTES - sizeof(std::atomic<int64_t>) - \
   sizeof(std::string)) /                                    \
      8

#ifndef DISRUPTOR_SEQUENCE_H_  // NOLINT
#define DISRUPTOR_SEQUENCE_H_  // NOLINT

#include "disruptor/utils.h"
#include <algorithm>
#include <atomic>
#include <future>
#include <limits.h>
#include <vector>

namespace disruptor {

// TODO by lipson, int64_t ->uint64_t for cycle-able
// special cursor values
constexpr int64_t kInitialCursorValue = -1L;
constexpr int64_t kAlertedSignal = -2L;
constexpr int64_t kTimeoutSignal = -3L;
constexpr int64_t kFirstSequenceValue = kInitialCursorValue + 1L;

// Sequence counter.
class Sequence {
 public:
  // Construct a sequence counter that can be tracked across threads.
  //
  // @param initial_value for the counter.
  Sequence(int64_t initial_value = kInitialCursorValue,
           const std::string& name = "")
      : sequence_(initial_value), name_(name) {}

  // Get the current value of the {@link Sequence}.
  //
  // @return the current value.
  int64_t sequence() const {
    return sequence_.load(std::memory_order::memory_order_acquire);
  }

  // Set the current value of the {@link Sequence}.
  //
  // @param the value to which the {@link Sequence} will be set.
  void set_sequence(int64_t value) {
    sequence_.store(value, std::memory_order::memory_order_release);
  }

  // Try to compare and set the current value of the {@link Sequence}.
  //
  // @param the value to which the {@link Sequence} will be set.
  // @return success or failure
  bool compare_and_swap(int64_t old_value, int64_t new_value) {
    return (sequence_.compare_exchange_weak(old_value, new_value,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed));
  }

  // Increment and return the value of the {@link Sequence}.
  //
  // @param increment the {@link Sequence}.
  // @return the new value incremented.
  int64_t IncrementAndGet(const int64_t& increment) {
    return sequence_.fetch_add(increment,
                               std::memory_order::memory_order_release) +
           increment;
  }

  std::string name() const { return name_; }

 private:
  // padding
  int64_t padding0_[SEQUENCE_PADDING_LENGTH];
  // members
  std::atomic<int64_t> sequence_;
  std::string name_;
  // padding
  int64_t padding1_[SEQUENCE_PADDING_LENGTH];

  DISALLOW_COPY_MOVE_AND_ASSIGN(Sequence);
};

int64_t GetMinimumSequence(const std::vector<Sequence*>& sequences, int64_t minimum = LONG_MAX) {
  for (Sequence* sequence_ : sequences) {
    const int64_t sequence = sequence_->sequence();
    minimum = minimum < sequence ? minimum : sequence;
  }

  return minimum;
};

void DumpMinimumSequence(const std::vector<Sequence*>& sequences,
                         const int64_t& write_point) {
  int64_t minimum = LONG_MAX;

  Sequence* min_address = nullptr;
  for (Sequence* sequence_ : sequences) {
    const int64_t sequence = sequence_->sequence();
    if (sequence < minimum) {
      minimum = sequence;
      min_address = sequence_;
    }
  }

  if (min_address) {
    // TODO to more performance
    fprintf(stderr,
            "reader lag! writer seq(%ld) reader min seq(%ld) name(%s) "
            "address(%p)\n",
            write_point, minimum, min_address->name().c_str(), min_address);
  }
}

static const int SEQUENCES_MAX_SEQS = 1024;
/**
 * Sequences thread safety maybe. with delay delete
 */
class Sequences {
  static_assert(((SEQUENCES_MAX_SEQS > 0) &&
                 ((SEQUENCES_MAX_SEQS & (~SEQUENCES_MAX_SEQS + 1)) ==
                  SEQUENCES_MAX_SEQS)),
                "SEQUENCES_MAX_SEQS must be a positive power of 2");

 public:
  Sequences() = default;

  ~Sequences() {
    for (auto& p : delay_delete_seqs_) {
      if (p) {
        delete p;
        p = nullptr;
      }
    }
    delete gating_sequences_.load();
  }

  Sequences(const Sequences& sequences) {
    gating_sequences_ =
        sequences.gating_sequences_.load(std::memory_order_relaxed);
  }

  Sequences& operator=(const Sequences& sequences) {
    gating_sequences_ =
        sequences.gating_sequences_.load(std::memory_order_relaxed);
    return *this;
  };

  const std::vector<Sequence*>& get() {
    return *(gating_sequences_.load(std::memory_order_relaxed));
  }

  void addGatingSequences(Sequence* sequence) {
    std::vector<Sequence*>* current = nullptr;
    std::vector<Sequence*>* updated = nullptr;
    do {
      if (!updated) {
        delete updated;
      }

      current = gating_sequences_.load();
      updated = new std::vector<Sequence*>(*current);
      updated->push_back(sequence);
    } while (!gating_sequences_.compare_exchange_strong(current, updated));

    delay_delete(current);
  }

  void delGatingSequence(Sequence* sequence) {
    std::vector<Sequence*>* current = nullptr;
    std::vector<Sequence*>* updated = nullptr;
    do {
      if (!updated) {
        delete updated;
      }

      current = gating_sequences_.load();
      updated = new std::vector<Sequence*>(*current);
      updated->push_back(sequence);
      auto itr = std::remove(updated->begin(), updated->end(), sequence);
      if (itr != updated->end()) updated->erase(itr, updated->end());
    } while (!gating_sequences_.compare_exchange_strong(current, updated));

    delay_delete(current);
  }

 protected:
  void delay_delete(std::vector<Sequence*>* p) {
    int index = delay_delete_last_.fetch_add(1) & (SEQUENCES_MAX_SEQS - 1);
    if (delay_delete_seqs_[index] != nullptr) {
      delete delay_delete_seqs_[index];
    }
    delay_delete_seqs_[index] = p;
  }

 private:
  std::atomic<std::vector<Sequence*>*> gating_sequences_{
      new std::vector<Sequence*>()};
  std::array<std::vector<Sequence*>*, SEQUENCES_MAX_SEQS> delay_delete_seqs_{
      nullptr};
  std::atomic_int delay_delete_last_{0};
};

};  // namespace disruptor

#endif  // DISRUPTOR_SEQUENCE_H_ NOLINT
