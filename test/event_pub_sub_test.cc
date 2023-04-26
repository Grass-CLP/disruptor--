// =================================
// Created by Lipson on 23-4-25.
// Email to LipsonChan@yahoo.com
// Copyright (c) 2023 Lipson. All rights reserved.
// Version 1.0
// =================================

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE EventPubSubTest

#include <boost/test/unit_test.hpp>
#include <disruptor/sequencer.h>
#include <thread>

namespace disruptor {
namespace test {

const int RING_BUFFER_SIZE = 1 << 14; // no cover TEST_SET, force into write ring-buf
const int TEST_SET = 1 << 14; // actually 2 * TEST_SET
const int SUB_THREADS = 64;

// using SequencerFixture =
//     Sequencer<int64_t, RING_BUFFER_SIZE,
//               MultiThreadedStrategy<RING_BUFFER_SIZE>, BusySpinStrategy>;

struct SequencerFixture
    : Sequencer<int64_t, RING_BUFFER_SIZE,
                MultiThreadedStrategy<RING_BUFFER_SIZE>, BusySpinStrategy> {};

void check_read(SequencerFixture *seq, bool *re) {
  *re = true;

  disruptor::Sequence reader(seq->GetCursor());
  //  auto dis_barrier = multiQueue.NewBarrier();
  SequencerFixture::Barrier dis_barrier(seq->GetSequence());
  dis_barrier.set_alerted(false);
  seq->addGatingSequences(&reader);
  //  auto cur = std::max(seq->GetCursor(), 0l);
  auto cur = std::max(seq->GetCursor(), 0l);
  *re &= cur == 0l;

  // read
  while (cur < 2 * TEST_SET - 1) {
    auto next_cur = dis_barrier.WaitFor(cur);
    while (next_cur < kFirstSequenceValue) {
      std::this_thread::yield();
      next_cur = dis_barrier.WaitFor(cur);
    }

    while (cur <= next_cur) {
      auto &event = (*seq)[cur];
      (*re) &= event == cur;
      if (event != cur) {
        *re = false;
      }
      ++cur;
    }

    reader.set_sequence(cur);
  }
}

// BOOST_FIXTURE_TEST_SUITE(Sequencer, SequencerFixture)

BOOST_AUTO_TEST_SUITE(EventPubSub)

BOOST_AUTO_TEST_CASE(VerifyPubSub) {
  SequencerFixture *multiQueue = new SequencerFixture();

  bool reader_result[SUB_THREADS] = {true};
  std::vector<std::thread> threads;
  for (bool &i : reader_result) {
    threads.emplace_back(check_read, multiQueue, &i);
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // pub
  int64_t index = (*multiQueue).Claim(TEST_SET);
  BOOST_CHECK(index > 0);
  for (int i = 0; i < TEST_SET; ++i) {
    (*multiQueue)[index - TEST_SET + 1 + i] = i;
    (*multiQueue).Publish(index - TEST_SET + 1 + i);
  }

  for (int i = 0; i < TEST_SET; ++i) {
    index = (*multiQueue).Claim();
    (*multiQueue)[index] = i + TEST_SET;
    (*multiQueue).Publish(index);
  }

  for (int i = 0; i < SUB_THREADS; i++) {
    threads[i].join();
    BOOST_CHECK(reader_result[i]);
  }
  delete multiQueue;
}

BOOST_AUTO_TEST_SUITE_END()

}  // namespace test
}  // namespace disruptor