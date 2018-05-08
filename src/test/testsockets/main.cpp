#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>

#include <iostream>
#include <random>
#include <string>
#include <algorithm>
#include <fstream>
#include <vector>
//#include "sample1.h"
#include "gtest/gtest.h"
#include "KeyValueRecord.h"
#include "Histogram.h"
#include "BucketTable.h"
#include "BufferMsg.h"
#include "BufferSorter.h"
#include "BaseBuffer.h"


namespace {
// The fixture for testing class Foo.
class SortTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SortTest() {
    // You can do set-up work for each test here.
    uint32_t Kbytes     = 100;
    keysize    = 10;
    valuesize  = 90;
    num_record = 1000*Kbytes/(keysize + valuesize);
    sorttype   = 1;
    buf        = new BaseBuffer();
    output     = new BaseBuffer();
    sorter     = new BufferSorter(num_record, sorttype);
  }

  virtual ~SortTest() {
    // You can do clean-up work that doesn't throw exceptions here.
    delete sorter;
    delete output;
    delete buf;
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
    buf->createRecords(num_record);
    output->createRecords(num_record);  

      std::random_device rd;
      std::default_random_engine gen = std::default_random_engine(rd());
      std::uniform_int_distribution<char> dis(0,255);

      for(uint32_t i = 0; i < num_record; i++){
        KeyValueRecord* kvr =buf->getRecords(i); 
        kvr->initRecord(keysize, valuesize);
      }

      for(uint32_t i = 0; i < num_record; i++){
        KeyValueRecord* kvr =output->getRecords(i);
        kvr->initRecord(keysize, valuesize);
      }
      
      for(int j=0; j< num_record; j++){
        KeyValueRecord* kvr =buf->getRecords(j);
        for (int index=0; index<keysize; index++){
          kvr->setKeyWithIndex(dis(gen),index);
        }
      }

      for(int j=0; j< num_record; j++){
        KeyValueRecord* kvr =buf->getRecords(j);
        for (int index=0; index<valuesize; index++){
          kvr->setValueWithIndex(dis(gen),index);
        }
      }    
  }

  bool checkKeySorted(KeyValueRecord* kvr1, KeyValueRecord* kvr2){
    for(uint32_t j = 0; j < keysize; j++){
      uint8_t k1 = kvr1->getKey(j);
      uint8_t k2 = kvr2->getKey(j);
      if(k1 < k2){
        return true;
      }
      else if (k1 > k2){
        return false;
      }
      else{ // k1 == k2 on this byte
        continue;
      }
      //printf("%" PRIu8 " ",k);
      //std::cout << +k << " ";
    }
    return false; // false for k1 <= k2
  }

  //virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  //}

  // Objects declared here can be used by all tests in the test case for Foo.
  uint32_t keysize;
  uint32_t valuesize;
  BaseBuffer* buf;
  BaseBuffer* output;
  BufferSorter* sorter;
  int sorttype;
  uint32_t num_record;
};



// Tests factorial of negative numbers.
TEST_F(SortTest, radix_sort) {
  // This test is named "Negative", and belongs to the "FactorialTest"
  // test case.
  sorter->radix_sort(buf,output,num_record);
  bool sorted = true;
  for(uint32_t i = 0; i < num_record - 1; i++){
    KeyValueRecord* kvr1 = output->getRecords(i);
    KeyValueRecord* kvr2 = output->getRecords(i+1);
    if( checkKeySorted(kvr1,kvr2) == false){
      std::cout << "unsorted record " << +i << "\n";
      for(uint32_t j = 0; j < keysize; j++){
        uint8_t k = kvr1->getKey(j);
        std::cout << +k << " ";
      }
      std::cout << "\n";
      sorted = false;
    }
    break;
  }
  EXPECT_EQ(true, sorted);
}


// Tests negative input.
/*TEST(IsPrimeTest, Negative) {
  // This test belongs to the IsPrimeTest test case.

  EXPECT_FALSE(IsPrime(-1));
  EXPECT_FALSE(IsPrime(-2));
  EXPECT_FALSE(IsPrime(INT_MIN));
}*/

}  // namespace

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    return ret;
}
