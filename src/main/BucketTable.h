#ifndef RADIXSORT_BUCKET_TABLE_H
#define RADIXSORT_BUCKET_TABLE_H

#include "Bucket.h"
#include "Histogram.h"
#include "KeyValueRecord.h"
#include <vector>
//#include "tbb/concurrent_vector.h" //<-- for concurrent_vector

#define NUM_BUCKETS 256

class BucketTable {
public:
  BucketTable(Histogram* histo, uint32_t radix);

  uint32_t getCurrentDigit();
  uint32_t getValueSize() const;
  uint32_t getKeySize() const;
  //tbb::concurrent_vector<Bucket*> getBuckets();
  void setKeySize(uint32_t ks);
  void setValueSize(uint32_t vs);
  void allocBuckets();
  void distributeBuffer(KeyValueRecord* kv);
  bool clearHistogram();

  void advanceDigit();
  uint32_t getDigit();
  bool checkSortingEnds();

inline Bucket* getBucket(uint8_t index){
  Bucket* bckt = bucket_safelist.at(index);
  return bckt;
}

inline uint32_t getBucketLength(){
  return bucket_safelist.size();
}

/*inline void pushBuffer(KeyValueRecord* kv, uint32_t target_bucket){
  Bucket* bckt = bucket_safelist.at(target_bucket);
  bckt->pushEntry(kv);
}

//inline KeyValueRecord* popBuffer(KeyValueRecord* kv, uint32_t target_bucket){
inline KeyValueRecord* popBuffer(uint32_t target_bucket){
  Bucket* bckt = bucket_safelist.at(target_bucket);
  KeyValueRecord* kv =bckt->popEntry();
  return kv;
}*/

inline bool updateHistogram(uint32_t bucket_num){
  histogram->increment(bucket_num);
}

private:
  //tbb::concurrent_vector<Bucket*> bucket_safelist; //<-- for concurrent_vector
  std::vector<Bucket*> bucket_safelist;
  Histogram* histogram;
  uint32_t radix;
  uint32_t keySize;
  uint32_t currentdigit;
  uint32_t valueSize;
};

#endif // RADIXSORT_BUCKET_TABLE_H
