#ifndef TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_HISTOGRAM_H
#define TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_HISTOGRAM_H

#define NUM_BUCKETS 256

/**
   Histogram is an array of frequency counts that supports retrieving and
   incrementing frequencies.
 */
class Histogram {
public:
  /**
     Reset all frequency counts to 0.
   */
  inline void reset() {
    memset(frequency, 0, NUM_BUCKETS * sizeof(uint32_t));
  }

  /**
     Increment a frequency count for a specific bucket.

     \param bucket the bucket to increment
   */
  inline void increment(uint32_t bucket) {
    ++frequency[bucket];
  }

  /**
     Get the frequency count for a specific bucket.

     \param the bucket to get

     \return the bucket's frequency count
   */
  inline uint32_t getCount(uint32_t bucket) {
    return frequency[bucket];
  }

private:
  uint32_t frequency[NUM_BUCKETS];
};

#endif // TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_HISTOGRAM_H
