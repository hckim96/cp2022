#define THREAD_NUM 40
#define JOIN_THREAD_NUM 100
#define FSSCAN_THREAD_NUM 100
#define MIN_WORK_SIZE 1000
#define FS_MIN_WORK_SIZE 10000
// #define MY_DEBUG

#ifdef MY_DEBUG
extern double preprocessTime;
extern double indexBuildTime;
extern double JoinHashBuildTime;
extern double JoinProbingTime;
extern double INLJoinTime;
extern double SelfJoinTime;
extern double SMJoinTime;
extern double FSScanTime;
#endif