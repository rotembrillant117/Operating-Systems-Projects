

#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <atomic>
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Barrier.h"
#include <algorithm>
#define ALLOCATION_ERROR "system error: Failed to allocate memory"
#define THREAD_CREATION_ERROR "system error: Failed to create threads"
#define THREAD_JOIN_ERROR "system error: Failed to join threads"
using std::atomic;
using std::vector;

typedef struct JobContext

{
    typedef struct ThreadContext
    {
        int threadId;
        JobContext * job;
        IntermediateVec threadVec;
    }ThreadContext;

    pthread_t * threadArr;
    ThreadContext * threadContexts;
    atomic<uint64_t> atomicCounter;
    atomic<uint64_t> loopAtomic;
    const InputVec &input;
    const MapReduceClient &client;
    Barrier * barrier;
    double numPairs;
    int shuffledSize;
    int multiThreadLevel;
    bool joined;
    pthread_mutex_t mappingAndReduceMutex;
    pthread_mutex_t percentageMutex;
    pthread_mutex_t emit3Mutex;
    pthread_mutex_t waitMutex;
    OutputVec & output;
    vector<IntermediateVec> intermediate;
    vector<IntermediateVec> shuffled;

}JobContext;

using ThreadContext = JobContext::ThreadContext;

/**
 * < Comparator for pairs of type IntermediatePair
 * @param pair1 The 1st pair
 * @param pair2 The 2nd.
 * @return True if K2 of pair1 is smaller than K2 of pair2, else false
 */
bool compareK2(IntermediatePair & pair1, IntermediatePair & pair2)
{
    return *(pair1.first) < *(pair2.first);
}

/**
 * = Comparator for pairs of type IntermediatePair
 * @param pair1 The 1st pair
 * @param pair2 The 2nd pair
 * @return True if K2 of pair1 is equal to K2 of pair2, else false
 */
bool compareEqualsK2(IntermediatePair & pair1, IntermediatePair & pair2)
{
    bool val1 = *(pair1.first) < *(pair2.first);
    bool val2 = *(pair2.first) < *(pair1.first);
    return !(val1 | val2);
}

/**
 * Takes care of the mapping part
 * @param context The job context, containing all necessary information for mapping.
 */
void mapping(ThreadContext * context)
{
    uint64_t oldVal;
    // each thread that reaches here will receive a task from the input vector[oldVal], when oldVal is an Atomic
    // variable to ensure that each task is done only once. The task is a pair of <K1=nullptr,V1=Some String>
    //The map is done with a specific map function that was chosen by the client
    while ((oldVal = (0x7fffffff & (context->job->loopAtomic)++)) < context->job->input.size())
    {
        context->job->client.map(context->job->input[oldVal].first, context->job->input[oldVal].second, context);
        context->job->atomicCounter++;
    }
    //Once the mapping process is finished each thread will sort its intermediate vector which holds pairs of
    //<K2=Some Char, V2=Num appearances>
    std::sort(context->threadVec.begin(), context->threadVec.end(), compareK2);
    //Then each thread will lock the mutex and add its intermediate vector to the job->intermediate vector
    // (which is a vector of sorted vectors that hold <K2=Some Char, V2=Num appearances> pairs
    pthread_mutex_lock(&(context->job->mappingAndReduceMutex));
    if (!context->threadVec.empty())
    {
        context->job->intermediate.push_back(context->threadVec);
        context->job->numPairs += context->threadVec.size();
    }
    pthread_mutex_unlock(&(context->job->mappingAndReduceMutex));
    //The barrier is here from reasons explained in mapping function, but also because we dont want thread 0 to continue
    // to shuffle process before the rest of the threads finished the mapping and sorting process
    context->job->barrier->barrier();
}

/**
 * Takes care of the shuffle phase for the first thread created.
 * @param context The job context, containing all necessary information for mapping.
 */
void shuffle(ThreadContext * context)
{
    //creates vectors such that each vector will contain pairs with the same keys, but different values.
    // These vectors are saved in the job->shuffled queue
    IntermediatePair max = context->job->intermediate[0][0];
    IntermediatePair min = context->job->intermediate[0][0];
    while (!context->job->intermediate.empty())
    {
        for (auto& v : context->job->intermediate)
        {
            if (!v.empty())
            {
                if (compareK2(max, v.back()))
                {
                    max = v.back();
                }
                if (compareK2(v.front(), min))
                {
                    min = v.front();
                }
            }
        }
        IntermediateVec ordered;
        for (auto& v : context->job->intermediate)
        {
            if (!v.empty())
            {
                while (compareEqualsK2(v.back(), max))
                {
                    ordered.push_back(v.back());
                    v.pop_back();
                    context->job->atomicCounter++;
                    if (v.empty())
                    {
                        break;
                    }
                }
            }
        }
        if (!ordered.empty())
        {
            context->job->shuffled.push_back(ordered);
        }
        if (context->job->intermediate.back().empty())
        {
            context->job->intermediate.pop_back();
        }
        if (!context->job->intermediate.empty())
        {
            max = min;
        }
    }
}

/**
 * Takes care of the reduce stage
 * @param context The job context, containing all necessary information for mapping.
 */
void reduce(ThreadContext * context)
{
    //All threads reach this function and get a task from the job->shuffled queue. We use a mutex for the
    // task assignment, and then we use the reduce function given by the client
    while ((0x7fffffff & (context->job->loopAtomic)++) < (uint64_t) context->job->shuffledSize)
    {
        pthread_mutex_lock(&(context->job->mappingAndReduceMutex));
        IntermediateVec cur = context->job->shuffled.back();
        context->job->shuffled.pop_back();
        context->job->atomicCounter++;
        pthread_mutex_unlock(&(context->job->mappingAndReduceMutex));
        context->job->client.reduce(&cur, context);
    }
}

/**
 * Maps, shuffles and reduces function, for thread 0 (first thread created).
 * @param threadContext Context of the thread, containing all info needed to perform the job.
 * @return 0 (temp int)
 */
void* mapReduceMain(void* threadContext)
{
    auto * context = (ThreadContext *)  threadContext;
    mapping(context);
    pthread_mutex_lock(&context->job->percentageMutex);
    uint64_t cur = 0x8000000000000000 + (((uint64_t) context->job->numPairs) << 31);
    context->job->atomicCounter = cur;
    pthread_mutex_unlock(&context->job->percentageMutex);

    //if there are no pairs, no reason to shuffle
    if (!context->job->intermediate.empty())
    {
        shuffle(context);
    }
    context->job->loopAtomic = 0;
    pthread_mutex_lock(&context->job->percentageMutex);
    cur = 0xc000000000000000 + (context->job->shuffled.size() << 31);
    context->job->atomicCounter = cur;
    pthread_mutex_unlock(&context->job->percentageMutex);
    context->job->shuffledSize = context->job->shuffled.size();
    context->job->barrier->barrier();
    reduce(context);
    return 0;
}

/**
 * Maps and reduces function, for the rest of the threads.
 * @param threadContext Context of the thread, containing all info needed to perform the job.
 * @return 0 (temp int)
 */
void * mapReduce(JobHandle threadContext)
{
    auto * context = (ThreadContext *)  threadContext;
    // each thread that is sent here(id > 0) goes first to mapping. Only when all threads are finished with the mapping process
    // and the sorting process (due to the barrier in the mapping function) all threads that their id > 0 end up back here
    // and wait until thread 0 finishes the shuffle process (in mapReduceMain function), there he will also meet the
    // barrier and will release the other threads to continue to the reduce process.
    mapping(context);
    context->job->barrier->barrier();
    reduce(context);
    return 0;
}

/**
 * Starts running the MapReduce algorithm and returns a JobHandle
 * @param client the task that the framework should run, of type client
 * @param inputVec a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
 * @param outputVec a vector of type std::vector<std::pair<K3*, V3*>>, to which the output elements will be added before returning
 * @param multiThreadLevel the number of worker threads to be used for running the algorithm
 * @return The JobHandle
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    // Inits the JobContext, with the amount of threads that were specified by multiThreadLevel (array of pthreads,
    // and an array of threadContexts)
    pthread_t * threadPool;
    JobContext::ThreadContext * threadsContexts;
    threadsContexts = new (std::nothrow) JobContext::ThreadContext[multiThreadLevel];
    threadPool = new (std::nothrow) pthread_t[multiThreadLevel];
    if (threadsContexts == nullptr)
    {
        std::cerr << ALLOCATION_ERROR << std::endl;
        exit(1);
    }
    else if (threadPool == nullptr)
    {
        std::cerr << ALLOCATION_ERROR << std::endl;
        exit(1);
    }
    auto * job = new (std::nothrow) JobContext{threadPool, threadsContexts,
                                               {inputVec.size() << 31}, {0}, inputVec, client,
                                               new Barrier(multiThreadLevel), 0, 0, multiThreadLevel, false,
                                               PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
                                               PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, outputVec};
    if (job == nullptr)
    {
        std::cerr << ALLOCATION_ERROR << std::endl;
        exit(1);
    }
    if (job->barrier == nullptr)
    {
        std::cerr << ALLOCATION_ERROR << std::endl;
        exit(1);
    }
    // inits the threadContexts (including thread 0, the main thread) with id = i and the pointer to the Job
    for (int i = 0; i < multiThreadLevel; i++)
    {
        job->threadContexts[i] = {i, job};
    }
    job->atomicCounter += 0x4000000000000000; // += 1 << 62
    // creates the Main thread (thread 0) which goes to its own function called mapReduceMain
    if (pthread_create(job->threadArr, NULL, mapReduceMain, job->threadContexts))
    {
        std::cerr << THREAD_CREATION_ERROR << std::endl;
        exit(1);
    }
    // creates the rest of the threads which all go to the function mapReduce
    for (int i = 1; i < multiThreadLevel; i++)
    {
        if (pthread_create(job->threadArr + i, NULL, mapReduce, job->threadContexts + i))
        {
            std::cerr << THREAD_CREATION_ERROR << std::endl;
            exit(1);
        }
    }
    return (void *) job;
}

/**
 * Saves the intermediary element in the context data structures
 * @param key The Key
 * @param value The Value
 * @param context The context used to retrieve the vector to push into
 */
void emit2 (K2* key, V2* value, void* context)
{
    auto * cont = (ThreadContext *) context;
    IntermediatePair p = {key, value};
    cont->threadVec.push_back(p);
}

/**
 * Frees all memory allocated.
 * @param job The context containing all info.
 */
void clean(JobContext * job)
{
    delete[] job->threadContexts;
    delete[] job->threadArr;
    delete job->barrier;
    delete job;
}

/**
 * Saves the output element in the context data structures (output vector)
 * @param key The key
 * @param value The value
 * @param context The context used to retrieve the output to push into
 */
void emit3 (K3* key, V3* value, void* context)
{
    auto * cont = (ThreadContext *) context;
    pthread_mutex_lock(&cont->job->emit3Mutex);
    cont->job->output.push_back(OutputPair(key, value));
    pthread_mutex_unlock(&cont->job->emit3Mutex);
}

/**
 * Gets JobHandle returned by startMapReduceFramework and waits until it is finished.
 * @param job The job returned by startMapReduce
 */
void waitForJob(JobHandle job)
{
    auto * context = (JobContext *) job;
    // in the reduce phase, it is possible that the main thread (The thread that created the rest of the threads, which
    // are multiThreadLevel in number) attempts to close the job before the threads are done with their tasks, so we
    // use pthread join and make the main thread wait until all the threads are finished.
    pthread_mutex_lock(&(context->waitMutex));

    if (context->joined)
    {
        return;
    }
    context->joined = true;
    for (int i = 0; i < context->multiThreadLevel; i++)
    {
        if (pthread_join(context->threadArr[i], NULL))
        {
            std::cerr << THREAD_JOIN_ERROR << std::endl;
            exit(1);
        }
    }
    pthread_mutex_unlock(&(context->waitMutex));
}

/**
 * Gets a JobHandle and updates the state of the job into the given JobState struct.
 * @param job The job to update into state.
 * @param state The state to update from the job.
 */
void getJobState(JobHandle job, JobState* state)
{
    auto * context = (JobContext *) job;
    pthread_mutex_lock(&context->percentageMutex);
    state->stage = (stage_t) (context->atomicCounter.load() >> 62);
    state->percentage = (float) (context->atomicCounter.load() & 0x7FFFFFFF) /
            (((context->atomicCounter.load() & 0x3FFFFFFF80000000) >> 31)) * 100;
    pthread_mutex_unlock(&context->percentageMutex);
}

/**
 * Releases all resources of a job
 * @param job The job of which its resources should be released.
 */
void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    auto * context = (JobContext *) job;
    pthread_mutex_destroy(&context->waitMutex);
    pthread_mutex_destroy(&context->emit3Mutex);
    pthread_mutex_destroy(&context->mappingAndReduceMutex);
    pthread_mutex_destroy(&context->percentageMutex);
    clean(context);
}