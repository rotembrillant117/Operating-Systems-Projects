//
// Created by taljoseph on 27/04/2021.
//

#include "uthreads.h"
#include <setjmp.h>
#include "string"
#include <list>
#include <map>
#include <signal.h>
#include <iostream>
#include <sys/time.h>
using std::string;
using std::list;
using std::map;

int x = 0;

#ifdef __x86_64__
/* code for 64 bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
                 "rol    $0x11,%0\n"
    : "=g" (ret)
    : "0" (addr));
    return ret;
}

#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
		"rol    $0x9,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}
#endif

class Thread
{
public:
    int id;
    char stack[STACK_SIZE];
    sigjmp_buf env;
    int quantums;

    Thread(int id, void (*f)(void)):
    id(id), env(), quantums(0)
    {
        if (id == 0)
        {
            return;
        }
        address_t sp, pc;

        sp = (address_t)stack + STACK_SIZE - sizeof(address_t);
        pc = (address_t)f;
        sigsetjmp(env, 1);
        (env->__jmpbuf)[JB_SP] = translate_address(sp);
        (env->__jmpbuf)[JB_PC] = translate_address(pc);
        sigemptyset(&env->__saved_mask);
    }
};

#define ERROR_BLOCK "system error: signal masking failed\n"
#define ERROR_UNBLOCK "system error: failed to unblock signals set\n"
enum state {Empty, Ready, Blocked, Running, MxBlocked, DBlocked};
int active_threads = 0;
list<Thread*> ready_threads;
Thread * running_thread = nullptr;
state threads[100]{};
struct sigaction sa{};
struct itimerval timer;

map<int, Thread*> blocked_threads;
int mutex = -1;
list<int> mxb_threads;
int total_quantums = 0;
sigset_t set{};

/**
 * Switches between threads, moves current thread to st state and runs the next thread in line
 * @param st The new state of the running thread
 */
void switch_threads(state st)
{
    int ret_val = sigsetjmp(running_thread->env, 1);
    if (ret_val == 1)
    {
        return;
    }
    threads[running_thread->id] = st;
    if (st == Ready)
    {
        ready_threads.push_back(running_thread);
    }
    running_thread = ready_threads.front();
    ready_threads.pop_front();
    threads[running_thread->id] = Running;
    running_thread->quantums++;
    total_quantums++;
    siglongjmp(running_thread->env, 1);
}

/**
 * Deletes all threads and releases allocated memory
 */
void clean()
{
    for (long unsigned int i = 0; i  < ready_threads.size(); i++)
    {
        Thread * tmp = ready_threads.front();
        ready_threads.pop_front();
        delete tmp;
    }
    auto it = blocked_threads.begin();
    while (it != blocked_threads.end())
    {
        Thread * tmp = it->second;
        it++;
        delete tmp;
    }
    delete running_thread;
}

/**
 * Block or unblock a signal
 * @param signal Int representing the signal
 * @param msg Blocked or unblocked message accordingly
 */
void sig_mask(int signal, string msg)
{
    if (sigprocmask(signal, &set, NULL))
    {
        std::cerr << msg;
        clean();
        exit(1);
    }
}

/**
 * A handler for the timer
 * @param sig Int representing the signal
 */
void switch_threads_timer(int sig)
{
    switch_threads(Ready);
}


int uthread_init(int quantum_usecs)
{
    if (quantum_usecs <= 0)
    {
        std::cerr << "thread library error: invalid quantom_usecs, should be a positive integer\n";
        return -1;
    }
    Thread * main_thread;
    main_thread = new (std::nothrow) Thread(0, nullptr);
    if (main_thread == nullptr)
    {
        std::cerr << "system error: Memory allocation failed\n";
        exit(1);
    }
    sa.sa_handler = &switch_threads_timer;
    if (sigaction(SIGVTALRM, &sa,NULL) < 0) {
        std::cerr << "system error: sigaction failed\n";
        delete main_thread;
        exit(1);
    }

    int seconds = (int) (quantum_usecs / 1e6);
    timer.it_value.tv_sec = seconds;		// first time interval, seconds part
    timer.it_value.tv_usec = quantum_usecs - (seconds * 1e6);		// first time interval, microseconds part
    timer.it_interval = timer.it_value;

    if (sigemptyset(&set))
    {
        std::cerr << "system error: failed to empty masking set\n";
        delete main_thread;
        exit(1);
    }
    if (sigaddset(&set, SIGVTALRM))
    {
        std::cerr << "system error: failed to add to masking set\n";
        delete main_thread;
        exit(1);
    }
    if (setitimer (ITIMER_VIRTUAL, &timer, NULL))
    {
        std::cerr << "system error: settimer failed\n";
        delete main_thread;
        exit(1);
    }
    running_thread = main_thread;
    active_threads++;
    threads[0] = Running;
    total_quantums++;
    running_thread->quantums++;
    return 0;
}

int uthread_spawn(void (*f)(void))
{
    sig_mask(SIG_BLOCK, ERROR_BLOCK);
    if (active_threads >= 100)
    {
        std::cerr << "thread library error: Maximum amount of threads exceeded\n";
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return -1;
    }
    Thread * new_thread;
    int i = 0;
    while (threads[i] != Empty)
    {
        i++;
    }
    new_thread = new (std::nothrow) Thread(i, f);
    if (new_thread == nullptr)
    {
        std::cerr << "system error: Memory allocation failed\n";
        clean();
        exit(1);
    }
    ready_threads.push_back(new_thread);
    threads[i] = Ready;
    active_threads++;
    sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
    return i;
}

/**
 * Unlock the mutex and releases the next mutex block thread in line if one exists.
 */
void mutex_unlock() {
    if (!mxb_threads.empty())
    {
        int tid = mxb_threads.front();
        mxb_threads.pop_front();
        if (threads[tid] == DBlocked)
        {
            threads[tid] = Blocked;
        }
        else if (threads[tid] == MxBlocked)
        {
            ready_threads.push_back(blocked_threads[tid]);
            blocked_threads.erase(tid);
            threads[tid] = Ready;
        }
    }
    mutex = -1;
}

int uthread_terminate(int tid)
{
    sig_mask(SIG_BLOCK, ERROR_BLOCK);
    if (tid == 0)
    {
        clean();
        exit(0);
    }
    else if(tid > 0 && tid < 100 && threads[tid] != Empty)
    {
        if (threads[tid] == Running)
        {
            threads[tid] = Empty;
            active_threads--;
            delete running_thread;
            running_thread = ready_threads.front();
            ready_threads.pop_front();
            threads[running_thread->id] = Running;
            running_thread->quantums++;
            total_quantums++;
            if (setitimer (ITIMER_VIRTUAL, &timer, NULL))
            {
                std::cerr << "system error: settimer failed\n";
                clean();
                exit(1);
            }
            if (mutex == tid)
            {
                mutex_unlock();
            }
            sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
            siglongjmp(running_thread->env, 1);
        }
        else if (threads[tid] == Ready)
        {
            auto it = ready_threads.begin();
            while((*it)->id != tid)
            {
                it++;
            }
            Thread * tmp = *it;
            ready_threads.erase(it);
            delete tmp;
        }
        else // threads[tid] == BLocked
        {
            auto it = blocked_threads.find(tid);
            Thread * tmp = it->second;
            blocked_threads.erase(tid);
            delete tmp;
        }
        threads[tid] = Empty;
        active_threads--;
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return 0;
    }
    else
    {
        std::cerr << "thread library error: Thread doesn't exist\n";
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return -1;
    }
}

int uthread_block(int tid)
{
    sig_mask(SIG_BLOCK, ERROR_BLOCK);
    if (tid > 0 && tid < 100 && threads[tid] != Empty)
    {
        if (threads[tid] == Running)
        {
            blocked_threads[tid] = running_thread;
            if (setitimer (ITIMER_VIRTUAL, &timer, NULL))
            {
                std::cerr << "system error: settimer failed\n";
                clean();
                exit(1);
            }
            sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
            switch_threads(Blocked);
        }
        else if (threads[tid] == Ready)
        {
            auto it = ready_threads.begin();
            while((*it)->id != tid)
            {
                it++;
            }
            threads[tid] = Blocked;
            blocked_threads[tid] = *it;
            ready_threads.erase(it);
        }
        else if (threads[tid] == MxBlocked)
        {
            threads[tid] = DBlocked;
        }
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return 0;
    }
    else
    {
        std::cerr << "thread library error: Thread doesn't exist or main thread was selected\n";
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return -1;
    }
}

int uthread_resume(int tid)
{
    sig_mask(SIG_BLOCK, ERROR_BLOCK);
    if (tid > 0 && tid < 100 && threads[tid] != Empty)
    {
        if (threads[tid] == Blocked) {
            auto it = blocked_threads.find(tid);
            threads[tid] = Ready;
            ready_threads.push_back(it->second);
            blocked_threads.erase(tid);
        }
        else if (threads[tid] == DBlocked)
        {
            threads[tid] = MxBlocked;
        }
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return 0;
    }
    std::cerr << "thread library error: Thread doesn't exist\n";
    sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
    return -1;
}

int uthread_mutex_lock()
{
    sig_mask(SIG_BLOCK, ERROR_BLOCK);
    if (mutex == -1) // mutex available
    {
        mutex = running_thread->id;
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return 0;
    }
    else if (running_thread->id == mutex)
    {
        std::cerr << "thread library error: Current thread is already holding a mutex\n";
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return -1;
    }
    else // mutex in use
    {
        while (mutex != -1)
        {
            mxb_threads.push_back(running_thread->id);
            blocked_threads[running_thread->id] = running_thread;
            if (setitimer (ITIMER_VIRTUAL, &timer, NULL))
            {
                std::cerr << "system error: settimer failed\n";
                clean();
                exit(1);
            }
            sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
            switch_threads(MxBlocked);
        }
        mutex = running_thread->id;
        return 0;
    }
}

int uthread_mutex_unlock()
{
    sig_mask(SIG_BLOCK, ERROR_BLOCK);
    if (mutex == -1)
    {
        std::cerr << "thread library error: Mutex already unlocked\n";
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return -1;
    }
    else if (mutex != running_thread->id)
    {
        std::cerr << "thread library error: Mutex is being held by another thread\n";
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return -1;
    }
    mutex_unlock();
    sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
    return 0;
}

int uthread_get_tid()
{
    return running_thread->id;
}

int uthread_get_total_quantums()
{
    return total_quantums;
}


int uthread_get_quantums(int tid)
{
    sig_mask(SIG_BLOCK, ERROR_BLOCK);
    if (tid < 0 || tid > 99 || threads[tid] == Empty)
    {
        std::cerr << "thread library error: Invalid Thread ID\n";
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return -1;
    }
    else if (threads[tid] == Running)
    {
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return running_thread->quantums;
    }
    else if (threads[tid] == Ready)
    {
        auto it = ready_threads.begin();
        while((*it)->id != tid)
        {
            it++;
        }
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return (*it)->quantums;
    }
    else  // Blocked or Mutex Blocked or both
    {
        auto it = blocked_threads.find(tid);
        sig_mask(SIG_UNBLOCK, ERROR_UNBLOCK);
        return (it->second)->quantums;
    }
}
