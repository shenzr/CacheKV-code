/* ********************************
 * Extensions:   Sudarsun Kannan
 * Author:       Johan Hanssen Seferidis
 * License:	     MIT
 * Description:  Library providing a threading pool where you can add
 *               work. For usage, check the thpool.h file or README.md
 *
 *//** @file thpool.h *//*
 * 
 ********************************/
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <time.h> 


#include "affinity.h"
#include "thpool.h"
#include "lsm_mutex.h"
#include "cpumap.h"

#ifdef THPOOL_DEBUG
#define THPOOL_DEBUG 1
#else
#define THPOOL_DEBUG 0
#endif

#define MAX_NANOSEC 999999999
#define CEIL(X) ((X-(int)(X)) > 0 ? (int)(X+1) : (int)(X))

#include <atomic>

static volatile int threads_keepalive;
static volatile int threads_on_hold;
volatile static int firstime=0;
volatile static int numthreads=0;

/* ========================== STRUCTURES ============================ */


/* Binary semaphore */
typedef struct bsem {
    pthread_mutex_t mutex;
    pthread_cond_t   cond;
    int v;
} bsem;


/* Job */
typedef struct job{
    struct job*  prev;                   /* pointer to previous job   */
    void*  (*function)(void* arg);       /* function pointer          */
    void*  arg;                          /* function's argument       */
    std::atomic_bool isEmpty;
} job;

job* g_newjob = NULL;


/* Job queue */
typedef struct jobqueue{
    pthread_mutex_t rwmutex;             /* used for queue r/w access */
    job  *front;                         /* pointer to front of queue */
    job  *rear;                          /* pointer to rear  of queue */
    bsem *has_jobs;                      /* flag as binary semaphore  */
    std::atomic_int len;                  /* number of jobs in queue   */
} jobqueue;


/* Thread */
typedef struct thread{
    int       id;                        /* friendly id               */
    pthread_t pthread;                   /* pointer to actual thread  */
    struct thpool_* thpool_p;            /* access to thpool          */
} thread;


/* /\* Threadpool *\/ */
/* typedef struct thpool_{ */
/* 	thread**   threads;                  /\* pointer to threads        *\/ */
/* 	volatile int num_threads_alive;      /\* threads currently alive   *\/ */
/* 	volatile int num_threads_working;    /\* threads currently working *\/ */
/* 	pthread_mutex_t  thcount_lock;       /\* used for thread count etc *\/ */
/* 	jobqueue*  jobqueue_p;               /\* pointer to the job queue  *\/     */
/* } thpool_; */

/* Threadpool */
typedef struct thpool_{
    thread**   threads;                  /* pointer to threads        */
    volatile int num_threads_alive;      /* threads currently alive   */
    std::atomic_int num_threads_working;    /* threads currently working */
    pthread_mutex_t  thcount_lock;       /* used for thread count etc */
    jobqueue*  jobqueue_p;               /* pointer to the job queue  */
    int firstime;
} thpool_;

    /* ========================== PROTOTYPES ============================ */


static void  thread_init(thpool_* thpool_p, struct thread** thread_p, int id);
//static void* thread_do(struct thread* thread_p);
static void* thread_do(void *);
static void  thread_hold();
static void  thread_destroy(struct thread* thread_p);

static int   jobqueue_init(thpool_* thpool_p);
static void  jobqueue_clear(thpool_* thpool_p);
static void  jobqueue_push(thpool_* thpool_p, struct job* newjob_p);
static struct job* jobqueue_pull(thpool_* thpool_p);
static void  jobqueue_destroy(thpool_* thpool_p);

static void  bsem_init(struct bsem *bsem_p, int value);
static void  bsem_reset(struct bsem *bsem_p);
static void  bsem_post(struct bsem *bsem_p);
static void  bsem_post_all(struct bsem *bsem_p);
static void  bsem_wait(struct bsem *bsem_p);


/*int pthread_mutex_lock(pthread_mutex_t *mutex){
  return pthread_mutex_lock(mutex);
}
int pthread_mutex_unlock(pthread_mutex_t *mutex){
  return pthread_mutex_unlock(mutex);
}*/
/* ========================== THREADPOOL ============================ */


/* Initialise thread pool */
struct thpool_* thpool_init(int num_threads){

    threads_on_hold   = 0;
    threads_keepalive = 1;
    numthreads = 0;

    if ( num_threads < 0){
        num_threads = 0;
    }

    /* Make new thread pool */
    thpool_* thpool_p;
    thpool_p = (struct thpool_*)malloc(sizeof(struct thpool_));
    if (thpool_p == NULL){
        fprintf(stderr, "thpool_init(): Could not allocate memory for thread pool\n");
        return NULL;
    }
    thpool_p->num_threads_alive   = 0;
    thpool_p->num_threads_working.store(0);

    /* Initialise the job queue */
    if (jobqueue_init(thpool_p) == -1){
        fprintf(stderr, "thpool_init(): Could not allocate memory for job queue\n");
        free(thpool_p);
        return NULL;
    }

    /* Make threads in pool */
    thpool_p->threads = (struct thread**)malloc(num_threads * sizeof(struct thread));
    if (thpool_p->threads == NULL){
        fprintf(stderr, "thpool_init(): Could not allocate memory for threads\n");
        jobqueue_destroy(thpool_p);
        free(thpool_p->jobqueue_p);
        free(thpool_p);
        return NULL;
    }

    pthread_mutex_init(&(thpool_p->thcount_lock), NULL);

    /* Fill CPU map information */
    fill_cpumap_info();

    /* Thread init */
    int n;
    for (n=0; n<num_threads; n++){
        //thread_init(thpool_p, &thpool_p->threads[n], 22);
        thread_init(thpool_p, &thpool_p->threads[n], n);
        if (THPOOL_DEBUG)
            printf("THPOOL_DEBUG: Created thread %d in pool \n", n);
        numthreads++;
    }

    /* Wait for threads to initialize */
    while (thpool_p->num_threads_alive != num_threads) {}
    g_newjob=(struct job*)malloc(sizeof(struct job) * num_threads);
    if (g_newjob==NULL){
        fprintf(stderr, "thpool_add_work(): Could not allocate memory for new job\n");
        return NULL;
    }
    for(int i=0; i<num_threads; i++)
        g_newjob[i].isEmpty.store(1);

    return thpool_p;
}

/* Add work to the thread pool */
int thpool_add_work(thpool_* thpool_p, void *(*function_p)(void*), void* arg_p, int i){

    /* add function and argument */
    g_newjob[i].function=function_p;
    g_newjob[i].arg=arg_p;
    g_newjob[i].isEmpty.store(0);
    thpool_p->jobqueue_p->len++;
    //pthread_mutex_lock(&thpool_p->jobqueue_p->rwmutex);
    /* add job to queue */
    //jobqueue_push(thpool_p, g_newjob);
    //pthread_mutex_unlock(&thpool_p->jobqueue_p->rwmutex);
    return 0;
}

#if 0
/* Add work to the thread pool */
int thpool_add_work(thpool_* thpool_p, void *(*function_p)(void*), void* arg_p){
    job* newjob;

    newjob=(struct job*)malloc(sizeof(struct job));
    if (newjob==NULL){
        fprintf(stderr, "thpool_add_work(): Could not allocate memory for new job\n");
        return -1;
    }
    /* add function and argument */
    newjob->function=function_p;
    newjob->arg=arg_p;

    //pthread_mutex_lock(&thpool_p->jobqueue_p->rwmutex);
    /* add job to queue */
    jobqueue_push(thpool_p, newjob);
    //pthread_mutex_unlock(&thpool_p->jobqueue_p->rwmutex);

    return 0;
}
#endif



int get_jobqueue_len(thpool_* thpool_p) {

    return thpool_p->jobqueue_p->len.load();
}


/* Wait until all jobs have finished */
void thpool_wait(thpool_* thpool_p){

    /* Continuous polling */
    //double timeout = 1.0;
    //time_t start, end;
    //double tpassed = 0.0;
    //time (&start);

    while (thpool_p->jobqueue_p->len.load() || thpool_p->num_threads_working.load())
    {
        //time (&end);
        //tpassed = difftime(end,start);
        //ATOMIC_LOAD(&thpool_p->jobqueue_p->len);
        //ATOMIC_LOAD(&thpool_p->num_threads_working);
    }

    /* Exponential polling */
    //long init_nano =  1; /* MUST be above 0 */
    //long new_nano;
    //double multiplier =  1.01;
    //int  max_secs   = 20;
    //struct timespec polling_interval;
    //polling_interval.tv_sec  = 0;
    //polling_interval.tv_nsec = init_nano;

    /* while (thpool_p->jobqueue_p->len || thpool_p->num_threads_working) */
    /* { */
    /* 	nanosleep(&polling_interval, NULL); */
    /* 	if ( polling_interval.tv_sec < max_secs ){ */
    /* 		new_nano = CEIL(polling_interval.tv_nsec * multiplier); */
    /* 		polling_interval.tv_nsec = new_nano % MAX_NANOSEC; */
    /* 		if ( new_nano > MAX_NANOSEC ) { */
    /* 			polling_interval.tv_sec ++; */
    /* 		} */
    /* 	} */
    /* 	else break; */
    /* } */

    /* /\* Fall back to max polling *\/ */
    /* while (thpool_p->jobqueue_p->len || thpool_p->num_threads_working){ */
    /* 	sleep(max_secs); */
    /* } */
}


/* Destroy the threadpool */
void thpool_destroy(thpool_* thpool_p){

    volatile int threads_total = thpool_p->num_threads_alive;

    /* End each thread 's infinite loop */
    threads_keepalive = 0;
#if 0	
    /* Give one second to kill idle threads */
    double TIMEOUT = 1.0;
    time_t start, end;
    double tpassed = 0.0;
    time (&start);
    while (tpassed < TIMEOUT && thpool_p->num_threads_alive){
        bsem_post_all(thpool_p->jobqueue_p->has_jobs);
        time (&end);
        tpassed = difftime(end,start);
    }

    /* Poll remaining threads */
    while (thpool_p->num_threads_alive){
        bsem_post_all(thpool_p->jobqueue_p->has_jobs);
        sleep(1);
    }
#endif
    /* Job queue cleanup */
    jobqueue_destroy(thpool_p);
    free(thpool_p->jobqueue_p);

    /* Deallocs */
    int n;
    for (n=0; n < threads_total; n++){
        thread_destroy(thpool_p->threads[n]);
    }
    free(thpool_p->threads);
    free(thpool_p);
}


/* Pause all threads in threadpool */
void thpool_pause(thpool_* thpool_p) {
    int n;
    for (n=0; n < thpool_p->num_threads_alive; n++){
        pthread_kill(thpool_p->threads[n]->pthread, SIGUSR1);
    }
}


/* Resume all threads in threadpool */
void thpool_resume(thpool_* thpool_p) {
    threads_on_hold = 0;
}

/* ============================ THREAD ============================== */

int th_setaffinity(pthread_t *thread, int cpuid)
{
    int s;
    cpu_set_t cpuset;
    int coreid = -1;

    CPU_ZERO(&cpuset);
    //TODO: Simply returns a core from decrementing order
    //Needs fix to identify cores that are used by other threads
    //of this process and return a free core.
    //coreid = get_free_core();
    //fprintf(stderr, "Setting affinity for read thread to %d\n", coreid);
    for(int i=0; i<31; i+=2)
        CPU_SET(i, &cpuset);
    s = pthread_setaffinity_np(*thread, sizeof(cpu_set_t), &cpuset);
    if (s != 0) {
        fprintf(stderr, "FAILED \n");
    }
    return 0;
}


/* Initialize a thread in the thread pool
 * 
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * 
 */
static void thread_init (thpool_* thpool_p, struct thread** thread_p, int id){

    *thread_p = (struct thread*)malloc(sizeof(struct thread));
    if (thread_p == NULL){
        fprintf(stderr, "thpool_init(): Could not allocate memory for thread\n");
        exit(1);
    }

    (*thread_p)->thpool_p = thpool_p;
    (*thread_p)->id       = id;
    thpool_p->firstime = 0;

    pthread_create(&(*thread_p)->pthread, NULL, thread_do, (*thread_p));
    //pthread_detach((*thread_p)->pthread);
    th_setaffinity(&(*thread_p)->pthread, 0);
}


/* Sets the calling thread on hold */
static void thread_hold () {
    threads_on_hold = 1;
    while (threads_on_hold){
        sleep(1);
    }
}


/* What each thread is doing
 *
 * In principle this is an endless loop. The only time this
 * loop gets interuppted is once thpool_destroy() is invoked
 * or the program exits.
 *
 * @param  thread  thread that will run this function
 * @return nothing
 */
static void* thread_do(void *arg){

    /* Assure all threads have been created before starting serving */
    struct thread* thread_p = (struct thread*)arg;
    thpool_* thpool_p = thread_p->thpool_p;
    int thread_id = thread_p->id;

    /* Mark thread as alive (initialized) */
    pthread_mutex_lock(&thpool_p->thcount_lock);
    thpool_p->num_threads_alive += 1;
    pthread_mutex_unlock(&thpool_p->thcount_lock);

    while(threads_keepalive){

        if(!thpool_p->firstime){
          while(thpool_p->jobqueue_p == NULL ||
                  thpool_p->jobqueue_p->len.load() == 0);
		  thpool_p->firstime = 1;

        }else {
            while(1){
         		if(thpool_p->jobqueue_p->len.load() && !g_newjob[thread_id % numthreads].isEmpty.load())
					break;
	    	}

			thpool_p->num_threads_working++;
			thpool_p->jobqueue_p->len--;


            g_newjob[thread_id % numthreads].function(g_newjob[thread_id % numthreads].arg);

            g_newjob[thread_id % numthreads].isEmpty.store(1);

			thpool_p->num_threads_working--;
        }
    }
    pthread_mutex_lock(&thpool_p->thcount_lock);
    thpool_p->num_threads_alive --;
    pthread_mutex_unlock(&thpool_p->thcount_lock);
    return NULL;
}

/*Generic function. Currently disabled for performance
 * tuning. We will switch to this later
 */
#if 0
static void* thread_do1(struct thread* thread_p){

    /* Assure all threads have been created before starting serving */
    thpool_* thpool_p = thread_p->thpool_p;
    //pthread_t thread;
    //thread = pthread_self();

    //th_setaffinity(&thread, (thread_p)->id);
    /* Register signal handler */
    /*struct sigaction act;
	act.sa_handler = thread_hold;
	if (sigaction(SIGUSR1, &act, NULL) == -1) {
		fprintf(stderr, "thread_do(): cannot handle SIGUSR1");
	}*/

    /* Mark thread as alive (initialized) */
    pthread_mutex_lock(&thpool_p->thcount_lock);
    thpool_p->num_threads_alive += 1;
    pthread_mutex_unlock(&thpool_p->thcount_lock);

    while(threads_keepalive){
        if(!firstime){
            bsem_wait(thpool_p->jobqueue_p->has_jobs);
            firstime = 1;
        }else {
            while(thpool_p->jobqueue_p->len == 0);
        }

        if (threads_keepalive){

            pthread_mutex_lock(&thpool_p->thcount_lock);
            thpool_p->num_threads_working++;
            pthread_mutex_unlock(&thpool_p->thcount_lock);

            /* Read job from queue and execute it */
            void*(*func_buff)(void* arg);
            void*  arg_buff;
            job* job_p;
            pthread_mutex_lock(&thpool_p->jobqueue_p->rwmutex);
            job_p = jobqueue_pull(thpool_p);
            pthread_mutex_unlock(&thpool_p->jobqueue_p->rwmutex);
            if (job_p) {
                func_buff = job_p->function;
                arg_buff  = job_p->arg;
                func_buff(arg_buff);
                free(job_p);
            }

            pthread_mutex_lock(&thpool_p->thcount_lock);
            thpool_p->num_threads_working--;
            pthread_mutex_unlock(&thpool_p->thcount_lock);

        }
    }
    pthread_mutex_lock(&thpool_p->thcount_lock);
    thpool_p->num_threads_alive --;
    pthread_mutex_unlock(&thpool_p->thcount_lock);

    return NULL;
}
#endif


/* Frees a thread  */
static void thread_destroy (thread* thread_p){
    free(thread_p);
}


/* ============================ JOB QUEUE =========================== */


/* Initialize queue */
static int jobqueue_init(thpool_* thpool_p){

    thpool_p->jobqueue_p = (struct jobqueue*)malloc(sizeof(struct jobqueue));
    if (thpool_p->jobqueue_p == NULL){
        return -1;
    }
    thpool_p->jobqueue_p->len.store(0);
    thpool_p->jobqueue_p->front = NULL;
    thpool_p->jobqueue_p->rear  = NULL;

    thpool_p->jobqueue_p->has_jobs = (struct bsem*)malloc(sizeof(struct bsem));
    if (thpool_p->jobqueue_p->has_jobs == NULL){
        return -1;
    }

    pthread_mutex_init(&(thpool_p->jobqueue_p->rwmutex), NULL);
    bsem_init(thpool_p->jobqueue_p->has_jobs, 0);

    return 0;
}


/* Clear the queue */
static void jobqueue_clear(thpool_* thpool_p){

    while(thpool_p->jobqueue_p->len.load()){
        free(jobqueue_pull(thpool_p));
    }

    thpool_p->jobqueue_p->front = NULL;
    thpool_p->jobqueue_p->rear  = NULL;
    bsem_reset(thpool_p->jobqueue_p->has_jobs);
    thpool_p->jobqueue_p->len.store(0);

}


/* Add (allocated) job to queue
 *
 * Notice: Caller MUST hold a mutex
 */
static void jobqueue_push(thpool_* thpool_p, struct job* newjob){

    newjob->prev = NULL;

    switch(thpool_p->jobqueue_p->len.load()){

    case 0:  /* if no jobs in queue */
        thpool_p->jobqueue_p->front = newjob;
        thpool_p->jobqueue_p->rear  = newjob;
        break;

    default: /* if jobs in queue */
        thpool_p->jobqueue_p->rear->prev = newjob;
        thpool_p->jobqueue_p->rear = newjob;

    }
    //FTL_ATOMIC_ADD_FETCH(&thpool_p->jobqueue_p->len, 1);
	thpool_p->jobqueue_p->len++;
    //if(!thpool_p->firstime)
    //bsem_post(thpool_p->jobqueue_p->has_jobs);
}


/* Get first job from queue(removes it from queue)
 * 
 * Notice: Caller MUST hold a mutex
 */
static struct job* jobqueue_pull(thpool_* thpool_p){

    job* job_p;
    job_p = thpool_p->jobqueue_p->front;

    switch(thpool_p->jobqueue_p->len.load()){

    case 0:  /* if no jobs in queue */
        break;

    case 1:  /* if one job in queue */
        thpool_p->jobqueue_p->front = NULL;
        thpool_p->jobqueue_p->rear  = NULL;
        thpool_p->jobqueue_p->len.store(0);
        break;

    default: /* if >1 jobs in queue */
        thpool_p->jobqueue_p->front = job_p->prev;
        thpool_p->jobqueue_p->len--;
        /* more than one job in queue -> post it */
        //bsem_post(thpool_p->jobqueue_p->has_jobs);

    }

    return job_p;
}


/* Free all queue resources back to the system */
static void jobqueue_destroy(thpool_* thpool_p){
    jobqueue_clear(thpool_p);
    free(thpool_p->jobqueue_p->has_jobs);
}

/* ======================== SYNCHRONISATION ========================= */


/* Init semaphore to 1 or 0 */
static void bsem_init(bsem *bsem_p, int value) {
    if (value < 0 || value > 1) {
        fprintf(stderr, "bsem_init(): Binary semaphore can take only values 1 or 0");
        exit(1);
    }
    pthread_mutex_init(&(bsem_p->mutex), NULL);
    pthread_cond_init(&(bsem_p->cond), NULL);
    bsem_p->v = value;
}


/* Reset semaphore to 0 */
static void bsem_reset(bsem *bsem_p) {
    bsem_init(bsem_p, 0);
}


/* Post to at least one thread */
static void bsem_post(bsem *bsem_p) {
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    pthread_cond_signal(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}


/* Post to all threads */
static void bsem_post_all(bsem *bsem_p) {
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    pthread_cond_broadcast(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}


/* Wait on semaphore until semaphore has value 0 */
static void bsem_wait(bsem* bsem_p) {
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1) {
        pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
    }
    bsem_p->v = 0;
    pthread_mutex_unlock(&bsem_p->mutex);
}

