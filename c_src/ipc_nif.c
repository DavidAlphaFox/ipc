//
//   PUBSUB system using shared memory
//
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <memory.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
#include <poll.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <pthread.h>

#include "erl_nif.h"

#define UNUSED(a) ((void) a)

typedef unsigned long unsigned_t;
typedef long          integer_t;


typedef struct _ipc_queue_t {
    unsigned_t size;    // total size of structure
    unsigned_t magic;   // queue magic
    unsigned_t nsize;   // length of name in words
    unsigned_t qsize;   // size of queue in words (=2^n)
    unsigned_t lfirst;  // offset to first cond_link
    unsigned_t type;
    unsigned_t qmask;   // = (1 << qsize)-1
    unsigned_t qhead;   // position to write & qmask
    unsigned_t data[];  // name + queue data
} ipc_queue_t;

#define QUEUE_MAGIC 0xE1E2E3E4

typedef struct _ipc_link_t {
    integer_t cnext;   // rel offset to next ipc_cond_t
    integer_t coffs;   // rel offset to current ipc_cond_t
    unsigned_t qid;    // abs offset to queue variable ipc_queue_t
    unsigned_t qtail;  // last position read in ipc_queue_t & qmask
} ipc_link_t;

// test if ipc_queue_t@qoffs is changed (negative is not)
// the jump offs otherwise next operation
// offs=0 == return FALSE, offs=1 == return TRUE
typedef struct _ipc_instr_t {
    integer_t qoffs;   // queue
    unsigned_t joffs;  // jump offset
} ipc_instr_t;

typedef struct _ipc_cond_t {
    unsigned_t size;       // total size of structure
    unsigned_t magic;      // cond magic
    unsigned_t nsize;      // name length
    unsigned_t lsize;      // number of link fields
    unsigned_t psize;      // number of words in prog
    pthread_cond_t cond;   // conditional variable
    pthread_mutex_t mutex; // condition mutex
    unsigned_t data[];     // name + prog + link data
} ipc_cond_t;

#define COND_MAGIC 0xC1C2C3C4

typedef struct _ipc_shm_t {
    size_t size;             // actual size of mapped data in words
    size_t avail;            // size of unused memory in words
    size_t foffs;            // word offset to available memory in data
    pthread_mutex_t mtx;     // allocation lock
    unsigned_t data[];    // actual data
} ipc_shm_t;

typedef struct _ipc_env_t {
    ipc_shm_t* mapped;
} ipc_env_t;

// fixme read cacheline value
#define MEM_ALIGN_SIZE  (64/sizeof(unsigned_t))  
#define FIRST MEM_ALIGN_SIZE

#define align(x, n)  (((((x)+(n)-1)/(n)))*(n))

#define number_of_words(n) (((n)+sizeof(unsigned_t)-1)/sizeof(unsigned_t))

#define ATOM(name) atm_##name

#define DECL_ATOM(name) \
    ERL_NIF_TERM atm_##name = 0

#define LOAD_ATOM(name)			\
    atm_##name = enif_make_atom(env,#name)

DECL_ATOM(ok);
DECL_ATOM(error);
DECL_ATOM(eot);
DECL_ATOM(unsigned8);
DECL_ATOM(unsigned16);
DECL_ATOM(unsigned32);
DECL_ATOM(unsigned64);
DECL_ATOM(integer8);
DECL_ATOM(integer16);
DECL_ATOM(integer32);
DECL_ATOM(integer64);
DECL_ATOM(float32);
DECL_ATOM(float64);
DECL_ATOM(object_type);
DECL_ATOM(condition);
DECL_ATOM(queue);
DECL_ATOM(size);
DECL_ATOM(type);
DECL_ATOM(name);

#define TYPE_UNSIGNED8   1
#define TYPE_UNSIGNED16  2
#define TYPE_UNSIGNED32  3
#define TYPE_UNSIGNED64  4
#define TYPE_INTEGER8    5
#define TYPE_INTEGER16   6
#define TYPE_INTEGER32   7
#define TYPE_INTEGER64   8
#define TYPE_FLOAT32     9
#define TYPE_FLOAT64     10

static int ipc_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);

static int ipc_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, 
			 ERL_NIF_TERM load_info);

static void ipc_unload(ErlNifEnv* env, void* priv_data);

static ERL_NIF_TERM nif_create(ErlNifEnv* env, int argc, 
			       const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_attach(ErlNifEnv* env, int argc, 
			       const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_create_queue(ErlNifEnv* env, int argc, 
				     const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_create_cond(ErlNifEnv* env, int argc,
				    const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_first(ErlNifEnv* env, int argc,
			      const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_next(ErlNifEnv* env, int argc,
			     const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_info(ErlNifEnv* env, int argc, 
			     const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_value(ErlNifEnv* env, int argc,
			      const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM nif_publish(ErlNifEnv* env, int argc,
				const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_subscribe(ErlNifEnv* env, int argc,
				  const ERL_NIF_TERM argv[]);


ErlNifFunc ipc_funcs[] =
{
    { "create",            2, nif_create },
    { "attach",            1, nif_attach },
    { "create_queue",      3, nif_create_queue },
    { "create_cond_",      3, nif_create_cond },
    { "first",             0, nif_first },
    { "next",              1, nif_next },
    { "info",              2, nif_info },
    { "publish_",          2, nif_publish },
    { "subscribe_",        1, nif_subscribe },
    { "value_",            2, nif_value },
};

static ipc_shm_t* mem_create(char* name, size_t size, mode_t mode)
{
    size_t page_size;
    size_t real_size;
    void* ptr;
    ipc_shm_t* mptr;
    pthread_mutexattr_t attrmutex;
    int fd;

    if ((page_size = sysconf(_SC_PAGE_SIZE)) == 0) {
	fprintf(stderr, "error: sysconf(_SC_PAGE_SIZE) return 0\n");
	return NULL;
    }

    real_size = align(size,page_size);  // size -> page aligned size
    if (!real_size)
	return NULL;

    if (shm_unlink(name) < 0) {
	if (errno != ENOENT)
	    perror("shm_unlink"); // normally ok if exited nice?
    }
    if ((fd=shm_open(name, O_CREAT | O_RDWR, mode)) < 0) {
	perror("shm_open");
	return NULL;
    }
    if (ftruncate(fd, real_size) < 0) {
	perror("ftruncate");
	close(fd);
	return NULL;
    }
    ptr = mmap(NULL, real_size, PROT_READ | PROT_WRITE, MAP_SHARED, 
	       fd, (off_t) 0);
    close(fd);
    if (ptr == MAP_FAILED) {
	perror("mmap");
	return NULL;
    }
    mptr = ptr;
    mptr->size = number_of_words(real_size);
    mptr->avail = number_of_words(real_size - sizeof(ipc_shm_t));
    mptr->foffs = 0;
    pthread_mutexattr_init(&attrmutex);
    pthread_mutexattr_setpshared(&attrmutex, PTHREAD_PROCESS_SHARED);
    if (pthread_mutex_init(&mptr->mtx, &attrmutex) < 0)
	perror("pthread_mutex_init");
    // pthread_mutexattr_destroy(&attrmutex);
    return mptr;
}

static ipc_shm_t* mem_open(char* name)
{
    size_t page_size;
    size_t buffer_size;
    void* ptr;
    ipc_shm_t* mptr;
    int fd;

    if ((page_size = sysconf(_SC_PAGE_SIZE)) == 0) {
	fprintf(stderr, "error: sysconf(_SC_PAGE_SIZE) return 0\n");
	return NULL;
    }
    if ((fd=shm_open(name, O_RDWR, 0)) < 0) {
	perror("shm_open");
	return NULL;
    }
    ptr = mmap(NULL, page_size, PROT_READ, MAP_SHARED, fd, (off_t) 0);
    if (ptr == MAP_FAILED) {
	perror("mmap");
	close(fd);
	return NULL;
    }
    mptr = ptr;
    // calculate size and remap
    buffer_size = mptr->size*sizeof(unsigned_t);

    if (munmap(ptr, page_size) < 0) {
	perror("munmap");
	close(fd);
	return NULL;
    }
    ptr = mmap(NULL, buffer_size, PROT_READ | PROT_WRITE, MAP_SHARED, 
	       fd, (off_t) 0);
    close(fd);
    if (ptr == MAP_FAILED) {
	perror("mmap");
	return NULL;
    }
    mptr = ptr;
    return mptr;
}

static int mem_close(ipc_shm_t* mptr)
{
    if (mptr != NULL) {
	size_t len = mptr->size*sizeof(unsigned_t);
	return munmap((void*) mptr, len);
    }
    return 0;
}

static int ilog2(unsigned_t x)
{
    int i = -1;
    while(x) {
	x >>= 1;
	i++;
    }
    return i;
}

// alloc nwords of data
static void* ipc_shm_alloc(ipc_env_t* eptr, unsigned_t nwords,
			   unsigned_t* offset)
{
    ipc_shm_t* mp = eptr->mapped;
    unsigned_t* ptr = NULL;
    unsigned_t size;

    size = align(nwords, MEM_ALIGN_SIZE);

    pthread_mutex_lock(&mp->mtx);
    if (size < mp->avail) {
	ptr = &mp->data[mp->foffs];
	*offset = mp->foffs;
	mp->foffs += size;
	mp->avail -= size;
	*ptr = size;
    }
    pthread_mutex_unlock(&mp->mtx);
    return ptr;
}

static void ipc_queue_init(ipc_queue_t* ptr,
			   char* buf, int n, unsigned_t type,
			   int qexp)
{
    ptr->magic = QUEUE_MAGIC;
    ptr->nsize = number_of_words(n);
    ptr->qsize = (1 << qexp);
    ptr->lfirst = 0;
    ptr->type = type;
    ptr->qmask = (1 << qexp)-1;
    ptr->qhead = 0;
    memset(ptr->data, 0, (ptr->nsize+ptr->qsize)*sizeof(unsigned_t));
    memcpy(ptr->data, buf, n);
}

static ipc_instr_t* ipc_cond_prog(ipc_cond_t* ptr)
{
    return (ipc_instr_t*) &ptr->data[ptr->nsize];
}

static ipc_link_t* ipc_cond_links(ipc_cond_t* ptr)
{
    return (ipc_link_t*) (ipc_cond_prog(ptr) + ptr->psize);
}

static int ipc_cond_eval(ipc_env_t* eptr, ipc_cond_t* ptr)
{
    ipc_shm_t* mptr = eptr->mapped;
    ipc_instr_t* prog;
    ipc_link_t* links = ipc_cond_links(ptr);
    int j = 0;

    prog = (ipc_instr_t*) &ptr->data[ptr->nsize];
    while(j < ptr->psize) {
	integer_t q = prog[j].qoffs;  // offset in links!
	unsigned_t qid;
	int negated = (q < 0);
	int updated = 0;
	ipc_link_t* lptr;
	ipc_queue_t* qptr;

	if (negated) q = -q;
	if (q >= ptr->lsize)
	    return -1;
	lptr = &links[q];
	qid  = lptr->qid;
	if (qid >= (mptr->size-number_of_words(sizeof(ipc_shm_t))))
	    return -1;
	switch(qid) {
	case 0: updated = 0; break;
	case 1: updated = negated ? 0 : 1; break;
	default:
	    if (mptr->data[lptr->qid+1] != QUEUE_MAGIC)
		return -1;
	    qptr = (ipc_queue_t*) &mptr->data[lptr->qid];
	    if (!negated && (qptr->qhead != lptr->qtail))
		updated = 1;
	    else if (negated && !(qptr->qhead != lptr->qtail))
		updated = 1;
	    break;
	}
	if (updated) {
	    switch(prog[j].joffs) {
	    case 0: return 0;
	    case 1: return 1;
	    default: j = j + prog[j].joffs; break;
	    }
	}
	else
	    j = j+1;
    }
    // program should return before this point
    return -1;
}

static void ipc_cond_init(ipc_cond_t* ptr,
			  char* buf, int n, 
			  ipc_instr_t* pp, int pn,
			  unsigned_t* qp, int qn)
{
    pthread_condattr_t attrcond;
    pthread_mutexattr_t attrmutex;
    int i, j, offs;

    ptr->magic = COND_MAGIC;
    ptr->nsize = number_of_words(n);
    ptr->lsize = qn;
    ptr->psize = pn;
    pthread_condattr_init(&attrcond);
    pthread_condattr_setpshared(&attrcond, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(&ptr->cond, &attrcond);

    pthread_mutexattr_init(&attrmutex);
    pthread_mutexattr_setpshared(&attrmutex, PTHREAD_PROCESS_SHARED);
    if (pthread_mutex_init(&ptr->mutex, &attrmutex) < 0)
	perror("pthread_mutex_init");
    // pthread_condattr_destroy(&attrcond);
    memset(ptr->data, 0, 
	   pn*sizeof(ipc_instr_t) + 
	   qn*sizeof(ipc_link_t) + ptr->nsize*sizeof(unsigned_t));
    memcpy(ptr->data, buf, n);  // copy name

    // we may memcpy here
    offs = ptr->nsize;
    j = 0;
    for (i = 0; i < pn; i++) {  // install prog
	ipc_instr_t* ip = (ipc_instr_t*) &ptr->data[offs+j];
	*ip = pp[i];
	j += number_of_words(sizeof(ipc_instr_t));
    }

    // but not here, index=0 and index=1 are not used, used for
    // nop and branch always / never
    offs = ptr->nsize + pn*number_of_words(sizeof(ipc_instr_t));
    j = 0;
    for (i = 0; i < qn; i++) { // install links
	ipc_link_t* lp = (ipc_link_t*) &ptr->data[offs+j];
	lp->cnext = 0;  // fixme: link to queue link
	lp->coffs = -(offs + number_of_words(sizeof(ipc_cond_t)));
	lp->qid   = qp[i];
	lp->qtail = 0;  // last updated qhead value
	j += number_of_words(sizeof(ipc_link_t));
    }
}

static int get_type(ErlNifEnv* env, ERL_NIF_TERM arg, unsigned_t* type)
{
    if (arg == ATOM(unsigned8))
	*type = TYPE_UNSIGNED8;
    else if (arg == ATOM(unsigned16))
	*type = TYPE_UNSIGNED16;
    else if (arg == ATOM(unsigned32))
	*type = TYPE_UNSIGNED32;
    else if (arg == ATOM(unsigned64))
	*type = TYPE_UNSIGNED64;
    else if (arg == ATOM(integer8))
	*type = TYPE_INTEGER8;
    else if (arg == ATOM(integer16))
	*type = TYPE_INTEGER16;
    else if (arg == ATOM(integer32))
	*type = TYPE_INTEGER32;
    else if (arg == ATOM(integer64))
	*type = TYPE_INTEGER64;
    else if (arg == ATOM(float32))
	*type = TYPE_FLOAT32;
    else if (arg == ATOM(float64))
	*type = TYPE_FLOAT64;
    else
	return 0;
    return 1;
}

static ERL_NIF_TERM make_type(ErlNifEnv* env, unsigned_t type)
{
    switch(type) {
    case TYPE_UNSIGNED8:  return ATOM(unsigned8);
    case TYPE_UNSIGNED16: return ATOM(unsigned16);
    case TYPE_UNSIGNED32: return ATOM(unsigned32);
    case TYPE_UNSIGNED64: return ATOM(unsigned64);
    case TYPE_INTEGER8:   return ATOM(integer8);
    case TYPE_INTEGER16:  return ATOM(integer16);
    case TYPE_INTEGER32:  return ATOM(integer32);
    case TYPE_INTEGER64:  return ATOM(integer64);
    case TYPE_FLOAT32:    return ATOM(float32);
    case TYPE_FLOAT64:    return ATOM(float64);
    default: return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM nif_create(ErlNifEnv* env, int argc, 
			       const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    char buf[1024];
    unsigned_t size;
    unsigned_t offset;
    ipc_shm_t* mptr;
    int mode = S_IRWXU | S_IRWXG;
    int n;

    if (!(n=enif_get_string(env,argv[0],buf,sizeof(buf),ERL_NIF_LATIN1)))
	return enif_make_badarg(env);
    if (!enif_get_ulong(env, argv[1], &size))
	return enif_make_badarg(env);

    if (eptr->mapped != NULL)
	return enif_make_badarg(env);

    if ((mptr = mem_create(buf, size+MEM_ALIGN_SIZE, mode)) == NULL)
	return enif_make_badarg(env);
    eptr->mapped = mptr;
    // now allocate one zero cache line, qid=0 can not be used as a queue
    ipc_shm_alloc(eptr, MEM_ALIGN_SIZE, &offset);
    
    return ATOM(ok);
}

static ERL_NIF_TERM nif_attach(ErlNifEnv* env, int argc, 
			       const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    char namebuf[1024];
    ipc_shm_t* mptr;
    int n;

    if (!(n=enif_get_string(env,argv[0],namebuf,sizeof(namebuf),
			    ERL_NIF_LATIN1)))
	return enif_make_badarg(env);
    if (eptr->mapped != NULL)
	return enif_make_badarg(env);
    if ((mptr = mem_open(namebuf)) == NULL)
	return enif_make_badarg(env);
    eptr->mapped = mptr;
    return ATOM(ok);
}



// create_queue(Name,Type,Size) -> {ok,ID} | exception badarg
static ERL_NIF_TERM nif_create_queue(ErlNifEnv* env, int argc,
				     const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    char buf[256];
    ipc_queue_t* qptr;
    unsigned_t type;
    unsigned_t size;
    unsigned_t qsize;
    unsigned_t offset;
    int qexp;
    int n;

    if (eptr->mapped == NULL)
	return enif_make_badarg(env);
    if (!(n=enif_get_atom(env,argv[0],buf,sizeof(buf),ERL_NIF_LATIN1)))
	return enif_make_badarg(env);
    if (!get_type(env, argv[1], &type))
	return enif_make_badarg(env);
    if (!enif_get_ulong(env, argv[2], &qsize))
	return enif_make_badarg(env);
    qexp = ilog2(qsize);
    if (qexp > 10)  // max size is 1024
	return enif_make_badarg(env);
    qsize = (1 << qexp);
    size = number_of_words(sizeof(ipc_queue_t)) + number_of_words(n) + qsize;
    
    if ((qptr = ipc_shm_alloc(eptr, size, &offset)) == NULL)
	return enif_make_badarg(env);  // out of memory?
    ipc_queue_init(qptr, buf, n, type, qexp);

    return enif_make_tuple2(env, ATOM(ok), enif_make_ulong(env, offset));
}

static ERL_NIF_TERM nif_create_cond(ErlNifEnv* env, int argc,
				    const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    char buf[256];
    ipc_cond_t* cptr;
    unsigned_t size;
    unsigned_t offset;
    const ERL_NIF_TERM* prog;
    const ERL_NIF_TERM* queues;
    ipc_instr_t pw[256];
    ipc_instr_t* pp = NULL;
    unsigned_t qw[256];
    unsigned_t* qp = NULL;
    int i, j, n, pn, qn;

    if (eptr->mapped == NULL)
	return enif_make_badarg(env);
    if (!(n=enif_get_atom(env,argv[0],buf,sizeof(buf),ERL_NIF_LATIN1)))
	return enif_make_badarg(env);
    if (!enif_get_tuple(env,argv[1],&pn,&prog))
	return enif_make_badarg(env);
    if (!enif_get_tuple(env,argv[2],&qn,&queues))  // list of queues
	return enif_make_badarg(env);
    if (pn < 256) // program fit in fixed area
	pp = pw;
    else
	pp = enif_alloc(sizeof(ipc_instr_t)*pn);
    j = 0;
    for (i = 0; i < pn; i++) {
	int arity;
	const ERL_NIF_TERM* instr;

	if (!enif_get_tuple(env,prog[i],&arity,&instr))
	    goto error;
	if (!enif_get_long(env,instr[0],&pp[i].qoffs))
	    goto error;
	// fixme: check that qoffs exist and is a queue
	if (!enif_get_ulong(env,instr[0],&pp[i].joffs))
	    goto error;
    }
    if (qn < 256)
	qp = qw;
    else
	qp = enif_alloc(sizeof(unsigned_t)*qn);
    j = 0;
    for (i = 0; i < qn; i++) {
	if (!enif_get_ulong(env,queues[i],&qp[j]))
	    goto error;
	// fixme: check that qp[j] exist and is a queue
	j++;
    }
    size = number_of_words(sizeof(ipc_cond_t)) + number_of_words(n) +
	number_of_words(sizeof(ipc_instr_t))*pn + 
	number_of_words(sizeof(ipc_link_t))*qn;
    if ((cptr = ipc_shm_alloc(eptr, size, &offset)) == NULL)
	return enif_make_badarg(env);  // out of memory?
    ipc_cond_init(cptr, buf, n, pp, pn, qp, qn);
    if (pp != pw) enif_free(pp);
    if (qp != qw) enif_free(qp);
    return enif_make_tuple2(env, ATOM(ok), enif_make_ulong(env, offset));
error:
    if (pp && (pp != pw)) enif_free(pp);
    if (qp && (qp != qw)) enif_free(qp);
    return enif_make_badarg(env);
}

static ERL_NIF_TERM nif_first(ErlNifEnv* env, int argc, 
			      const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);

    if (eptr->mapped == NULL)
	return enif_make_badarg(env);
    return enif_make_ulong(env, FIRST);
}

static ERL_NIF_TERM nif_next(ErlNifEnv* env, int argc, 
			     const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    ipc_shm_t* mptr;
    unsigned_t offset;
    unsigned_t val;

    if (eptr->mapped == NULL)
	return enif_make_badarg(env);
    if (!enif_get_ulong(env, argv[0], &offset))
	return enif_make_badarg(env);
    mptr = eptr->mapped;
    if (offset >= (mptr->size-number_of_words(sizeof(ipc_shm_t))))
	return enif_make_badarg(env);
    val = mptr->data[offset+1];    // assume magic field
    if ((val != COND_MAGIC) && (val != (QUEUE_MAGIC)))
	return enif_make_badarg(env);
    offset += mptr->data[offset];  // assume size field
    if (offset >= mptr->foffs)
	return ATOM(eot);
    val = mptr->data[offset+1];    // check magic field
    if ((val != COND_MAGIC) && (val != (QUEUE_MAGIC)))
	return enif_make_badarg(env);
    return enif_make_ulong(env, offset);
}

static ERL_NIF_TERM nif_info(ErlNifEnv* env, int argc, 
			     const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    ipc_shm_t* mptr;
    unsigned_t offset;
    unsigned_t val;
    if (eptr->mapped == NULL)
	return enif_make_badarg(env);
    if (!enif_get_ulong(env, argv[0], &offset))
	return enif_make_badarg(env);
    mptr = eptr->mapped;
    if (offset >= (mptr->size-number_of_words(sizeof(ipc_shm_t))))
	return enif_make_badarg(env);
    val = mptr->data[offset+1];    // assume magic field

    if (val == COND_MAGIC) {
	ipc_cond_t* cptr = (ipc_cond_t*) &mptr->data[offset];
	if (argv[1] == ATOM(object_type))
	    return ATOM(condition);
	else if (argv[1] == ATOM(name))
	    return enif_make_atom(env, (char*) &cptr->data[0]);
    }
    else if (val == QUEUE_MAGIC) {
	ipc_queue_t* qptr = (ipc_queue_t*) &mptr->data[offset];
	if (argv[1] == ATOM(object_type))
	    return ATOM(queue);
	else if (argv[1] == ATOM(name))
	    return enif_make_atom(env, (char*) &qptr->data[0]);
	else if (argv[1] == ATOM(size))
	    return enif_make_ulong(env, qptr->qsize);
	else if (argv[1] == ATOM(type))
	    return make_type(env, qptr->type);
    }
    return enif_make_badarg(env);	
}


static ERL_NIF_TERM nif_publish(ErlNifEnv* env, int argc, 
				const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    ipc_shm_t* mptr;
    ipc_queue_t* qptr;
    unsigned_t offset;
    long value;

    if ((mptr = eptr->mapped) == NULL)
	return enif_make_badarg(env);
    if (!enif_get_ulong(env, argv[0], &offset))
	return enif_make_badarg(env);
    if (!enif_get_long(env, argv[1], &value))
	return enif_make_badarg(env);
    if (offset >= (mptr->size-number_of_words(sizeof(ipc_shm_t))))
	return enif_make_badarg(env);
    if (mptr->data[offset+1] != QUEUE_MAGIC)    // assume magic field
	return enif_make_badarg(env);
    // fixme: add rw_lock to queue
    qptr = (ipc_queue_t*) &mptr->data[offset];
    qptr->data[qptr->nsize + qptr->qhead] = value;
    qptr->qhead = (qptr->qhead+1) & qptr->qmask;

    // scan link fields and eval conditions
    offset = qptr->lfirst;
    while(offset) {
	ipc_link_t* lptr = (ipc_link_t*) &mptr->data[offset];
	ipc_cond_t* cptr = (ipc_cond_t*) &mptr->data[offset+lptr->coffs];
	if (ipc_cond_eval(eptr, cptr) == 1) {
	    pthread_mutex_lock(&cptr->mutex);
	    pthread_cond_broadcast(&cptr->cond);
	    pthread_mutex_unlock(&cptr->mutex);
	}
	offset = lptr->cnext;
    }
    return ATOM(ok);
}

static ERL_NIF_TERM nif_value(ErlNifEnv* env, int argc, 
			      const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    ipc_shm_t* mptr;
    ipc_queue_t* qptr;
    unsigned_t offset;
    unsigned_t qpos;
    unsigned_t index;

    if ((mptr = eptr->mapped) == NULL)
	return enif_make_badarg(env);
    if (!enif_get_ulong(env, argv[0], &offset))
	return enif_make_badarg(env);
    if (!enif_get_ulong(env, argv[1], &index))
	return enif_make_badarg(env);

    if (offset >= (mptr->size-number_of_words(sizeof(ipc_shm_t))))
	return enif_make_badarg(env);
    if (mptr->data[offset+1] != QUEUE_MAGIC)    // assume magic field
	return enif_make_badarg(env);
    qptr = (ipc_queue_t*) &mptr->data[offset];
    qpos = (qptr->qhead - index - 1) & qptr->qmask;
    return enif_make_ulong(env, qptr->data[qptr->nsize + qpos]);
}

static ERL_NIF_TERM nif_subscribe(ErlNifEnv* env, int argc, 
				  const ERL_NIF_TERM argv[])
{
    ipc_env_t* eptr = enif_priv_data(env);
    ipc_shm_t* mptr;

    if ((mptr = eptr->mapped) == NULL)
	return enif_make_badarg(env);
    return enif_make_badarg(env);
}

static int  ipc_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ipc_env_t* eptr;

    if ((eptr = enif_alloc(sizeof(ipc_env_t))) == NULL)
	return -1;
    memset(eptr, 0, sizeof(ipc_env_t));
    *priv_data = eptr;
    LOAD_ATOM(ok);
    LOAD_ATOM(error);
    LOAD_ATOM(eot);
    LOAD_ATOM(unsigned8);
    LOAD_ATOM(unsigned16);
    LOAD_ATOM(unsigned32);
    LOAD_ATOM(unsigned64);
    LOAD_ATOM(integer8);
    LOAD_ATOM(integer16);
    LOAD_ATOM(integer32);
    LOAD_ATOM(integer64);
    LOAD_ATOM(float32);
    LOAD_ATOM(float64);
    LOAD_ATOM(object_type);
    LOAD_ATOM(condition);
    LOAD_ATOM(queue);
    LOAD_ATOM(size);
    LOAD_ATOM(type);
    LOAD_ATOM(name);
    return 0;
}

static int ipc_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data,
			ERL_NIF_TERM load_info)
{
    *priv_data = *old_priv_data;
    return 0;
}


static void ipc_unload(ErlNifEnv* env, void* priv_data)
{
    ipc_env_t* eptr = priv_data;
    
    mem_close(eptr->mapped);

    enif_free(eptr);
}

ERL_NIF_INIT(ipc, ipc_funcs, 
	     ipc_load, NULL,
	     ipc_upgrade, ipc_unload)
