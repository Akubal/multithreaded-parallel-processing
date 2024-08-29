#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <limits.h>
#include <regex.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <pthread.h>

void *encode();
void add_to_queue(char *addr, int size);

struct DataBlock
{
    int size;
    char *data;
    int pos;
};

typedef struct DataBlock DataBlock;

struct ListNode
{
    DataBlock *dataBlock;
    struct ListNode *next;
};

typedef struct ListNode ListNode;

ListNode *inputQueueHead;
ListNode *inputQueueTail;
ListNode *outputQueueHead;
ListNode *outputQueueTail;

int inputQueueSize = 0;
int outputQueueSize = 0;

pthread_mutex_t input_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t output_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty_input_queue = PTHREAD_COND_INITIALIZER;
pthread_cond_t output_queue_not_empty = PTHREAD_COND_INITIALIZER;

int main(int argc, char *const *argv) {
    
    int j=1;
    char opt;
    extern char *optarg;
    extern int optind;
    opt = getopt(argc, argv, "j:");
        switch (opt)
        {
        case 'j':
            j=atoi(optarg);
            break;
        
        default:
            break;
        }
    

    pthread_t threads[j];
    int totalSize = 0;
    for(int ar = optind;ar<argc;ar++) {
        int fd = open(argv[ar], O_RDONLY);
         if(fd == -1) {
            fprintf(stderr, "Error");
        }

        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            fprintf(stderr, "Error");
        }
        totalSize = totalSize + sb.st_size;
        char *addr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (addr == MAP_FAILED) {
            fprintf(stderr, "Error");
        }
        add_to_queue(addr, sb.st_size);
    }
    
    int output_queue_length = inputQueueSize;
    DataBlock *output_queue[output_queue_length];
    int th = 0;
    while(th<j) {
    
        pthread_create(&threads[th], NULL, encode, NULL);
        th++;
        
    }

    
    
    int iterations = output_queue_length;
    for(int j=0;j<iterations;j++) {
        
        pthread_mutex_lock(&output_queue_mutex);
        while(outputQueueSize == 0) {
            
            pthread_cond_wait(&output_queue_not_empty, &output_queue_mutex);
        }
        
        ListNode *output_block = outputQueueHead;

        DataBlock *output_block_data = output_block->dataBlock;
        
        output_queue[output_block_data->pos] = output_block_data;
        
        
        outputQueueHead = outputQueueHead->next;
        outputQueueSize--;
        pthread_mutex_unlock(&output_queue_mutex);
        free(output_block);
    }

    char *output = malloc(sizeof(char)*totalSize*5);
    char *actual_output;
    actual_output = output;
    int ofs = 0;
    for(int k=0;k<iterations;k++) {
        
        char *op1 = output_queue[k]->data;
        int op1_size = output_queue[k]->size;
        
        if(k==0) {
            memcpy(output, op1, op1_size);
            ofs = op1_size-2;
        }
        else {
            if(*(output+ofs) == *op1) {
                *(output+ofs+1) = *(output+ofs+1) + *(op1+1);
                if(op1_size > 2) {
                    
                    op1 = op1+2;
                    op1_size = op1_size-2;
                    memcpy(output+ofs+2, op1, op1_size);
                    ofs = ofs + op1_size;
                }
            }
            else {
                
                memcpy(output+ofs+2, op1, op1_size);
                ofs = ofs + op1_size;
            }
        } 
        
        
        
      
        free(output_queue[k]->data);
        free(output_queue[k]);
        
    }
    
    fwrite(actual_output, 1, ofs+2, stdout);
    free(actual_output);
    
    return 0;



    
}

void add_to_queue(char *addr, int size) {
    if(size < 4096) {
        ListNode *temp = malloc(sizeof(ListNode));
        DataBlock *data = malloc(sizeof(DataBlock));
        char *db = malloc(sizeof(char)*4096);
        memcpy(db, addr,size);
        data->data = db;

        if(inputQueueHead != NULL) {
                data->pos = inputQueueTail->dataBlock->pos + 1;
        }
        else {
                data->pos = 0;
        }
        data->size = size;
        temp->dataBlock = data;
        if(inputQueueHead == NULL) {
            inputQueueHead = temp;
            inputQueueTail = temp;
        }
        else {
            inputQueueTail->next = temp;
            inputQueueTail = temp;
        }
        inputQueueSize++;
    }
    else {
        int chunks = size/4096;
        int last = size%4096;
        int c = 1;
        
        for(int i=0;i<chunks;i++) {
            ListNode *temp = malloc(sizeof(ListNode));
            
            DataBlock *data = malloc(sizeof(DataBlock));
           
            char *db = malloc(sizeof(char)*4096);
           
            memcpy(db, addr, 4096);
            
            
            data->data = db;
            if(inputQueueHead != NULL) {
                data->pos = inputQueueTail->dataBlock->pos + 1;
            }
            else {
                data->pos = 0;
            }
            
            data->size = 4096;
        
            temp->dataBlock = data;
            
            if(inputQueueHead != NULL) {
                inputQueueTail->next = temp;
                inputQueueTail = temp;
            }
            else {
                inputQueueHead = temp;
                inputQueueTail = temp;
            }
            
            addr = addr + 4096;
            c++;
            inputQueueSize++;
            
        }
       
        if(last != 0 ) {
            ListNode *temp = malloc(sizeof(ListNode));
            
            DataBlock *data = malloc(sizeof(DataBlock));
            
            char *db = malloc(sizeof(char)*4096);
            memcpy(db, addr, last);
            
            data->data = db;
            data->pos = inputQueueTail->dataBlock->pos + 1;
            data->size = last;
            temp->dataBlock = data;
            
            inputQueueTail->next = temp;
            inputQueueTail = temp;
            //addr = addr + 4096;
            inputQueueSize++;
        }
        
    }

}

void *encode() {
    
    for(;;) {
        pthread_mutex_lock(&input_queue_mutex);
        while (inputQueueSize == 0)
        {
            
            pthread_cond_wait(&empty_input_queue, &input_queue_mutex);
        }
        if(inputQueueHead == NULL) {
            pthread_exit(NULL);
        }
        
        ListNode *input_block = inputQueueHead;
        
        inputQueueHead = inputQueueHead->next;
        inputQueueSize--;
        pthread_mutex_unlock(&input_queue_mutex);
        
        
        DataBlock *s = input_block->dataBlock;
        int count;
        char last = s->data[0];
        int size = s->size;
       
        char *encoded_str = malloc(sizeof(char)*4097*2);
        int j=0;
        
        for (int i = 0; i<size; i++) {
            count = 1;
            last = s->data[i];
            while (i<size-1 && last == s->data[i + 1]) {
                count++;
                i++;
            }
            *(encoded_str+j) = last;
            *(encoded_str+j+1) = (unsigned int)count;
            j=j+2;


            
        
        }
        
        ListNode *temp = malloc(sizeof(ListNode));
        DataBlock *data_block = malloc(sizeof(DataBlock));
        char *db = malloc(sizeof(char)*4097*2);
        memcpy(db, encoded_str,j);
        data_block->data = db;
        
        data_block->pos = s->pos;
        data_block->size = j;
        temp->dataBlock = data_block;
        
        pthread_mutex_lock(&output_queue_mutex);
        if(data_block->pos > 241099) {
            //printf("Found\n");
        }
        if(outputQueueHead == NULL) {
                outputQueueHead = temp;
                outputQueueTail = temp;
        }
        else {
            outputQueueTail->next = temp;
            outputQueueTail = outputQueueTail->next;
        }
        outputQueueSize++;
        pthread_cond_signal(&output_queue_not_empty);
        pthread_mutex_unlock(&output_queue_mutex);
        free(input_block->dataBlock->data);
        free(input_block->dataBlock);
        free(input_block);

    }
    
    return NULL;
    
}