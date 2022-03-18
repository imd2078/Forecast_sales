#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>


#define MATRIXSIZE 400
#define DB 0
#define arow 400
#define acol 400
#define brow 400
#define bcol 400

int main(int argc, char **argv) {
//Initializing matrices and arrays
        int processes, my_rank;
        int array[arow*acol];
        int arrayb[brow*bcol];

        int n = arow;
        if (argc > 1) n = strtol(argv[1], NULL, 10);

        int A[arow][acol];//={{1,0,0,2},{4,3,0,0},{1,1,0,0},{0,0,0,1}}; To test the code
        int B[brow][bcol];//={{1,2},{3,4},{5,6},{7,8}}; 
        int splitA[arow*acol];
        int splitB[arow*acol];
        int splitC[arow*bcol];
        int C[arow][bcol];

        double start_time, finish_time, final_time;

        srand(time(NULL));
        int num;
//Generating a sparse matrix and a dense matrix
         for (int i = 0; i < arow*acol; i++)
        {
                 if (rand() % 2 == 0)
         {
                 array[i] = rand() % 100;        }
         else
         {
         array[i] = 0;
         }
        }
         for (int i = 0; i < brow*bcol; i++)
        {
                 arrayb[i] = rand() % 100;

        }

        for (int i = 0; i < bcol; i++) {
            for (int j = 0; j < arow; j++) {
                 A[j][i] = array[(i*arow)+j];
                 B[j][i] = arrayb[(i*arow)+j];
            }
        }

//Initializing MPI
        MPI_Init(&argc, &argv);
        MPI_Comm_size(MPI_COMM_WORLD, &processes);
        MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
// Using non blocking communication
        MPI_Request request;
        MPI_Status status;
// Starting the timer
        start_time = MPI_Wtime();
        if (arow%processes != 0) {
                if (my_rank == 0) printf("%d not divisible by %d\n\n", arow, processes);
                MPI_Finalize();
                return 0;
        }

//Master process
        if (my_rank == 0) {
//Splitting A and B to distribute among processes
                for (int row = 0; row < arow; row++) {
                        for (int col = 0; col < acol; col++) {
                                splitA[(row*acol)+col] = A[row][col];
                        }
                }

                for (int col = 0; col < acol; col++) {
                        for (int row = 0; row < arow; row++) {
                                if((col*brow)+row > (brow*bcol)){
                                     splitB[(col*brow)+row] = 0;
                                }
                                else {
                                     splitB[(col*brow)+row] = B[row][col];
                                }
                        }
                }
        }

        if (DB && my_rank==0) printf("scattering\n");
//Scattering the elements of A, B 

        int localA[arow*acol/processes];
        int localB[arow*acol/processes];
        MPI_Scatter(&splitA, arow*acol/processes, MPI_INT, &localA, arow*acol/processes,MPI_INT, 0, MPI_COMM_WORLD);

        MPI_Scatter(&splitB, arow*acol/processes, MPI_INT, &localB, arow*acol/processes,MPI_INT, 0, MPI_COMM_WORLD);

        int localC[arow*acol/processes];
        int j = my_rank;
        int k = 0;
        int sum = 0;

        for (int iter = 0; iter < processes; iter++) {
//Matrix Multiplication 
                for (int i = 0; i < n/processes; i++) {
                        for (int k = 0; k < n/processes; k++) {
                        localC[k*n+i+j*n/processes] = 0;
                                for (int f = 0; f < n; f++) {
                                        sum += localA[f+i*n]*localB[f+k*n];
                                }
                                localC[k*n+i+j*n/processes] = sum;
                                sum = 0;
                        }
                }

                if (j == processes-1) j = 0;
                else j++;

                if (DB && my_rank==0) printf("Finished iter %d\n", iter+1);
//Using non blocking send and receive for better performance
                MPI_Isend(&localA, n*n/processes, MPI_INT, (my_rank==0 ?
                    processes-1 : my_rank-1), 0, MPI_COMM_WORLD, &request);
                MPI_Irecv(&localA, n*n/processes, MPI_INT, (my_rank==processes-1 ?
                    0 : my_rank+1), 0, MPI_COMM_WORLD, &request);
                MPI_Wait(&request, &status);
                if (DB && my_rank==0) printf("Finished ring pass\n");
        }

        if (DB && my_rank==0) printf("Completed scatter and calculations\n");
//Gathering the data from different processes into a local array
        MPI_Gather(&localC, n*n/processes, MPI_INT, &splitC, n*n/processes,
                 MPI_INT, 0, MPI_COMM_WORLD);

        for (int i = 0; i < bcol; i++) {
                for (int j = 0; j < arow; j++) {
                        C[j][i] = splitC[(i*n)+j];
                }
        }

        finish_time = MPI_Wtime();

        if (DB && my_rank==0) printf("Finished matrix.\n");
//printing out the matrix , processes, time taken
        if (my_rank == 0) {

               
                        for (int i = 0; i < arow; i++) {
                                for (int j = 0; j < bcol; j++) {
                                        printf("%4d", C[i][j]);
                                }
                                printf("\n");
                        }
                        printf("\n");
                

                final_time = finish_time - start_time;
                printf("Processes: %2d \t Matrix Size: %4d \t Final Time: %f\n\n",
                          processes, n*n, final_time);
        }

        MPI_Finalize();

        return 0;
 }

