#include <time.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include<math.h>
#define N   400
#define arow 400
#define acol 400
#define brow 400
#define bcol 400

MPI_Status status;

//Initializing matrices and arrays
double a[arow][acol]; // = {{1,0,0,2},{4,3,0,0},{1,1,0,0},{0,0,0,1}}; To test code
double b[brow][bcol]; // = {{1,2},{3,4},{5,6},{7,8}}; To test code
double c[arow][bcol];
double array[arow*acol];
double arrayb[brow*bcol];

main(int argc, char **argv)
{
  int processes,tasknum,workers,source,dest,rows,off,i,j,k;

  struct timeval start, stop;

//Initializing MPI

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &tasknum);
  MPI_Comm_size(MPI_COMM_WORLD, &processes);

  workers = processes-1;

  // Master process
  if (tasknum == 0) {
  // Generating a sparse matrix and a dense matrix
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
                 a[j][i] = array[(i*arow)+j];
                 b[j][i] = arrayb[(i*arow)+j];
            }

        }


    gettimeofday(&start, 0);

    //Distribute data among processes
    rows=arow/workers;
    off = 0;
//Blocking communication used(Send,Recv)
    for (dest=1; dest<=workers; dest++)
    {
      MPI_Send(&off, 1, MPI_INT, dest, 1, MPI_COMM_WORLD);
      MPI_Send(&rows, 1, MPI_INT, dest, 1, MPI_COMM_WORLD);
      MPI_Send(&a[off][0], rows*acol, MPI_DOUBLE,dest,1, MPI_COMM_WORLD);
      MPI_Send(&b, brow*bcol, MPI_DOUBLE, dest, 1, MPI_COMM_WORLD);
      printf("offset: %d",off);
      printf("rows: %d",rows);
      off = off + rows;
    }

   // Waiting for results from workers
    for (i=1; i<=workers; i++)
    {
      source = i;
      MPI_Recv(&off, 1, MPI_INT, source, 2, MPI_COMM_WORLD, &status);
      MPI_Recv(&rows, 1, MPI_INT, source, 2, MPI_COMM_WORLD, &status);
      MPI_Recv(&c[off][0], rows*bcol, MPI_DOUBLE, source, 2, MPI_COMM_WORLD, &status);
    }

    gettimeofday(&stop, 0);

    printf("Here is the result matrix:\n");
    for (i=0; i<arow; i++) {
      for (j=0; j<bcol; j++){
        printf("%6.2f   ", c[i][j]);
      printf ("\n");
    }

    fprintf(stdout,"Time = %.6f\n\n",
         (stop.tv_sec+stop.tv_usec*1e-6)-(start.tv_sec+start.tv_usec*1e-6));

  }

  //worker process
  if (tasknum > 0) {
    source = 0;
    MPI_Recv(&off, 1, MPI_INT, source, 1, MPI_COMM_WORLD, &status);
    MPI_Recv(&rows, 1, MPI_INT, source, 1, MPI_COMM_WORLD, &status);
    MPI_Recv(&a, rows*acol, MPI_DOUBLE, source, 1, MPI_COMM_WORLD, &status);
    MPI_Recv(&b, brow*bcol, MPI_DOUBLE, source, 1, MPI_COMM_WORLD, &status);

   //Matrix multiplication
    for (k=0; k<bcol; k++)
      for (i=0; i<arow; i++) {
        c[i][k] = 0.0;
        for (j=0; j<brow; j++)
          c[i][k] = c[i][k] + a[i][j] * b[j][k];
      }

//workers sending data to master
    MPI_Send(&off, 1, MPI_INT, 0, 2, MPI_COMM_WORLD);
    MPI_Send(&rows, 1, MPI_INT, 0, 2, MPI_COMM_WORLD);
    MPI_Send(&c, rows*bcol, MPI_DOUBLE, 0, 2, MPI_COMM_WORLD);
  }

  MPI_Finalize();
}
