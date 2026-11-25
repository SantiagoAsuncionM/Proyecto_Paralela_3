#include "mpi_cycle.h"

#include <mpi.h>
#include <stdio.h>

int main(int argc, char *argv[]) {
    int provided = 0;
    int rank = 0;
    int size = 0;

    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
    if (provided < MPI_THREAD_FUNNELED) {
        fprintf(stderr, "error: MPI_THREAD_FUNNELED no soportado\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    mpi_cycle_init();

    if (rank == 0) {
        printf("broker %d de %d listo (controlador)\n", rank, size);
    } else {
        printf("broker %d de %d listo\n", rank, size);
    }
    fflush(stdout);

    mpi_cycle_run();

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
