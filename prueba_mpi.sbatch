#!/bin/bash
#
#SBATCH --partition=all
#SBATCH --job-name=hello_world_py
#SBATCH --time=01:00
#SBATCH --output=hello_world_py.%j.out
#SBATCH --ntasks=10
#

echo "----------Preparando el entorno-------------"
module purge
module load prun gnu7/7.3.0 openmpi3/3.1.0 py2-mpi4py/3.0.0

echo "-------Inicio de Proceso en mpi Python------"
prun hello_world.py
echo "-------Fin de Proceso en mpi Python------"
