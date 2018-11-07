#!/bin/bash
#
#SBATCH --partition=all
#SBATCH --job-name=hello_world_py
#SBATCH --time=01:00
#SBATCH --output=hello_world_py.%j.out
#SBATCH --ntasks=9
#

echo "----------Preparando el entorno-------------"
module purge
module load prun gnu7 openmpi3 py2-mpi4py

echo "-------Inicio de Proceso en mpi Python------"
prun python run8.py
echo "-------Fin de Proceso en mpi Python------"
