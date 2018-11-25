#!/bin/bash
#
#SBATCH --partition=all
#SBATCH --job-name=mpi8
#SBATCH --time=15:00
#SBATCH --output=mpi8_.%j.out
#SBATCH --ntasks=16
#

echo "----------Preparando el entorno-------------"
module purge
module load prun gnu7/7.3.0 openmpi3/3.1.0 py3-mpi4py/3.0.0

echo "-------Inicio de Proceso en mpi Python------"
prun python3 programa.py
echo "-------Fin de Proceso en mpi Python------"
