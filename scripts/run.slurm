#!/bin/bash
#SBATCH --job-name=methane_analysis
#SBATCH --account=your-account
#SBATCH --partition=notchpeak
#SBATCH --mem=16GB
#SBATCH --time=02:00:00

module load miniconda3
source activate methane_study

# Pass date range as arguments
python src/data_aggregation.py --start-date $1 --end-date $2