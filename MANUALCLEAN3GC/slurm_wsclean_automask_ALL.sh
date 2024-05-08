#!/bin/bash


#file: /scratch3/users/username/sourcename/oxkatname/sourcedir/MANUALCLEAN2GC/slurm_wsclean_automask_ALL.sh:


#SBATCH --job-name=W2E1
#SBATCH --time=48:00:00
#SBATCH --partition=Main
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=115GB
#SBATCH --output=/scratch3/users/username/sourcename/oxkatname/sourcedir/MANUALCLEAN2GC/slurm_wsclean_automask_ALL.log


basedir="/scratch3/users/username/sourcename/oxkatname/sourcedir/MANUALCLEAN2GC"

containervar="/idia/projects/thunderkat/containers/OC/oxkat-0.5_vol2.sif"


namevar="-name ${basedir}/IMAGES/img_msname.ms_post2GC_automask"
tempvar="-temp-dir ${basedir}/automask_temp"
memvar="-abs-mem 105 -mem 95"
outputvar="-verbose -log-time -save-source-list"
datavar="-field 0 -data-column CORRECTED_DATA"
imagevar="-size 10240 10240 -scale 1.1asec -padding 1.2 -channels-out 8 -fit-spectral-pol 4 -join-channels"
gridvar="-gridder wgridder -wgridder-accuracy 1e-5 -parallel-gridding 8"
miscvar="-no-small-inversion -no-update-model-required -parallel-reordering 8 -parallel-deconvolution 2560"
threshvar="-local-rms -local-rms-method rms-with-min -local-rms-window 50 -auto-mask 3.6 -auto-threshold 1.5 -niter 1200000 -gain 0.1 -mgain 0.64"
weightvar="-weight briggs -0.05"
msvar="${basedir}/../msname.ms"

singularity exec $containervar wsclean $tempvar $memvar $outputvar $datavar $imagevar $gridvar $miscvar $threshvar $weightvar $namevar $msvar


sleep 10
