##############################
#   Joris Kersten            #
#   kerstenjoris@gmail.com   #
#   Fri 2024-06-07           #
##############################


# This program can be used to do file operations on all subdirectories of a working directory, or on all
# subdirectories with their name in a given list.


# Imports.
from pathlib import Path
# import shutil
import subprocess


# Settings.
WorkingDir = Path("/home/joris/Datadir/PhDProject/MyObjects/V426Oph/PyBDSF")
DirectoryListIPPeg = [
    'IPPeg20210604T0457',
    'IPPeg20210620T0044',
    'IPPeg20210920T1949',
    'IPPeg20210921T1809',
    'IPPeg20220110T1233',
]
DirectoryListV426Oph = [
    'V426Oph20210514T0218_V426Oph2021MayE1_L_1620958578',
    'V426Oph20210630T2211_V426Oph2021JunQ_L_1625091014',
    'V426Oph20220705T2033_V426Oph2022JulE1_L_1657053142',
    'V426Oph20220706T2105_V426Oph2022JulE2_L_1657141429',
    'V426Oph20220707T2117_V426Oph2022JulE3_L_1657228571',
    'V426Oph20220709T1732_V426Oph2022JulE4_L_1657387878',
    'V426Oph20220711T2157_V426Oph2022JulE5_L_1657576575',
    'V426Oph20220712T1732_V426Oph2022JulE6_L_1657647078',
    'V426Oph20220713T2022_V426Oph2022JulE7_L_1657743676',
]
DirectoryListRUPeg = [
    "RUPeg20210427T0618_2021AprE1_1619504195_L",
    "RUPeg20210429T0622_2021AprE2_1619677251_L",
    "RUPeg20210528T0332_2021MayQ_1622172679_L",
    "RUPeg20220603T0012_2022JunE1_1654215068_L",
    "RUPeg20220604T0007_2022JunE2_1654301175_L",
    "RUPeg20220605T0008_2022JunE3_1654387577_L",
    "RUPeg20220606T0002_2022JunE4_1654473674_L",
    "RUPeg20220606T2358_2022JunE5_1654559771_L",
    "RUPeg20220607T2353_2022JunE6_1654645871_L",
    "RUPeg20220608T2347_2022JunE7_1654731977_L",
    "RUPeg20220609T2343_2022JunE8_1654818135_L", 
]


# Print a welcome message.
print("file_operations.py")
print("------------------")
print()


working_dir = WorkingDir.resolve()
dir_list = [working_dir / cur_dir for cur_dir in DirectoryListV426Oph]
# dir_list = sorted([f for f in working_dir.iterdir() if f.is_dir()])


for cur_dir in dir_list:
# for cur_dir in working_dir.iterdir():
    if cur_dir.is_dir() or (not cur_dir.exists()):
        print("Processing: {}\n".format(cur_dir))
        
        # src = Path()
        # dst = Path()
        # shutil.copy2(src, dst, follow_symlinks=False)
        
        cur_cmd = ("rsync -hvPrlpEt --mkpath itransfer:/scratch3/users/jkersten/V426Oph/Oxkat050/{0}/MANUALCLEAN3GC/"
                   "BriggsMinusZeroPointThree/PyBDSF_1_10_4_dev_Output_2.2sigma_3.0sigma_RMSBOX "
                   "{1}/MANUALCLEAN3GC/BriggsMinusZeroPointThree/"
                   .format(cur_dir.name, cur_dir))
        # cur_cmd = "which python"
        
        print("---- Start of subprocess.run() ----")
        subprocess.run(cur_cmd, shell=True, executable="/bin/bash")
        print("---- End of subprocess.run()   ----")
        
        print("\nProcessing done.\n")


# copypairs = []  # A list of lists. Inner lists should have two elements: [src_path, dst_path] .
# for src_dst in copypairs:
#     shutil.copy2(src_dst[0], src_dst[1], follow_symlinks=False)
