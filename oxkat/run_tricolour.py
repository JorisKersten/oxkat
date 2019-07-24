# ian.heywood@physics.ox.ac.uk


import pickle
from optparse import OptionParser


parser = OptionParser(usage='%prog [options] rms_map')
parser.add_option('--col',dest='column_selection',help='Data column',default='DATA')
parser.add_option('--fields',dest='fields',help='Select "cals", "targets" or "all"',default='all')
(options,args) = parser.parse_args()
coloumn_selection = options.coloumn_selection
fields = options.fields


project_info = pickle.load(open('project_info.p','rb'))
bpcal = project_info['primary']
pcal = project_info['secondary']
targets = project_info['target_list'] 


if len(args) != 1:
        myms = project_info['master_ms']
else:
        myms = args[0].rstrip('/')


cal_ids = bpcal[1]+','+pcal[1]
target_ids = []
for target in targets:
    target_ids.append(target[1])
target_ids = ','.join(target_ids)

if fields == 'cals':
    field_selection = cal_ids
elif fields == 'targets':
    field_selection = target_ids
elif field_selection == 'all'
    field_selection = cal_ids+','+target_ids


syscall = 'tricolour '
syscall += '--data-column '+coloumn_selection+' '
syscall += '--field-names '+field_selection+' '
syscall += myms


os.system(sysall)
