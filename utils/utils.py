'''
    Name of Module: utils.py
    Contents:
        clean filepath
        get_core_ref
        set_roots
    Contributors: Kyle Simpson
'''
# Import packages
import getpass, sys, yaml
import pandas as pd

nf_repo = '/ihme/code/nfrqe/nf_covid/'


def clean_filepath(path):
    ''' Removes any bracketed items from filepaths

    Arguments:
        path : str
    '''
    if not isinstance(path, str):
        raise TypeError('Supplied path is not a string.')

    if '[mnt]' in path:
        path = path.replace('[mnt]', '/mnt/team/nfrqe/')
    if '[share]' in path:
        path = path.replace('[share]', '/ihme/')
    if '[hsp_icu_input_date]' in path:
        path = path.replace('[hsp_icu_input_date]', get_core_ref('hsp_icu_input_date'))
    if '[infect_death_input_date]' in path:
        path = path.replace('[infect_death_input_date]', get_core_ref('infect_death_input_date'))
    
    return(path)


def get_core_ref(param_name, sub_key=None):
    ''' Convenience function to pull static reference from refs.yaml.

    Arguments:
        param_name : str
            A string containing the reference desired.
        sub_key : str (optional)
            A string containing the sub-reference needed.
    '''
    # Error handling
    if param_name is None:
        raise ValueError('Supplied param_name is None. You must supply a value.')

    # Pull reference
    with open('{}refs.yaml'.format(nf_repo)) as file:
        refs = yaml.full_load(file)
    
    ref = ''
    if sub_key is None:
        ref = refs[param_name]
    else:
        ref = refs[param_name][sub_key]

    # Clean filepath
    if '[' in str(ref):
        ref = clean_filepath(ref)

    return(ref)


def set_roots():
    ''' Convenience function to create root filepaths.

    Arguments:
        None
    '''
    roots = {'j' : '', 'h' : '', 'k' : '', 'share' : '', 'nf_repo' : '', 
             'mnt' : '', 'hsp_icu_input_path' : '', 'infect_death_input_path' : '', 
             'jobmon_logs_base' : '', 'gbd_round' : '', 'decomp_step' : ''}

    if sys.platform.lower() == 'linux':
        roots['j'] = '/home/j/'
        roots['h'] = '/ihme/homes/{}/'.format(getpass.getuser())
        roots['k'] = '/ihme/cc_resources/'
        roots['share'] = '/ihme/'
    elif 'win' in sys.platform.lower():
        roots['j'] = 'J:/'
        roots['h'] = 'H:/'
        roots['k'] = 'K:/'
    
    roots['nf_repo'] = nf_repo
    roots['mnt'] = '/mnt/team/nfrqe/'
    roots['hsp_icu_input_path'] = get_core_ref('hsp_icu_input_path')
    roots['infect_death_input_path'] = get_core_ref('infect_death_input_path')
    roots['jobmon_logs_base'] = get_core_ref('jobmon_logs_base')
    roots['gbd_round'] = get_core_ref('gbd_round_id')
    roots['decomp_step'] = get_core_ref('decomp_step')

    return(roots)


# def get_age_metadata():
#     ''' Returns age group ids and names '''
#     return(pd.DataFrame({'age_group_id' : [22, 5],
#                         'age_group_name' : ['All Ages', '10-14']
#                         }))

roots = set_roots()