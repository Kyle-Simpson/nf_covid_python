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

nf_repo = ''


def clean_filepath(path):
    ''' Removes any bracketed items from filepaths

    Arguments:
        path : str
    '''
    if not isinstance(path, str):
        raise TypeError('Supplied path is not a string.')

    if '[mnt]' in path:
        path = path.replace('[mnt]', '')
    if '[share]' in path:
        path = path.replace('[share]', '')
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
    if ('[' in str(ref)) & (param_name != 'age_group_ids'):
        ref = clean_filepath(ref)

    return(ref)


def set_roots():
    ''' Convenience function to create root filepaths.

    Arguments:
        None
    '''
    roots = {
             # Base paths
             'j' : '', 
             'h' : '', 
             'k' : '', 
             'share' : '', 
             'mnt' : '/mnt/team/nfrqe/', 
             'nf_repo' : nf_repo, 
             # Specific paths
             'hsp_icu_input_path' : get_core_ref('hsp_icu_input_path'), 
             'infect_death_input_path' : get_core_ref('infect_death_input_path'), 
             'age_sex_specific_input_path' : get_core_ref('age_sex_specific_input_path'),
             'disability_weight' : get_core_ref('disability_weight_path'),
             'jobmon_logs_base' : get_core_ref('jobmon_logs_base'), 
             # GBD stuff
             'gbd_round' : get_core_ref('gbd_round_id'), 
             'gbd_year' : get_core_ref('gbd_year'),
             'decomp_step' : get_core_ref('decomp_step'),
             'age_groups' : get_core_ref('age_group_ids'),
             # Default multipliers and date lags
             'defaults' : {'prop_asymp' : get_core_ref('prop_asymp'),
                           'asymp_duration' : get_core_ref('asymp_duration'),
                           'incubation_period' : get_core_ref('incubation_period'),
                           'midmod_duration_no_hsp' : get_core_ref('midmod_duration_no_hsp'),
                           'infect_to_hsp_admit_duration' : get_core_ref('infect_to_hsp_admit_duration'),
                           'symp_to_hsp_admit_duration' : get_core_ref('symp_to_hsp_admit_duration'),
                           'prop_mild' : get_core_ref('prop_mild'),
                           'prop_mod' : get_core_ref('prop_mod'),
                           'icu_to_death_duration' : get_core_ref('icu_to_death_duration'),
                           'hsp_death_duration' : get_core_ref('hsp_death_duration'),
                           'hsp_no_icu_no_death_duration' : get_core_ref('hsp_no_icu_no_death_duration'),
                           'hsp_no_icu_death_duration' : get_core_ref('hsp_no_icu_death_duration'),
                           'hsp_icu_no_death_duration' : get_core_ref('hsp_icu_no_death_duration'),
                           'hsp_icu_death_duration' : get_core_ref('hsp_icu_death_duration'),
                           'icu_no_death_duration' : get_core_ref('icu_no_death_duration'),
                           'hsp_midmod_after_discharge_duration' : get_core_ref('hsp_midmod_after_discharge_duration'),
                           'icu_midmod_after_discharge_duration' : get_core_ref('icu_midmod_after_discharge_duration'),
                           'prop_deaths_icu' : get_core_ref('prop_deaths_icu'),
                           'mild_hhseqid' : get_core_ref('mild_hhseqid'),
                           'moderate_hhseqid' : get_core_ref('moderate_hhseqid'),
                           'severe_hhseqid' : get_core_ref('severe_hhseqid'),
                           'icu_hhseqid' : get_core_ref('icu_hhseqid')
                           }
            }

    if sys.platform.lower() == 'linux':
        roots['j'] = ''
        roots['h'] = ''
        roots['k'] = ''
        roots['share'] = ''
    elif 'win' in sys.platform.lower():
        roots['j'] = ''
        roots['h'] = ''
        roots['k'] = ''

    return(roots)


roots = set_roots()