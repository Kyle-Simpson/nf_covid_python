'''
    Script: 6_long_covid.py
    Author: Kyle Simpson
    Last Edit Date: Feb 8, 2021

    Description:
        The premise of this script was to test whether Python or R is the 
        faster / more efficient language for the types of calculations
        required for this analysis. It has long been quoted that Python is 
        (basically) always faster than R for scientific computations, so I
        saught out to test this assumption by translating one of our core
        scripts from R into Python.

    Context:
        At the beginning of the implementation of the non-fatal COVID outcomes
        it was decided that all calculation code would be written in R to allow 
        all current team members to be able to read / write code if necessary. 
        Not everyone knew Python, so it was quickly decided that we would not
        widely use the language for construction of the pipeline. That said, to
        use JOBMON at the time of conception we needed the parent script in
        Python which was the only planned script to be in the language. With
        this in mind, the purpose of this script was not necessarily to replace
        the already written R code with a Python implementation, but was 
        instead for me to challenge myself to consider how the implementation
        would differ between the languages and to learn more about where each
        language gains efficiency over the other.

    Limitations & Future Testing:
        The largest limitation is that I am not 100% versed in writing compact 
        and efficient Python code, meaning there are likely computations that 
        could be expedited by more performant code. The largest performance 
        gain found was implementing the Dataset() class to format, store, and
        compute upon the input data, and there are likely many other efficiencies
        to be gained. In the future, specific areas of improvement could include:
        functionalizing more similar computations between severities, thinking
        of a better way to reshape & merge data sets, and/or further tailoring 
        of the Dataset() class to more efficiently store the +/- 2GB data sets.

    Testing Scenarios:
        Both the R and Python scripts were run using a 25GB 4 thread qsub on 
        the i.q cluster queue with a max runtime of 24 hours. The scripts were 
        equallized in the input and output operations - the R script had 
        disgnostics disabled for the tests as to not add processing time which
        the Python code was not written to perform.
        R qsub: 
            "qsub -e [homes]long_covid_r_errors.txt -o [homes]long_covid_r_output.txt -N r_long_covid 
            -l archive=True -q i.q -P proj_nfrqe -l fthread=4 -l m_mem_free=25G 
            -l h_rt=24:00:00 [share]singularity-images/rstudio/shells/execRscript.sh 
            -i [share]singularity-images/rstudio/ihme_rstudio_4030.img 
            -s [homes]repos/nf_covid/src/6_long_covid.R"
        Python qsub:
            "qsub -e [homes]long_covid_python_errors.txt -o 
            [homes]long_covid_python_output.txt -N python_long_covid 
            -l archive=True -q i.q -P proj_nfrqe -l fthread=4 -l m_mem_free=25G 
            -l h_rt=24:00:00 [homes]repos/surge_utils/shell_python.sh 
            [homes]repos/nf_covid/src/6_long_covid.py"

    Findings:
        Runtime (in HH:MM:SS):
            |---------------|-------------------|
            |       R       |       Python      |
            |---------------|-------------------|
            |    00:03:13   |      00:09:47     |
            |---------------|-------------------|
        Memory (in GBs) Used:
            |---------------|-------------------|
            |       R       |       Python      |
            |---------------|-------------------|
            |      13.6     |        22.7       |
            |---------------|-------------------|

    Conclusion:
        Though the description of this analysis quotes the goal as being "finding 
        out which language is faster", the truth of the matter is that one 
        language will never be a "one-size-fits-all" solution. Each language
        excels at different operations and has sets of use cases where one would
        be preferred over the other - like the one noted above where not all 
        team members know Python. The conclusion of this analysis does not 
        attempt to ration one language as definitively better, but does quote
        R as being the faster language for the implementations presented above.
'''

from classes.Dataset import Dataset
from nf_covid.utils.utils import get_core_ref, roots
from db_queries import get_population
import datetime, copy
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")


def calc_prev(df, dp, dst_population, dst_outcome, calc_col_stub):
    ''' Convenience function to calculate prevalence considering duration scaling
    Arguments:
        df : Pandas DataFrame
        dp : Pandas DataFrame
        dst_population : str
        dst_outcome : str
        calc_col_stub : str
    '''
    # Setup durations
    EOY = datetime.datetime(2020, 12, 31)
    durs = pd.DataFrame({
        'date' : df.date.unique(),
        'duration_mean' : pd.to_timedelta(round(dp.loc[(dp.outcome==dst_outcome) & 
                                                       (dp.population==dst_population), 
                                                       'duration_mean'].values[0] * 366), 
                                          unit='D')
        })

    # EOY duration scaling
    durs['new_duration'] = EOY - durs.date
    durs.loc[durs.new_duration < durs.duration_mean, 
             'duration_mean'] = durs.new_duration
    durs.loc[durs.duration_mean < pd.to_timedelta(0, unit='D'), 
             'duration_mean'] = pd.to_timedelta(0, unit='D')
    durs.drop(columns='new_duration', inplace=True)
    durs.duration_mean = durs.duration_mean.dt.days

    # Merge durations with input data
    df = pd.merge(df, durs, on='date', how='left')

    # Calculate prevalence
    df['{}prev'.format(calc_col_stub)] = (df['{}inc'.format(calc_col_stub)] * 
                                          df.duration_mean)
    df.drop(columns='duration_mean', inplace=True)
    return(df)


def main(loc_id, loc_name, output_version):
    print('Reading in short-term outcomes...')
    ## Read in short-term outcomes
    # region -------------------------------------------------------------------
    
    # Durations and proportions
    dp = pd.read_csv('{}WORK/12_bundle/covid/data/long_covid/long_covid_proportions_durations_with_overlaps.csv'.format(roots['j']))
    
    # Mild/Moderate
    print('  mild/moderate...')
    midmod = Dataset(loc_id, loc_name, output_version, 'midmod', nf_type='long')
    
    # Hospital
    print('  hospital...')
    hospital = Dataset(loc_id, loc_name, output_version, 'hsp_admit', nf_type='long')

    # Icu
    print('  icu...')
    icu = Dataset(loc_id, loc_name, output_version, 'icu_admit', nf_type='long')

    # endregion ----------------------------------------------------------------
    

    print('Calculating mild/moderate incidence & prevalence...')
    ## Mild/Moderate Incidence & Prevalence
    # region -------------------------------------------------------------------
    # Shift hospitalizations 7 days
    lag_hsp = copy.deepcopy(hospital)
    lag_hsp.data = lag_hsp.data.drop(columns=['hospital_deaths'])
    lag_hsp.data.date = lag_hsp.data.date + pd.to_timedelta(roots['defaults']['symp_to_hsp_admit_duration'], unit='D')


    # Merge midmod and lag_hsp
    midmod.data = pd.merge(midmod.data, lag_hsp.data, how='left',
                           on=['location_id', 'age_group_id', 'sex_id', 
                               'draw_var', 'date'])
    del lag_hsp
    

    # mild/moderate at risk number = (mild/moderate incidence - hospital admissions|7 days later) |
    #                                 shift forward by {incubation period + mild/moderate duration|no hospital}
    midmod.data['midmod_risk_num'] = midmod.data.midmod_inc - midmod.data.hospital_inc
    midmod.data.date = midmod.data.date + pd.to_timedelta((roots['defaults']['incubation_period'] + 
                                                           roots['defaults']['midmod_duration_no_hsp']), unit='D')


    # Calculate the incidence of each symptom and overlap, regardless of co-occurrence of additional symptoms (not mutually exclusive)
    # mild/moderate long-term incidence = mild/moderate number at risk * proportion of mild/moderate with each long-term symptom cluster
    midmod.data['midmod_cog_inc'] = (midmod.data.midmod_risk_num * 
                                     dp.loc[(dp.outcome=='cognitive') & 
                                            (dp.population=='midmod'), 
                                            'proportion_mean'].values[0])
    midmod.data['midmod_fat_inc'] = (midmod.data.midmod_risk_num * 
                                     dp.loc[(dp.outcome=='fatigue') & 
                                            (dp.population=='midmod'), 
                                            'proportion_mean'].values[0])
    midmod.data['midmod_resp_inc'] = (midmod.data.midmod_risk_num * 
                                      dp.loc[(dp.outcome=='respiratory') & 
                                             (dp.population=='midmod'), 
                                             'proportion_mean'].values[0])
    midmod.data['midmod_cog_fat_inc'] = (midmod.data.midmod_risk_num *
                                         dp.loc[(dp.outcome=='cognitive_fatigue') & 
                                                (dp.population=='midmod'), 
                                                'proportion_mean'].values[0])
    midmod.data['midmod_cog_resp_inc'] = (midmod.data.midmod_risk_num *
                                          dp.loc[(dp.outcome=='cognitive_respiratory') & 
                                                 (dp.population=='midmod'), 
                                                 'proportion_mean'].values[0])
    midmod.data['midmod_fat_resp_inc'] = (midmod.data.midmod_risk_num *
                                          dp.loc[(dp.outcome=='fatigue_respiratory') & 
                                                 (dp.population=='midmod'), 
                                                 'proportion_mean'].values[0])
    midmod.data['midmod_cog_fat_resp_inc'] = (midmod.data.midmod_risk_num *
                                              dp.loc[(dp.outcome=='cognitive_fatigue_respiratory') & 
                                                     (dp.population=='midmod'), 
                                                     'proportion_mean'].values[0])

    # Creating mutually exclusive categories of symptoms
    # cog_inc = cog_inc - (cog_fat_inc - cog_fat_resp_inc) - (cog_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    midmod.data.midmod_cog_inc = (midmod.data.midmod_cog_inc - 
                                  (midmod.data.midmod_cog_fat_inc - midmod.data.midmod_cog_fat_resp_inc) - 
                                  (midmod.data.midmod_cog_resp_inc - midmod.data.midmod_cog_fat_resp_inc) - 
                                  midmod.data.midmod_cog_fat_resp_inc)

    # fat_inc = fat_inc - (cog_fat_inc - cog_fat_resp_inc) -  (fat_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    midmod.data.midmod_fat_inc = (midmod.data.midmod_fat_inc - 
                                  (midmod.data.midmod_cog_fat_inc - midmod.data.midmod_cog_fat_resp_inc) - 
                                  (midmod.data.midmod_fat_resp_inc - midmod.data.midmod_cog_fat_resp_inc) - 
                                  midmod.data.midmod_cog_fat_resp_inc)

    # resp_inc = resp_inc - (fat_resp_inc - cog_fat_resp_inc) - (cog_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    midmod.data.midmod_resp_inc = (midmod.data.midmod_resp_inc - 
                                   (midmod.data.midmod_fat_resp_inc - midmod.data.midmod_cog_fat_resp_inc) - 
                                   (midmod.data.midmod_cog_resp_inc - midmod.data.midmod_cog_fat_resp_inc) - 
                                   midmod.data.midmod_cog_fat_resp_inc)

    # cog_fat_inc = cog_fat_inc - cog_fat_resp_inc
    midmod.data.midmod_cog_fat_inc = (midmod.data.midmod_cog_fat_inc - midmod.data.midmod_cog_fat_resp_inc)
    
    # cog_resp_inc = cog_resp_inc - cog_fat_resp_inc
    midmod.data.midmod_cog_resp_inc = (midmod.data.midmod_cog_resp_inc - midmod.data.midmod_cog_fat_resp_inc)
    
    # fat_resp_inc = fat_resp_inc - cog_fat_resp_inc
    midmod.data.midmod_fat_resp_inc = (midmod.data.midmod_fat_resp_inc - midmod.data.midmod_cog_fat_resp_inc)

    
    # mild/moderate long-term prevalence = mild/moderate long-term incidence * [duration]
    midmod.data = calc_prev(df=midmod.data, dp=dp, dst_population='midmod', 
                       dst_outcome='cognitive', calc_col_stub='midmod_cog_')
    midmod.data = calc_prev(df=midmod.data, dp=dp, dst_population='midmod', 
                       dst_outcome='fatigue', calc_col_stub='midmod_fat_')
    midmod.data = calc_prev(df=midmod.data, dp=dp, dst_population='midmod', 
                       dst_outcome='respiratory', calc_col_stub='midmod_resp_')
    midmod.data = calc_prev(df=midmod.data, dp=dp, dst_population='midmod', 
                       dst_outcome='cognitive_fatigue', calc_col_stub='midmod_cog_fat_')
    midmod.data = calc_prev(df=midmod.data, dp=dp, dst_population='midmod', 
                       dst_outcome='cognitive_respiratory', calc_col_stub='midmod_cog_resp_')
    midmod.data = calc_prev(df=midmod.data, dp=dp, dst_population='midmod', 
                       dst_outcome='fatigue_respiratory', calc_col_stub='midmod_fat_resp_')
    midmod.data = calc_prev(df=midmod.data, dp=dp, dst_population='midmod', 
                       dst_outcome='cognitive_fatigue_respiratory', 
                       calc_col_stub='midmod_cog_fat_resp_')


    # Drop unneeded cols
    midmod.data = midmod.data.drop(columns=['midmod_inc', 'hospital_inc', 'midmod_risk_num'])

    # endregion ----------------------------------------------------------------


    print('Calculating severe incidence and prevalence...')
    ## Severe Incidence & Prevalence
    # region -------------------------------------------------------------------

    # Shift icu admissions
    lag_icu = copy.deepcopy(icu)
    lag_icu.data = lag_icu.data.drop(columns=['icu_deaths'])
    lag_icu.data.date = lag_icu.data.date + pd.to_timedelta(roots['defaults']['icu_to_death_duration'], unit='D')


    # Shift hospital deaths
    lag_hsp = copy.deepcopy(hospital)
    lag_hsp.data = lag_hsp.data.drop(columns=['hospital_inc'])
    lag_hsp.data.date = lag_hsp.data.date + pd.to_timedelta(roots['defaults']['hsp_no_icu_death_duration'], unit='D')


    # Merge lagged datasets
    lag = pd.merge(lag_icu.data, lag_hsp.data, how='left',
                   on=['location_id', 'age_group_id', 'sex_id', 
                        'draw_var', 'date'])
    del lag_icu, lag_hsp
    hospital.data = pd.merge(hospital.data.drop(columns=['hospital_deaths']), 
                             lag, how='left', on=['location_id', 'age_group_id', 
                                                  'sex_id', 'draw_var', 'date'])
    del lag


    # severe at risk number = (hospital admissions - ICU admissions|3 days later - hospital deaths|6 days later) |
    #                          shift forward by {hospital duration if no ICU no death + hospital mild moderate duration after discharge}
    hospital.data['hospital_risk_num'] = (hospital.data.hospital_inc - hospital.data.icu_inc - 
                                          hospital.data.hospital_deaths)
    hospital.data.date = hospital.data.date + pd.to_timedelta((roots['defaults']['hsp_no_icu_no_death_duration'] + 
                                                               roots['defaults']['hsp_midmod_after_discharge_duration']), unit='D')


    # Calculate the incidence of each symptom and overlap, regardless of co-occurrence of additional symptoms (not mutually exclusive)
    # severe long-term incidence = severe at risk number * proportion of severe survivors with each long-term symptom cluster
    hospital.data['hospital_cog_inc'] = (hospital.data.hospital_risk_num * 
                                         dp.loc[(dp.outcome=='cognitive') & 
                                                (dp.population=='hospital'), 
                                                'proportion_mean'].values[0])
    hospital.data['hospital_fat_inc'] = (hospital.data.hospital_risk_num * 
                                         dp.loc[(dp.outcome=='fatigue') & 
                                                (dp.population=='hospital'), 
                                                'proportion_mean'].values[0])
    hospital.data['hospital_resp_inc'] = (hospital.data.hospital_risk_num * 
                                          dp.loc[(dp.outcome=='respiratory') & 
                                                 (dp.population=='hospital'), 
                                                 'proportion_mean'].values[0])
    hospital.data['hospital_cog_fat_inc'] = (hospital.data.hospital_risk_num *
                                             dp.loc[(dp.outcome=='cognitive_fatigue') & 
                                                    (dp.population=='hospital'), 
                                                    'proportion_mean'].values[0])
    hospital.data['hospital_cog_resp_inc'] = (hospital.data.hospital_risk_num *
                                              dp.loc[(dp.outcome=='cognitive_respiratory') & 
                                                     (dp.population=='hospital'), 
                                                     'proportion_mean'].values[0])
    hospital.data['hospital_fat_resp_inc'] = (hospital.data.hospital_risk_num *
                                              dp.loc[(dp.outcome=='fatigue_respiratory') & 
                                                     (dp.population=='hospital'), 
                                                     'proportion_mean'].values[0])
    hospital.data['hospital_cog_fat_resp_inc'] = (hospital.data.hospital_risk_num *
                                                  dp.loc[(dp.outcome=='cognitive_fatigue_respiratory') & 
                                                         (dp.population=='hospital'), 
                                                         'proportion_mean'].values[0])

    # Creating mutually exclusive categories of symptoms
    # cog_inc = cog_inc - (cog_fat_inc - cog_fat_resp_inc) - (cog_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    hospital.data.hospital_cog_inc = (hospital.data.hospital_cog_inc - 
                                      (hospital.data.hospital_cog_fat_inc - hospital.data.hospital_cog_fat_resp_inc) - 
                                      (hospital.data.hospital_cog_resp_inc - hospital.data.hospital_cog_fat_resp_inc) - 
                                      hospital.data.hospital_cog_fat_resp_inc)

    # fat_inc = fat_inc - (cog_fat_inc - cog_fat_resp_inc) -  (fat_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    hospital.data.hospital_fat_inc = (hospital.data.hospital_fat_inc - 
                                      (hospital.data.hospital_cog_fat_inc - hospital.data.hospital_cog_fat_resp_inc) - 
                                      (hospital.data.hospital_fat_resp_inc - hospital.data.hospital_cog_fat_resp_inc) - 
                                      hospital.data.hospital_cog_fat_resp_inc)

    # resp_inc = resp_inc - (fat_resp_inc - cog_fat_resp_inc) - (cog_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    hospital.data.hospital_resp_inc = (hospital.data.hospital_resp_inc - 
                                       (hospital.data.hospital_fat_resp_inc - hospital.data.hospital_cog_fat_resp_inc) - 
                                       (hospital.data.hospital_cog_resp_inc - hospital.data.hospital_cog_fat_resp_inc) - 
                                       hospital.data.hospital_cog_fat_resp_inc)

    # cog_fat_inc = cog_fat_inc - cog_fat_resp_inc
    hospital.data.hospital_cog_fat_inc = (hospital.data.hospital_cog_fat_inc - hospital.data.hospital_cog_fat_resp_inc)
    
    # cog_resp_inc = cog_resp_inc - cog_fat_resp_inc
    hospital.data.hospital_cog_resp_inc = (hospital.data.hospital_cog_resp_inc - hospital.data.hospital_cog_fat_resp_inc)
    
    # fat_resp_inc = fat_resp_inc - cog_fat_resp_inc
    hospital.data.hospital_fat_resp_inc = (hospital.data.hospital_fat_resp_inc - hospital.data.hospital_cog_fat_resp_inc)
    

    # severe long-term prevalence = severe long-term incidence * [duration]
    hospital.data = calc_prev(df=hospital.data, dp=dp, dst_population='hospital', 
                       dst_outcome='cognitive', calc_col_stub='hospital_cog_')
    hospital.data = calc_prev(df=hospital.data, dp=dp, dst_population='hospital', 
                       dst_outcome='fatigue', calc_col_stub='hospital_fat_')
    hospital.data = calc_prev(df=hospital.data, dp=dp, dst_population='hospital', 
                       dst_outcome='respiratory', calc_col_stub='hospital_resp_')
    hospital.data = calc_prev(df=hospital.data, dp=dp, dst_population='hospital', 
                       dst_outcome='cognitive_fatigue', calc_col_stub='hospital_cog_fat_')
    hospital.data = calc_prev(df=hospital.data, dp=dp, dst_population='hospital', 
                       dst_outcome='cognitive_respiratory', calc_col_stub='hospital_cog_resp_')
    hospital.data = calc_prev(df=hospital.data, dp=dp, dst_population='hospital', 
                       dst_outcome='fatigue_respiratory', calc_col_stub='hospital_fat_resp_')
    hospital.data = calc_prev(df=hospital.data, dp=dp, dst_population='hospital', 
                       dst_outcome='cognitive_fatigue_respiratory', 
                       calc_col_stub='hospital_cog_fat_resp_')


    # Remove unneeded cols
    hospital.data = hospital.data.drop(columns=['hospital_inc', 'icu_inc', 'hospital_deaths', 
                                                'hospital_risk_num'])
    
    # endregion ----------------------------------------------------------------


    print('Calculating critical incidence and prevalence...')
    ## Critical Incidence & Prevalence
    # region -------------------------------------------------------------------

    # Shift icu deaths
    lag_icu = copy.deepcopy(icu)
    lag_icu.data = lag_icu.data.drop(columns='icu_inc')
    lag_icu.data.date = lag_icu.data.date + pd.to_timedelta(roots['defaults']['icu_to_death_duration'], unit='D')


    # Merge icu and lag_icu
    icu.data = pd.merge(icu.data.drop(columns='icu_deaths'),
                        lag_icu.data, how='left', on=['location_id', 'age_group_id', 
                                                      'sex_id', 'draw_var', 'date'])
    del lag_icu


    # critical at risk number = (ICU admissions - ICU deaths|3 days later) |
    #                            shift forward by {ICU duration if no death + ICU mild moderate duration after discharge}
    icu.data['icu_risk_num'] = icu.data.icu_inc - icu.data.icu_deaths
    icu.data.date = icu.data.date - pd.to_timedelta((roots['defaults']['icu_no_death_duration'] + 
                                                     roots['defaults']['icu_midmod_after_discharge_duration']), unit='D')


    # Calculate the incidence of each symptom and overlap, regardless of co-occurrence of additional symptoms (not mutually exclusive)
    # critical long-term incidence = critical number at risk * proportion of critical with each long-term symptom cluster
    icu.data['icu_cog_inc'] = (icu.data.icu_risk_num * 
                               dp.loc[(dp.outcome=='cognitive') & 
                                      (dp.population=='icu'), 
                                      'proportion_mean'].values[0])
    icu.data['icu_fat_inc'] = (icu.data.icu_risk_num * 
                               dp.loc[(dp.outcome=='fatigue') & 
                                      (dp.population=='icu'), 
                                      'proportion_mean'].values[0])
    icu.data['icu_resp_inc'] = (icu.data.icu_risk_num * 
                                dp.loc[(dp.outcome=='respiratory') & 
                                       (dp.population=='icu'), 
                                       'proportion_mean'].values[0])
    icu.data['icu_cog_fat_inc'] = (icu.data.icu_risk_num *
                                   dp.loc[(dp.outcome=='cognitive_fatigue') & 
                                          (dp.population=='icu'), 
                                          'proportion_mean'].values[0])
    icu.data['icu_cog_resp_inc'] = (icu.data.icu_risk_num *
                                    dp.loc[(dp.outcome=='cognitive_respiratory') & 
                                           (dp.population=='icu'), 
                                           'proportion_mean'].values[0])
    icu.data['icu_fat_resp_inc'] = (icu.data.icu_risk_num *
                                    dp.loc[(dp.outcome=='fatigue_respiratory') & 
                                           (dp.population=='icu'), 
                                           'proportion_mean'].values[0])
    icu.data['icu_cog_fat_resp_inc'] = (icu.data.icu_risk_num *
                                        dp.loc[(dp.outcome=='cognitive_fatigue_respiratory') & 
                                               (dp.population=='icu'), 
                                               'proportion_mean'].values[0])

    # Creating mutually exclusive categories of symptoms
    # cog_inc = cog_inc - (cog_fat_inc - cog_fat_resp_inc) - (cog_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    icu.data.icu_cog_inc = (icu.data.icu_cog_inc - 
                            (icu.data.icu_cog_fat_inc - icu.data.icu_cog_fat_resp_inc) - 
                            (icu.data.icu_cog_resp_inc - icu.data.icu_cog_fat_resp_inc) - 
                            icu.data.icu_cog_fat_resp_inc)

    # fat_inc = fat_inc - (cog_fat_inc - cog_fat_resp_inc) -  (fat_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    icu.data.icu_fat_inc = (icu.data.icu_fat_inc - 
                            (icu.data.icu_cog_fat_inc - icu.data.icu_cog_fat_resp_inc) - 
                            (icu.data.icu_fat_resp_inc - icu.data.icu_cog_fat_resp_inc) - 
                            icu.data.icu_cog_fat_resp_inc)

    # resp_inc = resp_inc - (fat_resp_inc - cog_fat_resp_inc) - (cog_resp_inc - cog_fat_resp_inc) - cog_fat_resp_inc
    icu.data.icu_resp_inc = (icu.data.icu_resp_inc - 
                             (icu.data.icu_fat_resp_inc - icu.data.icu_cog_fat_resp_inc) - 
                             (icu.data.icu_cog_resp_inc - icu.data.icu_cog_fat_resp_inc) - 
                             icu.data.icu_cog_fat_resp_inc)

    # cog_fat_inc = cog_fat_inc - cog_fat_resp_inc
    icu.data.icu_cog_fat_inc = (icu.data.icu_cog_fat_inc - icu.data.icu_cog_fat_resp_inc)
    
    # cog_resp_inc = cog_resp_inc - cog_fat_resp_inc
    icu.data.icu_cog_resp_inc = (icu.data.icu_cog_resp_inc - icu.data.icu_cog_fat_resp_inc)
    
    # fat_resp_inc = fat_resp_inc - cog_fat_resp_inc
    icu.data.icu_fat_resp_inc = (icu.data.icu_fat_resp_inc - icu.data.icu_cog_fat_resp_inc)
    

    # critical long-term prevalence = critical long-term incidence * [duration]
    icu.data = calc_prev(df=icu.data, dp=dp, dst_population='icu', 
                         dst_outcome='cognitive', calc_col_stub='icu_cog_')
    icu.data = calc_prev(df=icu.data, dp=dp, dst_population='icu', 
                         dst_outcome='fatigue', calc_col_stub='icu_fat_')
    icu.data = calc_prev(df=icu.data, dp=dp, dst_population='icu', 
                         dst_outcome='respiratory', calc_col_stub='icu_resp_')
    icu.data = calc_prev(df=icu.data, dp=dp, dst_population='icu', 
                         dst_outcome='cognitive_fatigue', calc_col_stub='icu_cog_fat_')
    icu.data = calc_prev(df=icu.data, dp=dp, dst_population='icu', 
                         dst_outcome='cognitive_respiratory', calc_col_stub='icu_cog_resp_')
    icu.data = calc_prev(df=icu.data, dp=dp, dst_population='icu', 
                         dst_outcome='fatigue_respiratory', calc_col_stub='icu_fat_resp_')
    icu.data = calc_prev(df=icu.data, dp=dp, dst_population='icu', 
                         dst_outcome='cognitive_fatigue_respiratory', 
                         calc_col_stub='icu_cog_fat_resp_')


    # Remove unneeded cols
    icu.data = icu.data.drop(columns=['icu_inc', 'icu_deaths', 'icu_risk_num'])
    del dp

    # endregion ----------------------------------------------------------------


    print('Aggregating severities...')
    ## Aggregate Severities
    # region -------------------------------------------------------------------
    
    df = copy.deepcopy(midmod)
    del midmod
    df.data = pd.merge(df.data, hospital.data, how='outer',
                       on=['location_id', 'age_group_id', 'sex_id', 'draw_var', 
                           'date'])
    del hospital
    df.data = pd.merge(df.data, icu.data, how='outer',
                       on=['location_id', 'age_group_id', 'sex_id', 'draw_var', 
                           'date'])
    del icu


    # Incidence
    df.data['cognitive_inc'] = df.data[['midmod_cog_inc', 'hospital_cog_inc', 
                                        'icu_cog_inc']].sum(axis=1)
    df.data.drop(columns=['midmod_cog_inc', 'hospital_cog_inc', 'icu_cog_inc'], 
                 inplace=True)
    df.data['fatigue_inc'] = df.data[['midmod_fat_inc', 'hospital_fat_inc', 
                                      'icu_fat_inc']].sum(axis=1)
    df.data.drop(columns=['midmod_fat_inc', 'hospital_fat_inc', 'icu_fat_inc'], 
                 inplace=True)
    df.data['respiratory_inc'] = df.data[['midmod_resp_inc', 'hospital_resp_inc', 
                                          'icu_resp_inc']].sum(axis=1)
    df.data.drop(columns=['midmod_resp_inc', 'hospital_resp_inc', 'icu_resp_inc'], 
                 inplace=True)
    df.data['cognitive_fatigue_inc'] = df.data[['midmod_cog_fat_inc', 'hospital_cog_fat_inc', 
                                                'icu_cog_fat_inc']].sum(axis=1)
    df.data.drop(columns=['midmod_cog_fat_inc', 'hospital_cog_fat_inc', 'icu_cog_fat_inc'], 
                 inplace=True)
    df.data['cognitive_respiratory_inc'] = df.data[['midmod_cog_resp_inc',
                                                    'hospital_cog_resp_inc',
                                                    'icu_cog_resp_inc']].sum(axis=1)
    df.data.drop(columns=['midmod_cog_resp_inc', 'hospital_cog_resp_inc', 'icu_cog_resp_inc'], 
                 inplace=True)
    df.data['fatigue_respiratory_inc'] = df.data[['midmod_fat_resp_inc', 
                                                  'hospital_fat_resp_inc', 
                                                  'icu_fat_resp_inc']].sum(axis=1)
    df.data.drop(columns=['midmod_fat_resp_inc', 'hospital_fat_resp_inc', 'icu_fat_resp_inc'], 
                 inplace=True)
    df.data['cognitive_fatigue_respiratory_inc'] = df.data[['midmod_cog_fat_resp_inc', 
                                                            'hospital_cog_fat_resp_inc', 
                                                            'icu_cog_fat_resp_inc'
                                                            ]].sum(axis=1)
    df.data.drop(columns=['midmod_cog_fat_resp_inc', 'hospital_cog_fat_resp_inc', 
                          'icu_cog_fat_resp_inc'], inplace=True)

    
    # Prevalence
    df.data['cognitive_prev'] = df.data[['midmod_cog_prev', 'hospital_cog_prev', 
                                         'icu_cog_prev']].sum(axis=1)
    df.data.drop(columns=['midmod_cog_prev', 'hospital_cog_prev', 'icu_cog_prev'], 
                 inplace=True)
    df.data['fatigue_prev'] = df.data[['midmod_fat_prev', 'hospital_fat_prev', 
                                       'icu_fat_prev']].sum(axis=1)
    df.data.drop(columns=['midmod_fat_prev', 'hospital_fat_prev', 'icu_fat_prev'], 
                 inplace=True)
    df.data['respiratory_prev'] = df.data[['midmod_resp_prev', 'hospital_resp_prev', 
                                           'icu_resp_prev']].sum(axis=1)
    df.data.drop(columns=['midmod_resp_prev', 'hospital_resp_prev', 'icu_resp_prev'], 
                 inplace=True)
    df.data['cognitive_fatigue_prev'] = df.data[['midmod_cog_fat_prev', 'hospital_cog_fat_prev', 
                                                 'icu_cog_fat_prev']].sum(axis=1)
    df.data.drop(columns=['midmod_cog_fat_prev', 'hospital_cog_fat_prev', 'icu_cog_fat_prev'], 
                 inplace=True)
    df.data['cognitive_respiratory_prev'] = df.data[['midmod_cog_resp_prev',
                                                     'hospital_cog_resp_prev',
                                                     'icu_cog_resp_prev']].sum(axis=1)
    df.data.drop(columns=['midmod_cog_resp_prev', 'hospital_cog_resp_prev', 'icu_cog_resp_prev'], 
                 inplace=True)
    df.data['fatigue_respiratory_prev'] = df.data[['midmod_fat_resp_prev', 
                                                   'hospital_fat_resp_prev', 
                                                   'icu_fat_resp_prev']].sum(axis=1)
    df.data.drop(columns=['midmod_fat_resp_prev', 'hospital_fat_resp_prev', 'icu_fat_resp_prev'], 
                 inplace=True)
    df.data['cognitive_fatigue_respiratory_prev'] = df.data[['midmod_cog_fat_resp_prev', 
                                                             'hospital_cog_fat_resp_prev', 
                                                             'icu_cog_fat_resp_prev'
                                                             ]].sum(axis=1)
    df.data.drop(columns=['midmod_cog_fat_resp_prev', 'hospital_cog_fat_resp_prev', 
                           'icu_cog_fat_resp_prev'], inplace=True)

    # endregion ----------------------------------------------------------------


    print('Aggregating by year...')
    ## Aggregate by year
    # region -------------------------------------------------------------------

    # Subset to 2020
    df.data = df.data[(df.data.date >= datetime.datetime(2020, 1, 1)) &
                       (df.data.date <= datetime.datetime(2020, 12, 31))]


    # Sum by day
    df.collapse(agg_function='sum',
                group_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var'],
                calc_cols=['cognitive_inc', 'cognitive_prev', 'fatigue_inc', 'fatigue_prev',
                           'respiratory_inc', 'respiratory_prev', 'cognitive_fatigue_inc', 
                           'cognitive_fatigue_prev','cognitive_respiratory_inc', 
                           'cognitive_respiratory_prev', 'fatigue_respiratory_inc', 
                           'fatigue_respiratory_prev','cognitive_fatigue_respiratory_inc', 
                           'cognitive_fatigue_respiratory_prev'])


    # Divide prevalence by 366
    df.data.cognitive_prev = df.data.cognitive_prev / 366
    df.data.fatigue_prev = df.data.fatigue_prev / 366
    df.data.respiratory_prev = df.data.respiratory_prev / 366
    df.data.cognitive_fatigue_prev = df.data.cognitive_fatigue_prev / 366
    df.data.cognitive_respiratory_prev = df.data.cognitive_respiratory_prev / 366
    df.data.fatigue_respiratory_prev = df.data.fatigue_respiratory_prev / 366
    df.data.cognitive_fatigue_respiratory_prev = df.data.cognitive_fatigue_respiratory_prev / 366


    # Ensure incidence and prevalence aren't negative
    df.check_neg(calc_cols=['cognitive_inc', 'cognitive_prev', 'fatigue_inc', 'fatigue_prev',
                            'respiratory_inc', 'respiratory_prev', 'cognitive_fatigue_inc', 
                            'cognitive_fatigue_prev','cognitive_respiratory_inc', 
                            'cognitive_respiratory_prev', 'fatigue_respiratory_inc', 
                            'fatigue_respiratory_prev','cognitive_fatigue_respiratory_inc', 
                            'cognitive_fatigue_respiratory_prev'])

    # endregion ----------------------------------------------------------------


    print('Calculating rates...')
    ## Calculate rates
    # region -------------------------------------------------------------------

    # Pull population
    pop = get_population(age_group_id = roots['age_groups'],
                         single_year_age = False, location_id = loc_id, 
                         location_set_id = 35, year_id = roots['gbd_year'], 
                         sex_id = [1,2], gbd_round_id = roots['gbd_round'], 
                         status = 'best', decomp_step = roots['decomp_step'])
    pop.drop(columns=['year_id', 'run_id'], inplace=True)

    
    # Merge population
    df.data = pd.merge(df.data, pop, how='left',
                       on=['location_id', 'age_group_id', 'sex_id'])


    # Calculate rates
    df.data['cognitive_inc_rate'] = df.data.cognitive_inc / df.data.population
    df.data['fatigue_inc_rate'] = df.data.fatigue_inc / df.data.population
    df.data['respiratory_inc_rate'] = df.data.respiratory_inc / df.data.population
    df.data['cognitive_fatigue_inc_rate'] = df.data.cognitive_fatigue_inc / df.data.population
    df.data['cognitive_respiratory_inc_rate'] = df.data.cognitive_respiratory_inc / df.data.population
    df.data['fatigue_respiratory_inc_rate'] = df.data.fatigue_respiratory_inc / df.data.population
    df.data['cognitive_fatigue_respiratory_inc_rate'] = df.data.cognitive_fatigue_respiratory_inc / df.data.population

    df.data['cognitive_prev_rate'] = df.data.cognitive_prev / df.data.population
    df.data['fatigue_prev_rate'] = df.data.fatigue_prev / df.data.population
    df.data['respiratory_prev_rate'] = df.data.respiratory_prev / df.data.population
    df.data['cognitive_fatigue_prev_rate'] = df.data.cognitive_fatigue_prev / df.data.population
    df.data['cognitive_respiratory_prev_rate'] = df.data.cognitive_respiratory_prev / df.data.population
    df.data['fatigue_respiratory_prev_rate'] = df.data.fatigue_respiratory_prev / df.data.population
    df.data['cognitive_fatigue_respiratory_prev_rate'] = df.data.cognitive_fatigue_respiratory_prev / df.data.population
    # endregion ----------------------------------------------------------------


    print('Calculating YLDs...')
    ## Calculate YLDs
    # region -------------------------------------------------------------------

    # Read in disability weights
    dw = pd.read_csv('{}dws.csv'.format(roots['disability_weight']))

    # Temporary values
    df.data['cognitive_YLD'] = df.data.cognitive_prev_rate * 0.01
    df.data['fatigue_YLD'] = df.data.fatigue_prev_rate * 0.01
    df.data['respiratory_YLD'] = df.data.respiratory_prev_rate * 0.01
    df.data['cognitive_fatigue_YLD'] = df.data.cognitive_fatigue_prev_rate * 0.01
    df.data['cognitive_respiratory_YLD'] = df.data.cognitive_respiratory_prev_rate * 0.01
    df.data['fatigue_respiratory_YLD'] = df.data.fatigue_respiratory_prev_rate * 0.01
    df.data['cognitive_fatigue_respiratory_YLD'] = df.data.cognitive_fatigue_respiratory_prev_rate * 0.01

    del dw

    # endregion ----------------------------------------------------------------


    print('Saving datasets and running diagnostics...')
    ## Save datasets & run diagnostics
    # region -------------------------------------------------------------------

    # Cognitive
    df.save_data(output_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var',
                              'cognitive_inc', 'cognitive_prev', 'cognitive_inc_rate',
                              'cognitive_prev_rate', 'cognitive_YLD'],
                 filename='cognitive', stage='stage_2')

    # Fatigue
    df.save_data(output_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var',
                              'fatigue_inc', 'fatigue_prev', 'fatigue_inc_rate',
                              'fatigue_prev_rate', 'fatigue_YLD'],
                 filename='fatigue', stage='stage_2')

    # Respiratory
    df.save_data(output_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var',
                              'respiratory_inc', 'respiratory_prev', 'respiratory_inc_rate',
                              'respiratory_prev_rate', 'respiratory_YLD'],
                 filename='respiratory', stage='stage_2')

    # Cognitive Fatigue
    df.save_data(output_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var',
                              'cognitive_fatigue_inc', 'cognitive_fatigue_prev', 
                              'cognitive_fatigue_inc_rate', 'cognitive_fatigue_prev_rate', 
                              'cognitive_fatigue_YLD'],
                 filename='cognitive_fatigue', stage='stage_2')

    # Cognitive Respiratory
    df.save_data(output_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var',
                              'cognitive_respiratory_inc', 'cognitive_respiratory_prev', 'cognitive_respiratory_inc_rate',
                              'cognitive_respiratory_prev_rate', 'cognitive_respiratory_YLD'],
                 filename='cognitive_respiratory', stage='stage_2')

    # Fatigue Respiratory
    df.save_data(output_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var',
                              'fatigue_respiratory_inc', 'fatigue_respiratory_prev', 
                              'fatigue_respiratory_inc_rate', 'fatigue_respiratory_prev_rate', 
                              'fatigue_respiratory_YLD'],
                 filename='fatigue_respiratory', stage='stage_2')

    # Cognitive Fatigue Respiratory
    df.save_data(output_cols=['location_id', 'age_group_id', 'sex_id', 'draw_var',
                              'cognitive_fatigue_respiratory_inc', 
                              'cognitive_fatigue_respiratory_prev', 
                              'cognitive_fatigue_respiratory_inc_rate',
                              'cognitive_fatigue_respiratory_prev_rate', 
                              'cognitive_fatigue_respiratory_YLD'],
                 filename='cognitive_fatigue_respiratory', stage='stage_2')

    # endregion ----------------------------------------------------------------


if __name__ == '__main__':
    loc_id = 160
    loc_name='Afghanistan'
    output_version='2021-02-04.01'

    main(loc_id, loc_name, output_version)