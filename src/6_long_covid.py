from src.classes.Dataset import Dataset
import datetime, copy
import pandas as pd


def main(loc_id, loc_name, output_version):
    ## Read in short-term outcomes
    # region -------------------------------------------------------------------
    # Mild/Moderate
    midmod = Dataset(loc_id, loc_name, output_version, 'long_midmod')
    midmod.wide_to_long(stubnames='draw_', i=['location_id', 'age_group_id', 
                                              'sex_id', 'date'],
                        j='draw_var')
    midmod.data = midmod.data.rename(columns={'draw_' : 'midmod_inc'})

    # Hospital
    hospital = Dataset(loc_id, loc_name, output_version, 'long_hospital')
    hospital.data = hospital.data.rename(columns={'variable' : 'msre'})
    hospital.wide_to_long(stubnames='draw_', i=['location_id', 'age_group_id', 
                                                'sex_id', 'date', 'msre'],
                          j='draw_var')
    hospital.long_to_wide(stub='draw_', i=['location_id', 'age_group_id',
                                           'sex_id', 'date', 'draw_var'],
                          j='msre')
    hospital.data = hospital.data.rename(columns={'draw_hsp_admit' : 'hospital_inc',
                                                  'draw_hospital_deaths' : 'hospital_deaths'})

    # Icu
    icu = Dataset(loc_id, loc_name, output_version, 'long_icu')
    icu.data = icu.data.rename(columns={'variable' : 'msre'})
    icu.wide_to_long(stubnames='draw_', i=['location_id', 'age_group_id', 
                                            'sex_id', 'date', 'msre'],
                     j='draw_var')
    icu.long_to_wide(stub='draw_', i=['location_id', 'age_group_id',
                                      'sex_id', 'date', 'draw_var'],
                     j='msre')
    icu.data = icu.data.rename(columns={'draw_icu_admit' : 'icu_inc',
                                        'draw_icu_deaths' : 'icu_deaths'})

    # endregion ----------------------------------------------------------------


    ## Mild/Moderate Incidence & Prevalence
    # region -------------------------------------------------------------------
    # Shift hospitalizations 7 days
    lag_hsp = copy.deepcopy(hospital)
    lag_hsp.data = lag_hsp.data.drop(columns=['hospital_deaths'])
    lag_hsp.data.date = lag_hsp.data.date + pd.to_timedelta(1, unit='D')


    # Merge midmod and lag_hsp
    midmod.data = pd.merge(midmod.data, lag_hsp.data, how='left',
                           on=['location_id', 'age_group_id', 'sex_id', 
                               'draw_var', 'date'])
    

    # mild/moderate at risk number = (mild/moderate incidence - hospital admissions|7 days later) |
    #                                 shift forward by {incubation period + mild/moderate duration|no hospital}
    midmod.data['midmod_risk_num'] = midmod.data.midmod_inc - midmod.data.hospital_inc
    midmod.data.date = midmod.data.date + pd.to_timedelta(2, unit='D')


    # mild/moderate long-term incidence = mild/moderate number at risk * proportion of mild/moderate with each long-term symptom cluster
    midmod.data['midmod_cognitive_inc'] = midmod.data.midmod_risk_num * 0.5
    midmod.data['midmod_fatigue_inc'] = midmod.data.midmod_risk_num * 0.5
    midmod.data['midmod_respiratory_inc'] = midmod.data.midmod_risk_num * 0.5


    # mild/moderate long-term prevalence = mild/moderate long-term incidence * [duration]
    midmod.data['midmod_cognitive_prev'] = midmod.data.midmod_cognitive_inc * 0.166
    midmod.data['midmod_fatigue_prev'] = midmod.data.midmod_fatigue_inc * 0.166
    midmod.data['midmod_respiratory_prev'] = midmod.data.midmod_respiratory_inc * 0.166


    # Drop unneeded cols
    midmod.data = midmod.data.drop(columns=['midmod_inc', 'hospital_inc', 'midmod_risk_num'])

    # endregion ----------------------------------------------------------------


    ## Severe Incidence & Prevalence
    # region -------------------------------------------------------------------

    # Shift icu admissions
    lag_icu = copy.deepcopy(icu)
    lag_icu.data = lag_icu.data.drop(columns=['icu_deaths'])
    lag_icu.data.date = lag_icu.data.date + pd.to_timedelta(1, unit='D')


    # Shift hospital deaths
    lag_hsp = copy.deepcopy(hospital)
    lag_hsp.data = lag_hsp.data.drop(columns=['hospital_inc'])
    lag_hsp.data.date = lag_hsp.data.date + pd.to_timedelta(1, unit='D')


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
    hospital.data.date = hospital.data.date + pd.to_timedelta(1, unit='D')


    # severe long-term incidence = severe number at risk * proportion of severe with each long-term symptom cluster
    hospital.data['hospital_cognitive_inc'] = hospital.data.hospital_risk_num * 0.5
    hospital.data['hospital_fatigue_inc'] = hospital.data.hospital_risk_num * 0.5
    hospital.data['hospital_respiratory_inc'] = hospital.data.hospital_risk_num * 0.5


    # severe long-term prevalence = severe long-term incidence * [duration]
    hospital.data['hospital_cognitive_prev'] = hospital.data.hospital_cognitive_inc * 0.166
    hospital.data['hospital_fatigue_prev'] = hospital.data.hospital_fatigue_inc * 0.166
    hospital.data['hospital_respiratory_prev'] = hospital.data.hospital_respiratory_inc * 0.166


    # Remove unneeded cols
    hospital.data = hospital.data.drop(columns=['hospital_inc', 'icu_inc', 'hospital_deaths', 
                                                'hospital_risk_num'])

    # endregion ----------------------------------------------------------------


    ## Critical Incidence & Prevalence
    # region -------------------------------------------------------------------

    # Shift icu deaths
    lag_icu = copy.deepcopy(icu)
    lag_icu.data = lag_icu.data.drop(columns='icu_inc')
    lag_icu.data.date = lag_icu.data.date + pd.to_timedelta(1, unit='D')


    # Merge icu and lag_icu
    icu.data = pd.merge(icu.data.drop(columns='icu_deaths'),
                        lag_icu.data, how='left', on=['location_id', 'age_group_id', 
                                                      'sex_id', 'draw_var', 'date'])
    del lag_icu


    # critical at risk number = (ICU admissions - ICU deaths|3 days later) |
    #                            shift forward by {ICU duration if no death + ICU mild moderate duration after discharge}
    icu.data['icu_risk_num'] = icu.data.icu_inc - icu.data.icu_deaths
    icu.data.date = icu.data.date - pd.to_timedelta(1, unit='D')


    # critical long-term incidence = critical number at risk * proportion of critical with each long-term symptom cluster
    icu.data['icu_cognitive_inc'] = icu.data.icu_risk_num * 0.5
    icu.data['icu_fatigue_inc'] = icu.data.icu_risk_num * 0.5
    icu.data['icu_respiratory_inc'] = icu.data.icu_risk_num * 0.5


    # critical long-term prevalence = critical long-term incidence * [duration]
    icu.data['icu_cognitive_prev'] = icu.data.icu_cognitive_inc * 0.166
    icu.data['icu_fatigue_prev'] = icu.data.icu_fatigue_inc * 0.166
    icu.data['icu_respiratory_prev'] = icu.data.icu_respiratory_inc * 0.166


    # Remove unneeded cols
    icu.data = icu.data.drop(columns=['icu_inc', 'icu_deaths', 'icu_risk_num'])

    # endregion ----------------------------------------------------------------


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
    df.data['cognitive_inc'] = (df.data.midmod_cognitive_inc +
                                df.data.hospital_cognitive_inc +
                                df.data.icu_cognitive_inc)
    df.data['fatigue_inc'] = (df.data.midmod_fatigue_inc +
                              df.data.hospital_fatigue_inc +
                              df.data.icu_fatigue_inc)
    df.data['respiratory_inc'] = (df.data.midmod_respiratory_inc +
                              df.data.hospital_respiratory_inc +
                              df.data.icu_respiratory_inc)
    df.data = df.data.drop(columns=['midmod_cognitive_inc', 'hospital_cognitive_inc', 
                                    'icu_cognitive_inc', 'midmod_fatigue_inc', 
                                    'hospital_fatigue_inc', 'icu_fatigue_inc',
                                    'midmod_respiratory_inc', 'hospital_respiratory_inc', 
                                    'icu_respiratory_inc'])


    # Prevalence
    df.data['cognitive_prev'] = (df.data.midmod_cognitive_prev +
                                df.data.hospital_cognitive_prev +
                                df.data.icu_cognitive_prev)
    df.data['fatigue_prev'] = (df.data.midmod_fatigue_prev +
                              df.data.hospital_fatigue_prev +
                              df.data.icu_fatigue_prev)
    df.data['respiratory_prev'] = (df.data.midmod_respiratory_prev +
                              df.data.hospital_respiratory_prev +
                              df.data.icu_respiratory_prev)
    df.data = df.data.drop(columns=['midmod_cognitive_prev', 'hospital_cognitive_prev', 
                                    'icu_cognitive_prev', 'midmod_fatigue_prev', 
                                    'hospital_fatigue_prev', 'icu_fatigue_prev',
                                    'midmod_respiratory_prev', 'hospital_respiratory_prev', 
                                    'icu_respiratory_prev'])

    # endregion ----------------------------------------------------------------


    ## Aggregate by year
    # region -------------------------------------------------------------------

    # Subset to 2020
    df.data = df.data[(df.data.date >= datetime.datetime(2020, 1, 1)) &
                       (df.data.date <= datetime.datetime(2020, 12, 31))]


    # Sum by day
    df.collapse(agg_function='sum', group_cols=['location_id', 'age_group_id', 
                                                'sex_id', 'draw_var'],
                calc_cols=['cognitive_inc', 'cognitive_prev', 'fatigue_inc', 
                           'fatigue_prev', 'respiratory_inc', 'respiratory_prev'])


    # Divide prevalence by 366
    df.data.cognitive_prev = df.data.cognitive_prev / 366
    df.data.fatigue_prev = df.data.fatigue_prev / 366
    df.data.respiratory_prev = df.data.respiratory_prev / 366


    # Ensure incidence and prevalence aren't negative
    df.check_neg(calc_cols=['cognitive_inc', 'cognitive_prev', 'fatigue_inc', 'fatigue_prev',
                            'respiratory_inc', 'respiratory_prev'])

    # endregion ----------------------------------------------------------------


    ## Calculate rates
    # region -------------------------------------------------------------------

    # Pull population
    pop = pd.DataFrame({'location_id' : [35507,35507],
                        'age_group_id' : [22,22],
                        'sex_id' : [1,2],
                        'population' : [1000000, 1000001]})

    
    # Merge population
    df.data = pd.merge(df.data, pop, how='left',
                       on=['location_id', 'age_group_id', 'sex_id'])


    # Calculate rates
    df.data['cognitive_inc_rate'] = df.data.cognitive_inc / df.data.population
    df.data['fatigue_inc_rate'] = df.data.fatigue_inc / df.data.population
    df.data['respiratory_inc_rate'] = df.data.respiratory_inc / df.data.population
    df.data['cognitive_prev_rate'] = df.data.cognitive_prev / df.data.population
    df.data['fatigue_prev_rate'] = df.data.fatigue_prev / df.data.population
    df.data['respiratory_prev_rate'] = df.data.respiratory_prev / df.data.population
    # endregion ----------------------------------------------------------------


    ## Calculate YLDs
    # region -------------------------------------------------------------------

    # Read in disability weights


    # Temporary values
    df.data['cognitive_YLD'] = df.data.cognitive_prev_rate * 0.01
    df.data['fatigue_YLD'] = df.data.fatigue_prev_rate * 0.01
    df.data['respiratory_YLD'] = df.data.respiratory_prev_rate * 0.01
    print(df.data)

    # endregion ----------------------------------------------------------------


    ## Save datasets & run diagnostics
    # region -------------------------------------------------------------------

    # Cognitive
    df.save_data()

    # Fatigue
    df.save_data()

    # Respiratory
    df.save_data()

    # endregion ----------------------------------------------------------------

if __name__ == '__main__':
    loc_id = 160
    loc_name='Abruzzo'
    output_version='2021-01-31.01'

    main(loc_id, loc_name, output_version)

