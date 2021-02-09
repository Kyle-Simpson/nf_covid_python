import pandas as pd
import numpy as np
import datetime, copy, os
from nf_covid.utils.utils import get_core_ref, roots

class Dataset():
    def __init__(self, loc_id, loc_name, output_version, dst_type, nf_type):

        def init_data(self):
            ''' Collect input data '''
            if self.nf_type == 'short' :
                if self.dataset_type in ['infections', 'deaths']:
                    df = pd.read_csv('{}daily_{}.csv'.format(roots['infect_death_input_path'], 
                                                             self.dataset_type))
                elif self.dataset_type in ['hospital_admit', 'icu_admit']:
                    df = pd.read_csv('{}{}_{}/{}.csv'.format(roots['hsp_icu_input_path'], 
                                                             self.loc_name, self.loc_id, 
                                                             self.dataset_type))
            else :
                df = pd.read_csv('{}{}/stage_1/_for_long_covid/{}_{}_{}.csv'.format(get_core_ref('data_output', 'stage_1'), 
                                                                                    self.output_version,
                                                                                    self.loc_name, self.loc_id, 
                                                                                    self.dataset_type))
                
            # Make dates
            df.date = [datetime.date(int(d.split('-')[0]),
                                     int(d.split('-')[1]),
                                     int(d.split('-')[2])) 
                       for d in df.date]
            df.date = pd.to_datetime(df.date)
            
            # Reshape long if nf_type is long_covid
            if self.nf_type == 'long':
                if self.dataset_type == 'midmod':
                    df = df.melt(id_vars=['location_id', 'age_group_id', 
                                          'sex_id', 'date'])
                    df = df.rename(columns={'variable' : 'draw_var', 
                                            'value' : 'midmod_inc'})
                elif self.dataset_type == 'hsp_admit':
                    df = df.rename(columns={'variable' : 'msre'})
                    df = df.melt(id_vars=['location_id', 'age_group_id', 
                                           'sex_id', 'date', 'msre'])
                    df = df.set_index(['location_id', 'age_group_id', 'sex_id', 
                                       'date', 'variable', 'msre']).unstack(level=-1)
                    df = df.reset_index()
                    df['hospital_inc'] = df.value.hospital_inc
                    df['hospital_deaths'] = df.value.hospital_deaths
                    df = df.drop(columns='value')
                    df = df.droplevel('msre', axis=1)
                    df = df.rename(columns={'variable' : 'draw_var'})
                else:
                    df = df.rename(columns={'variable' : 'msre'})
                    df = df.melt(id_vars=['location_id', 'age_group_id', 
                                           'sex_id', 'date', 'msre'])
                    df = df.set_index(['location_id', 'age_group_id', 'sex_id', 
                                       'date', 'variable', 'msre']).unstack(level=-1)
                    df = df.reset_index()
                    df['icu_inc'] = df.value.icu_inc
                    df['icu_deaths'] = df.value.icu_deaths
                    df = df.drop(columns='value')
                    df = df.droplevel('msre', axis=1)
                    df = df.rename(columns={'variable' : 'draw_var'})
            
            return(df.reset_index(drop=True))


        self.loc_id = int(loc_id)
        self.loc_name = str(loc_name)
        self.output_version = str(output_version)
        self.dataset_type = str(dst_type)
        self.nf_type = str(nf_type)

        self.data = init_data(self)


    def collapse(self, agg_function='sum', group_cols=None, calc_cols=None):
        ''' Convenience function for STATA-like collapsing. Like STATA, 
            removes any columns not specified in either group_cols or 
            calc_cols. '''
        
        # Get columns and ensure proper var types
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        # Get all columns other than group_cols if no calc_cols given
        if not calc_cols:
            calc_cols = [c for c in list(self.data) if c not in group_cols]
        if isinstance(calc_cols, str):
            calc_cols = [calc_cols]

        # Remove columns that are not included in group or aggregation calculation
        # (mimics STATA behavior)
        df = self.data[group_cols + calc_cols]
        g = df.groupby(group_cols)

        # Make the calculation
        if agg_function == 'sum':
            g = g[calc_cols].agg(np.sum)
        elif agg_function == 'mean':
            g = g[calc_cols].agg(np.mean)
        elif agg_function == 'min':
            g = g[calc_cols].agg(np.min)
        elif agg_function == 'max':
            g = g[calc_cols].agg(np.max)

        self.data = g.reset_index()


    def check_neg(self, calc_cols, check_cols=None, add_cols=None):
        ''' Check for negative values in specified columns '''
        orig = copy.deepcopy(self.data)

        if 'location_id' not in self.data.columns:
            self.data['location_id'] = self.loc_id
        if 'location_name' not in self.data.columns:
            self.data['location_name'] = self.loc_name
        if check_cols is None:
            check_cols = calc_cols
        if add_cols is not None:
            for col in add_cols.keys():
                self.data[col] = add_cols[col]
        
        # Take mean of calc_cols by location_id, location_name, age_group_id, sex_id
        self.collapse(agg_function='mean', group_cols=['location_id', 'location_name', 
                                                       'age_group_id', 'sex_id'],
                      calc_cols=calc_cols)

        for col in check_cols:
            if min(self.data[col]) < 0:
                issues = self.data[(self.data[col] < 0)]
                issues.to_csv('{}{}/nf_covid_{}/errors/{}_cov_{}_{}_errors.csv'.format(roots['jobmon_logs_base'], self.output_version.split('.')[0], 
                                                                                      self.output_version, self.nf_type, self.loc_id, col),
                             )
                raise ValueError('Negative values in {}'.format(col))

        self.data = orig
        
        
    def save_data(self, output_cols, filename, stage):
        ''' Save out dataset and run diagnostics '''

        df = self.data[output_cols]
        

        # Check for squareness
        
        
        # Pull output filepath
        out_loc = '{}{}/{}/{}_{}/'.format(get_core_ref('data_output', stage), 
                                          self.output_version, stage, self.loc_name, 
                                          self.loc_id)
        
        
        # Ensure output filepath exists
        os.makedirs(out_loc, exist_ok=True)
        os.makedirs('{}diagnostics/'.format(out_loc), exist_ok=True)
        
        
        # Output csv
        df.to_csv('{}{}.csv'.format(out_loc, filename), index=False)
        
        
        
        
        
        
        
        
        
        
        
        
        