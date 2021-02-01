import pandas as pd
import numpy as np
import datetime, copy

class Dataset():
    def __init__(self, loc_id, loc_name, output_version, dst_type):

        def init_data(dst_type, loc_id):
            ''' Collect input data '''
            if dst_type in ['infections', 'deaths']:
                df = pd.read_csv('/Users/Kyle/Desktop/data/{}.csv'.format(dst_type))
                df = df[(df.location_id == loc_id)]
            elif dst_type in ['hospital', 'icu']:
                df = pd.read_csv('/Users/Kyle/Desktop/data/{}_admit.csv'.format(dst_type))
            elif dst_type in ['long_midmod', 'long_hospital', 'long_icu']:
                df = pd.read_csv('/Users/Kyle/Desktop/data/{}.csv'.format(dst_type))
            
            # Make dates
            df.date = [datetime.date(int(d.split('/')[2]),
                                     int(d.split('/')[0]),
                                     int(d.split('/')[1])) 
                       for d in df.date]
            df.date = pd.to_datetime(df.date)
            return(df.reset_index(drop=True))


        self.loc_id = int(loc_id)
        self.loc_name = str(loc_name)
        self.output_version = str(output_version)
        self.dataset_type = str(dst_type)

        self.data = init_data(self.dataset_type, self.loc_id)


    def collapse(self, agg_function='sum', group_cols=None, calc_cols=None):
        ''' Convenience function for STATA-like collapsing. Like STATA, 
            removes any columns not specified in either group_cols or 
            calc_cols. '''
        
        # Get columns and ensure proper var types
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        # Get all columns other than group_cols if no calc_cols given
        if not calc_cols:
            calc_cols = [c for c in list(df) if c not in group_cols]
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


    def wide_to_long(self, stubnames, i, j, drop_others=False):
        ''' A convenience function to reshape a DataFrame wide to long. '''
        df = self.data
        
        def get_varnames(df, stub):
            return(df.filter(regex=stub).columns.tolist())

        def melt_stub(df, stub, newVarNm):
            varnames = get_varnames(df, stub)
            # Use all cols as ids
            ids = [c for c in df.columns.values if c not in varnames]
            newdf = pd.melt(df, id_vars=ids, value_vars=varnames,
                            value_name=stub, var_name=newVarNm)
            # remove 'stub' from observations in 'newVarNm' columns, then
            #   recast to int typeif numeric suffixes were used new
            try:
                if newdf[newVarNm].unique().str.isdigit().all():
                    newdf[newVarNm] = newdf[newVarNm].str.replace(
                        stub, '').astype(int)
            except AttributeError:
                newdf[newVarNm] = newdf[newVarNm].str.replace(stub, '').astype(str)
            return newdf

        # Error handling
        if isinstance(i, str):
            i = [i]
        if isinstance(stubnames, str):
            stubnames = [stubnames]
        if isinstance(j, str):
            j = [j]
        if len(j) != len(stubnames):
            raise ValueError("Stub list must be same length as j list.")
        if any(map(lambda s: s in list(df), stubnames)):
            raise ValueError("Stubname can't be identical to a column name.")
        if df[i].duplicated().any():
            raise ValueError("The id variables don't uniquely identify each row.")

        # Start the reshaping (pop stub in prep for rewriting for multiple stubs)
        stubcols = []
        for s in stubnames:
            stubcols +=  get_varnames(df, s)
        non_stubs = [c for c in df.columns if c not in stubcols+i]
        for pos, stub in enumerate(stubnames):
            jval = j[pos]
            temp_df = df.copy()
            # Drop extra columns if requested
            if drop_others:
                temp_df = temp_df[i + get_varnames(df, stub)]
            else:
                temp_df = temp_df[i + get_varnames(df, stub) + non_stubs]
            # add melted data to output dataframe 
            if pos == 0:
                newdf = melt_stub(temp_df, stub, jval)
            else:
                newdf = newdf.merge(melt_stub(temp_df, stub, jval))

        self.data = newdf.reset_index(drop=True)


    def long_to_wide(self, stub, i, j, drop_others=False):
        ''' Convenience function to reshape DataFrame long to wide. '''
        df = self.data
        if isinstance(i, str):
            i = [i]
        # Error Checking
        if df[i + [j]].duplicated().any():
            raise ValueError("`i` and `j` don't uniquely identify each row.")
        if df[j].isnull().any():
            raise ValueError("`j` column has missing values. cannot reshape.")
        if df[j].astype(str).str.isnumeric().all():
            if df.loc[df[j] != df[j].astype(int), j].any():
                print(
                    "Decimal values cannot be used in reshape suffix. {} coerced to integer".format(j))
            df.loc[:, j] = df[j].astype(int)
        else:
            df.loc[:, j] = df[j].astype(str)
        # Perform reshape
        if drop_others:
            df = df[i + [j]]
        else:
            i = [x for x in list(df) if x not in [stub, j]]
        df = df.set_index(i + [j]).unstack(fill_value=np.nan)
        # Ensure all stubs and suffixes are strings and join them to make col names
        cols = pd.Index(df.columns)
        # for each s in each col in cols, e.g. cols = [(s, s1), (s, s2)]
        cols = map(lambda col: map(lambda s: str(s), col), cols)
        cols = [''.join(c) for c in cols]
        # Set columns to the stub+suffix name and remove MultiIndex
        df.columns = cols
        df = df.reset_index()
        self.data = df


    def check_neg(self, calc_cols, check_cols=None, add_cols=None):
        ''' Check for negative values in specified columns '''
        orig = copy.deepcopy(self.data)

        if 'location_id' not in self.data.columns:
            self.data['location_id'] = self.loc_id
        if 'location_name' not in self.data.columns:
            self.data['location_name'] = self.loc_name
        if check_cols is None:
            check_cols = calc_cols
        # if add_cols is not None:
        #     df = [df[col] = add_cols[col] for col in add_cols.columns]
        
        # Take mean of calc_cols by location_id, location_name, age_group_id, sex_id
        self.collapse(agg_function='mean', group_cols=['location_id', 'location_name', 
                                                       'age_group_id', 'sex_id'],
                      calc_cols=calc_cols)

        for col in check_cols:
            if min(self.data[col]) < 0:
                self.data[col] = 0
                issues = self.data[(self.data[col] < 0)]
                # raise ValueError('Negative values in {}'.format(col))
            else:
                print('No negatives in {}'.format(col))

        self.data = orig