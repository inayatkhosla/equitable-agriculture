"""
plotters.py:
    Reads, processes and plots data availability, current market conditions, 
    and market trends

    DATA AVAILABILITY
    DataAvailabilityProcessor (cls): Processes prices and arrival data for plotting
    DataAvailabilityPlotter (cls): Plots price or arrival data availability
    DataAvailability (cls): Wrapper - Pulls, processes, and plots data availability

    CURRENT MARKETS
    CurrentMarketProcessor (cls): Processes current market prices and arrivals for plotting
    CurrentMarketPlotter (cls): Plots current market conditions
    CurrentMarkets (cls): Wrapper - Pulls, processes, and plots current market conditions

    MARKET TRENDS
    TrendProcessor (cls): Processes market price and arrival trends for plotting
    TrendPlotter (cls): Plots market price and arrival trends
    Trends (cls): Wrapper - Pulls, processes, and plots market trends
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import plotly.graph_objs as go
import plotly.figure_factory as ff
from plotly import tools
import cufflinks as cf
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

import lib.db_puller as db


## --------------------------
## Data Availability
## --------------------------

class DataAvailabilityProcessor(object):
    """
    Processes prices and arrival data for plotting

    Args:
        df (df): Dataframe to be processed
        col (str): 'state' or 'district' - level of availability
        lm (df): state > district > market mappings
    """
    def __init__(self, df, col, lm):
        self.df = df
        self.col = col
        self.lm = lm
    
    
    def restructure(self, f, i):
        # Improve readability
        fss = f[f[self.col]==i]
        if len(fss) > 1:
            fss['next_date'] = fss['date'].shift(-1)
            fss['data_gap'] = (fss['next_date'] - fss['date']).dt.days 
            ep = fss[fss['data_gap'] > 1]
            ep['start_point'] = ep['next_date'].shift(1)
            ep = ep[[self.col,'start_point','date']].copy()
            lcr = self.extract_last_record(fss, ep)
            epf = pd.concat([ep, lcr])
            epf.columns = ['Task', 'Start', 'Finish']
            epf['Resource'] = 'Available'
            epf.dropna(inplace=True)
            return epf
    
    
    def extract_last_record(self, fss, ep):
        lcs = fss[fss['date'] >  ep['date'].max()]
        lcr = lcs[:1]
        lcr['next_date'] = lcs['date'].max()
        lcr.drop('data_gap', axis=1, inplace=True)
        lcr.columns = [self.col, 'start_point', 'date']
        return lcr
    
    
    def prep_data(self):
        processed_dfs = []
        f = self.df[[self.col,'date']].drop_duplicates().sort_values([self.col,'date'])
        for i in f[self.col].unique():
            epf = self.restructure(f, i)
            processed_dfs.append(epf)
        processed = pd.concat(processed_dfs, sort=True)
        processed.reset_index(drop=True,inplace=True)
        if self.col != 'state':
            smaps = self.lm[['state', 'district']].drop_duplicates().set_index('district')['state'].to_dict()
            processed['state'] = processed['Task'].map(smaps)
        return processed


class DataAvailabilityPlotter(DataAvailabilityProcessor):
    """
    Plots price or arrival data availability

    Args:
        datatype (str): 'prices' or 'arrivals'
        df (df): Dataframe to be processed
        col (str): 'state' or 'district' - level of availability
        lm (df): state > district > market mappings
        state (str): [Optional] state to plot district data availability for
    """
    def __init__(self, datatype, df, lm, col, state=None):
        self.datatype = datatype
        self.df = df
        self.col = col
        self.lm = lm
        self.state = state
        
        
    def process_data(self):
        processed = self.prep_data()
        if self.state:
            processed = processed[processed['state'] == self.state]
            processed.reset_index(drop=True,inplace=True)
        self.processed = processed

           
    def plotter(self):
        colors = {'Available': '#191970'}
        title='Data Availability: {}'.format(self.datatype)
        if self.state:
            title = title + ' - {}'.format(self.state)
        
        fig = ff.create_gantt(self.processed, 
                              title=title, 
                              colors=colors, 
                              index_col='Resource', group_tasks=True, 
                              showgrid_x=True, showgrid_y=True)
        
        left_margin = (self.processed['Task'].str.len().max())*8
        start = str(pd.to_datetime(self.processed['Start'].min()).date())
        end = str(pd.to_datetime(self.processed['Finish'].max()).date())
        rangeselector=dict(
            buttons=list([
                    dict(count=1,
                         label='1m',
                         step='month',
                         stepmode='backward'),
                    dict(count=3,
                         label='3m',
                         step='month',
                         stepmode='backward'),
                    dict(count=6,
                         label='6m',
                         step='month',
                         stepmode='backward'),
                    dict(step='all')
                    ]))
        xaxis = dict(autorange=False, range=[start, end], rangeselector=rangeselector)
        fig['layout'].update(margin=go.Margin(l=left_margin), xaxis=xaxis)
        iplot(fig)
        
    
    def plot(self):
        self.process_data()
        self.plotter()


class DataAvailability(object):
    """
    Wrapper - Pulls, processes, and plots data availability. plot() takes datatype, 
    region level, and state arguments

    Args:
        commodity (str): Commodity to see availability of
        start (str): Start date of availability evaluation period; defaults to Oct 2015
        end (str): End date of availability evaluation period; defaults to today

    Usage:
        da = DataAvailability()
        da = DataAvailability(start='2017-10-01', end='2018-03-31')
        da.plot('Prices', 'state')
        da.plot('Arrivals', 'state')
        da.plot('Prices', 'district', 'Haryana')
        da.plot('Arrivals', 'district', 'Himachal Pradesh')
    """
    def __init__(self, commodity='Kinnow', start='2015-10-01', end=None):
        self.commodity = commodity
        self.start = start
        self.end = end
        if not self.end:
            self.end = str(pd.to_datetime('today').date())
        self.get_data()
                    
        
    def get_data(self):
        d = db.DBPuller(self.commodity, self.start, self.end)
        d.get_data()
        self.prices, self.arrivals, self.lm = d.prices, d.arrivals, d.lm
        
    
    def plot(self, datatype, col, state=None):
        df = self.prices if datatype == 'Prices' else self.arrivals
        dap = DataAvailabilityPlotter(datatype, df, self.lm, col, state)
        dap.plot()


## --------------------------
## Current Markets
## --------------------------

class CurrentMarketProcessor(object):
    """
    Processes current market prices and arrivals for plotting

    Args:
        prices (df): Prices dataframe to be processed
        arrivals (df): Arrivals dataframe to be processed
        qcutoff (int): Minimum arrival tonnage for market inclusion
        tcutoff (int): Recency cutoff in days
        period (int): Rolling average window in days
    """
    def __init__(self, prices, arrivals, qcutoff, tcutoff, period):
        self.prices = prices
        self.arrivals = arrivals
        self.qcutoff = qcutoff
        self.tcutoff = tcutoff
        self.period = period
        

    def update_dtypes(self):
        self.prices['date'] = pd.to_datetime(self.prices['date'])
        self.arrivals['date'] = pd.to_datetime(self.arrivals['date'])
        

    def limit_to_recent(self):
        cutoff = (pd.to_datetime('today') - pd.Timedelta(days=self.tcutoff))
        self.recent_p = self.prices[self.prices['date'] > cutoff]
        self.recent_a = self.arrivals[self.arrivals['date'] > cutoff]
    

    def filter_small_markets(self):
        grouped = self.recent_a.groupby('market')
        self.recent_a = grouped.filter(lambda x: x['quantity'].sum() > self.qcutoff)
        large_markets = list(self.recent_a['market'].unique())
        self.recent_p = self.recent_p[self.recent_p['market'].isin(large_markets)]
    

    def get_rolling_means(self):
        rp, ra = self.recent_p, self.recent_a
        
        grp_p = ['state','district','market','grade']
        grp_a = ['state','district','market']
        rp = rp.sort_values(grp_p +['date'])
        ra = ra.sort_values(grp_a +['date'])

        rp['price_range'] = rp['max_price'] - rp['min_price']
        rp['r_modal_price'] = rp.groupby(grp_p)['modal_price'].apply(lambda x:x.rolling(self.period, min_periods=1).mean())
        rp['r_price_range'] = rp.groupby(grp_p)['price_range'].apply(lambda x:x.rolling(self.period, min_periods=1).mean())
        ra['r_quantity'] = ra.groupby(grp_a)['quantity'].apply(lambda x:x.rolling(self.period, min_periods=1).mean())
        
        rp[['r_modal_price', 'r_price_range']] = rp[['r_modal_price', 'r_price_range']].round()
        ra['r_quantity'] = ra['r_quantity'].round(1)
        self.rolling_p, self.rolling_a = rp, ra
        

    def get_latest_numbers(self):
        lp = self.rolling_p.loc[self.rolling_p.groupby(['state','district','market','grade'])['date'].idxmax()]
        la = self.rolling_a.loc[self.rolling_a.groupby(['state','district','market'])['date'].idxmax()]
        self.latest_p, self.latest_a = lp, la
        

    def merge_pq(self, lp, la, grade):
        lpg = lp[lp['grade'] == grade]
        lm = lpg.merge(la[['market','quantity','r_quantity']], on='market')
        lm['r_quantity_l'] = np.log(lm['r_quantity'])
        lm['r_quantity_l'][lm['r_quantity_l'] < 0] = 0
        lm['r_quantity_l'] = lm['r_quantity_l'].round(2)
        return lm

    def prep_data(self):
        self.update_dtypes()
        self.limit_to_recent()
        self.filter_small_markets()
        self.get_rolling_means()
        self.get_latest_numbers()
        return self.latest_p, self.latest_a


class CurrentMarketPlotter(CurrentMarketProcessor):
    """
    Plots current market conditions

    Args:
        commodity (str): Commodity to plot conditions for
        prices (df): Prices dataframe to be processed
        arrivals (df): Arrivals dataframe to be processed
        qcutoff (int): Minimum arrival tonnage for market inclusion
        tcutoff (int): Recency cutoff in days
        period (int): Rolling average window in days
    """
    def __init__(self, commodity, prices, arrivals, qcutoff, tcutoff, period):
        self.commodity = commodity
        self.prices = prices
        self.arrivals = arrivals
        self.qcutoff = qcutoff
        self.tcutoff = tcutoff
        self.period = period
        self.process_data()
       
        
    def process_data(self):
        self.latest_p, self.latest_a = self.prep_data()
        
    
    def plot_mkt_overview(self, grade='Medium'):
        lp, la = self.latest_p, self.latest_a
        lp = lp[lp['grade'] == grade]
        lp = lp.sort_values('r_modal_price', ascending=False)
        bottom_margin = (lp['market'].str.len().max())*4
        
        trace1 = go.Bar(
            x=lp['market'],
            y=lp['r_modal_price'],
            name='Modal Price',
            opacity=0.7
            )

        trace2 = go.Bar(
            x=la['market'],
            y=la['r_quantity'],
            name='Arrivals',
            yaxis='y2',
            opacity=0.7
            )
        
        data = [trace1, trace2]

        layout = go.Layout(
            title='{} <br> Latest Market Conditions <br> {} Day Averages: Grade - {}'.
                    format(self.commodity,self.period, grade),
            titlefont=dict(
                size=16
            ),
            yaxis=dict(
                title='Prices per Quintal'
            ),
            yaxis2=dict(
                title='Arrivals in Tonnes',
                overlaying='y',
                side='right',
                showgrid=False
            ),
            xaxis=dict(
                tickangle=90
            )
        )

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(margin=go.Margin(b=bottom_margin))
        iplot(fig)
        
        
    def generate_hover_text(self, df):
        hover_text = []
        for index, row in df.iterrows():
            hover_text.append(('Market: {}<br>' +
                               'Modal Price: {}<br>' +
                               'Quantity: {}<br>' +
                               'Price Range: {}').format(row['market'], row['r_modal_price'],
                                                     row['r_quantity'], row['r_price_range']))
        df['text'] = hover_text
        return df
        
    
    
    def plot_mkt_overview_alt(self, grade='Medium'):
        lm = self.merge_pq(self.latest_p, self.latest_a, grade)
        lm = self.generate_hover_text(lm)
        lm.iplot(kind='bubble', x='r_quantity', y='r_modal_price', size='r_quantity',
          text='text', categories = 'state', 
          colors=['#071e3d','#1f4287','#278ea5','#a7d129'],
          xTitle='Quantity', yTitle='Modal Price', 
                 title='{} <br> Prices vs. Quantities <br> {} Day Averages: Grade - {}'.
                    format(self.commodity,self.period, grade))
    
    
    
    def plot_price_variation(self, grade='Medium'):
        lm = self.merge_pq(self.latest_p, self.latest_a, grade)
        lm = self.generate_hover_text(lm)
        lm.iplot(kind='bubble', x='r_quantity_l', y='r_modal_price', size='r_price_range',
          text='text', categories = 'state', 
          colors=['#071e3d','#1f4287','#278ea5','#a7d129'],
          xTitle='Arrivals <br> (Log Scale)', yTitle='Modal Price', 
                  title='{} <br> Price Variations within Markets <br> {} Day Averages: Grade - {}'.
                    format(self.commodity,self.period, grade))



class CurrentMarkets(object):
    """
    Wrapper - Pulls, processes, and plots current market conditions for a 
    given commodity. plot() takes plottype, grade, tonnage, recency,
    and rolling agv window size arguments

    Args:
        commodity (str): Commodity to see availability of
        start (str): Start date of availability evaluation period
        end (str): End date of availability evaluation period; defaults to today

    Plot Options:
        overview: Bar - Prices and Arrivals by Market
        overview_alt: Bubble - Prices and Arrivals by Market
        price_var: Bubble - Prices, Arrivals, and Price Variation by Market

    Usage:
        cm = CurrentMarkets()
        cm.plot('overview')
        cm.plot('overview_alt')
        cm.plot('price_var')
        cm.plot('price_var','Large')
    """
    def __init__(self, commodity='Kinnow', start=None, end=None):
        self.commodity = commodity
        self.start = start
        self.end = end
        if not self.start:
            self.start = str(pd.to_datetime('today') - pd.Timedelta(days=92))
        if not self.end:
            self.end = str(pd.to_datetime('today').date())
        self.get_data()
                    
        
    def get_data(self):
        d = db.DBPuller(self.commodity, self.start, self.end)
        d.get_data()
        self.prices, self.arrivals, self.lm = d.prices, d.arrivals, d.lm
        
        
    def plot(self, plottype, grade='Medium', qcutoff=3 , tcutoff=7, period=3):
        cmp = CurrentMarketPlotter(self.commodity, self.prices, self.arrivals, qcutoff, tcutoff, period)
        if plottype == 'overview':
            cmp.plot_mkt_overview(grade)
        elif plottype == 'overview_alt':
            cmp.plot_mkt_overview_alt(grade)
        elif plottype == 'price_var':
            cmp.plot_price_variation(grade)


## --------------------------
## Market Trends
## --------------------------

class TrendProcessor(object):
    """
    Processes market price and arrival trends for plotting

    Args:
        prices (df): Prices dataframe to be processed
        arrivals (df): Arrivals dataframe to be processed
        state (str): State to plot trends for
        market (str): Market to plot trends for
        grade (str): Grade to plot trends for
    """
    def __init__(self, prices, arrivals, state, market, grade):
        self.prices = prices
        self.arrivals = arrivals
        self.state = state
        self.market = market
        self.grade = grade
        
    
    def process_states(self, prices, arrivals):
        prices = prices[prices['state'] == self.state]
        arrivals = arrivals[arrivals['state'] == self.state]
        p_aggs = prices[['date','min_price','modal_price','max_price']].groupby('date').median().reset_index()
        a_aggs = arrivals[['date','quantity']].groupby('date').sum().reset_index()
        return p_aggs, a_aggs
    
    
    def prep_data(self):
        pg = self.prices[self.prices['grade'] == self.grade]
        if self.market:
            p = pg[pg['market'] == self.market]
            a = self.arrivals[self.arrivals['market'] == self.market]
        elif self.state == 'Combined':
            p = pg[['date','min_price','modal_price','max_price']].groupby('date').median().reset_index()
            a = self.arrivals[['date','quantity']].groupby('date').sum().reset_index()
        else:
            p, a = self.process_states(pg, self.arrivals)
        return p, a


class TrendPlotter(TrendProcessor):
    """
    Plots market price and arrival trends

    Args:
        commodity (str): Commodity to plot trends for
        prices (df): Prices dataframe to be processed
        arrivals (df): Arrivals dataframe to be processed
        state (str): State to plot trends for
        market (str): Market to plot trends for
        grade (str): Grade to plot trends for
    """
    def __init__(self, commodity, prices, arrivals, state='Combined', market=None, grade='Medium'):
        self.commodity = commodity
        self.prices = prices
        self.arrivals = arrivals
        self.state = state
        self.market = market
        self.grade = grade
        self.process_data()
        
        
    def process_data(self):
        self.p, self.a = self.prep_data()
        
    
    def plotter(self):
        p = self.p.sort_values('date')
        a = self.a.sort_values('date')
        
        trace_max = go.Scatter(
            x=p.date,
            y=p['max_price'],
            connectgaps=False,
            name = "Max",
            line = dict(color = '#278ea5', dash='dash'),
            opacity = 1)


        trace_modal = go.Scatter(
            x=p.date,
            y=p['modal_price'],
            connectgaps=False,
            name = "Modal",
            line = dict(color = '#071e3d'),
            opacity = 1)


        trace_min = go.Scatter(
            x=p.date,
            y=p['min_price'],
            connectgaps=False,
            name = "Min",
            line = dict(color = '#278ea5', dash='dash'),
            opacity = 1)


        trace_fill = go.Scatter(
            x=p.date,
            y=p['max_price'],
            fill='tonexty',
            fillcolor='rgba(0,100,80,0.1)',
            line=dict(color='rgba(255,255,255,0)'),
            showlegend=False,
            hoverinfo='skip'
        )

        
        trace_arrivals = go.Bar(
            x=a.date,
            y=a['quantity'],
            name='Arrivals',
            marker=dict(
                color='#1f4287'
            )
        )

        data = [trace_max, trace_modal, trace_min, trace_fill, trace_arrivals]

        if not self.market:
            region = 'All Markets - {}'.format(self.state)
        else:
            region = self.market
        layout = dict(
            title='{} <br> {} <br> Market over Time'.format(self.commodity, region),
        #     paper_bgcolor='rgba(245, 246, 249, 0.4)',
        #     plot_bgcolor='rgba(245, 246, 249, 0.4)',
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=14,
                             label='2w',
                             step='day',
                             stepmode='backward'),
                        dict(count=31,
                             label='1m',
                             step='day',
                             stepmode='backward'),
                        dict(count=92,
                             label='3m',
                             step='day',
                             stepmode='backward'),
                        dict(step='all')
                    ])
                ),
                rangeslider=dict(
                    #visible = True
                ),
                type='date'
            ),
            yaxis=dict(
                range=[0, p['max_price'].max()+200],
                title='Arrivals in Tonnes --- Prices per Quintal'
            ),
            yaxis2=dict(
                title='Arrivals in Tonnes',
                overlaying='y',
                side='right'
            )
        )


        fig = dict(data=data, layout=layout)
        iplot(fig)


class Trends(object):
    """
    Wrapper - Pulls, processes, and plots market trends for a 
    given commodity, state, grade, and market. plot() takes 
    state, grade, and market arguments

    Args:
        commodity (str): Commodity to see availability of
        start (str): Start date of availability evaluation period
        end (str): End date of availability evaluation period; defaults to today

    Usage:
        t = Trends()
        t.plot()
        t.plot(state='Punjab')
        t.plot(market='Malout')
    """
    def __init__(self, commodity='Kinnow', start=None, end=None):
        self.commodity = commodity
        self.start = start
        self.end = end
        if not self.start:
            self.start = str(pd.to_datetime('today') - pd.Timedelta(days=92))
        if not self.end:
            self.end = str(pd.to_datetime('today').date())
        self.get_data()
                    
        
    def get_data(self):
        d = db.DBPuller(self.commodity, self.start, self.end)
        d.get_data()
        self.prices, self.arrivals, self.lm = d.prices, d.arrivals, d.lm
        
        
    def plot(self, state='Combined', market=None, grade='Medium'):
        tp = TrendPlotter(self.commodity, self.prices, self.arrivals, state, market, grade)
        tp.plotter()