import numpy as np
import pandas as pd
import calendar as cldr
import matplotlib.pyplot as plt

def get_clim_data(file,kind='p50',temporal=True,ro=False):
    cols = ['rcp%d' % t for t in [26,45,85]]
    df = pd.read_csv(file)
    name = df.columns[4][6:-4]
    if temporal:
        df['time'] = [int(t.split('-')[0]) for t in df['time']]
    else:
        df['time'] = [t[0]+'-'+t[1] for t in
                      [x.split('-')[:2] for x in df['time']]]
    df.rename(columns={'%s_%s_%s' % (t,name,kind):'%s' % t for t in cols},
              inplace=True)
    df.set_index('time',inplace=True)
    if ro:
        df = df[cols].groupby([df.index])[cols].mean().round().astype('int64')
    else:
        df = df[cols].groupby([df.index])[cols].mean()
    return df

def get_crop_data(file):
    df = pd.read_csv(file)[['REF_DATE','GEO','Type of crop','VALUE']]
    df.rename(columns={'REF_DATE':'time',
               'GEO':'province',
               'Type of crop':'type',
               'VALUE':'kg/hec'},
              inplace=True)
    df.set_index('province',inplace=True)
    return df

def is_leap_year(year):
    return year%4 == 0 and (year%400 == 0 or year%100 != 0)

def to_month_inner(num,year):
    large = {1,3,5,7,8,10,12}
    small = set(range(1,13)) - large
    if is_leap_year(year):
        feb = 29
    else:
        feb = 28
    month = 1
    while True:
        if month > 12:
            break;
        if month in small:
            num -= 30
        elif month in large:
            num -= 31
        else:
            num -= feb
        if num < 1:
            break;
        else:
            month += 1
    return month

def to_month(df):
    df.round()
    cols = ['rcp%d' % t for t in [26,45,85]]
    for year in df.index:
        for col in cols:
            df.at[year,col] = to_month_inner(df.at[year,col],year)
        
def get_precip(df1,df2,rcp):
    rain = []
    for year in df1.index:
        amount = []
        small = df1.loc[year,'spring']
        large = df1.loc[year,'fall']
        for y,m in [t.split('-') for t in df2.index]:
            if int(y) == year and int(m) <= large and int(m) >= small:
                amount.append(df2.loc[y+'-'+m,rcp])
        rain.append(sum(amount))
    return rain

def get_tmean(df1,df2,rcp):
    cumm = []
    for year in df1.index:
        amount = []
        small = df1.loc[year,'spring']
        large = df1.loc[year,'fall']
        for y,m in [t.split('-') for t in df2.index]:
            if int(y) == year and int(m) <= large and int(m) >= small:
                amount.append(df2.loc[y+'-'+m,rcp])
        cumm.append(sum(amount))
    return cumm

def ab_crop(kind):
    # spring data
    spring_leth = get_clim_data('leth-last-spring-frost.csv')
    spring_ed = get_clim_data('ed-last-spring-frost.csv')
    spring_ver = get_clim_data('ver-last-spring-frost.csv')
    spring = (spring_leth + spring_ed + spring_ver) / 3

    # fall data
    fall_leth = get_clim_data('leth-first-fall-frost.csv')
    fall_ed = get_clim_data('ed-first-fall-frost.csv')
    fall_ver = get_clim_data('ver-first-fall-frost.csv')
    fall = (fall_leth + fall_ed + fall_ver) / 3

    # frost free days
    frost = fall - spring

    # convert from day of year to month of year
    to_month(spring)
    to_month(fall)

    # precipitation 
    leth_precip = get_clim_data('leth-month-total-precip.csv',temporal=False)
    ed_precip = get_clim_data('ed-month-total-precip.csv',temporal=False)
    ver_precip = get_clim_data('ver-month-total-precip.csv',temporal=False)
    precip = (leth_precip + ed_precip + ver_precip) / 3

    # mean temperature
    leth_tmean = get_clim_data('leth-month-tmean.csv',temporal=False)
    ed_tmean = get_clim_data('ed-month-tmean.csv',temporal=False)
    ver_tmean = get_clim_data('ver-month-tmean.csv',temporal=False)
    tmean = (leth_tmean + ed_tmean + ver_tmean) / 3

    # days w/ tmax > 32
    leth_tmax = get_clim_data('leth-tmax.csv')
    ed_tmax = get_clim_data('ed-tmax.csv')
    ver_tmax = get_clim_data('ver-tmax.csv')
    tmax = (leth_tmax + ed_tmax + ver_tmax) / 3

    # grab data of parameter kind
    crops = get_crop_data('crop-yields.csv')['Alberta':'Alberta'].set_index('type')\
                                                       .dropna()
    crop26 = crops.loc[kind:kind].set_index('time') # for rcp26
    crop45 = crop26.copy() # for rcp45
    crop85 = crop26.copy() # for rcp85

    # get last frost of spring
    crop26['spring'] = spring['rcp26']
    crop45['spring'] = spring['rcp45']
    crop85['spring'] = spring['rcp85']

    # get first frost of fall
    crop26['fall'] = fall['rcp26']
    crop45['fall'] = fall['rcp45']
    crop85['fall'] = fall['rcp85']

    # get days w/ temp > 32
    crop26['days > 32'] = tmax['rcp26']
    crop45['days > 32'] = tmax['rcp45']
    crop85['days > 32'] = tmax['rcp85']
    
    # get frost free days
    crop26['frost free days'] = frost['rcp26']
    crop45['frost free days'] = frost['rcp45']
    crop85['frost free days'] = frost['rcp85']

    # get precipitation data
    crop26['precipitation'] = get_precip(crop26,precip,'rcp26')
    crop45['precipitation'] = get_precip(crop45,precip,'rcp45')
    crop85['precipitation'] = get_precip(crop85,precip,'rcp85')

    # get_tmean
    crop26['mean temperature'] = get_tmean(crop26,tmean,'rcp26')
    crop45['mean temperature'] = get_tmean(crop45,tmean,'rcp45')
    crop85['mean temperature'] = get_tmean(crop85,tmean,'rcp85')

    # drop spring and fall columns
    # reset index and drop time column
    crop26 = crop26.drop(columns=['spring','fall'])#.reset_index()
    crop45 = crop45.drop(columns=['spring','fall'])#.reset_index()
    crop85 = crop85.drop(columns=['spring','fall'])#.reset_index()

    # dictionary of data frames
    df = dict()
    df['rcp26'] = crop26
    df['rcp45'] = crop45
    df['rcp85'] = crop85

    return df

def bc_crop(kind):
    # spring data
    spring_kel = get_clim_data('kel-last-spring-frost.csv')
    spring_pg = get_clim_data('pg-last-spring-frost.csv')
    spring_lp = get_clim_data('lp-last-spring-frost.csv')
    spring = (spring_kel + spring_pg + spring_lp) / 3

    # fall data
    fall_kel = get_clim_data('kel-first-fall-frost.csv')
    fall_pg = get_clim_data('pg-first-fall-frost.csv')
    fall_lp = get_clim_data('lp-first-fall-frost.csv')
    fall = (fall_kel + fall_pg + fall_lp) / 3

    # frost free days
    frost = fall - spring

    # convert from day of year to month of year
    to_month(spring)
    to_month(fall)

    # precipitation 
    kel_precip = get_clim_data('kel-month-total-precip.csv',temporal=False)
    pg_precip = get_clim_data('pg-month-total-precip.csv',temporal=False)
    lp_precip = get_clim_data('lp-month-total-precip.csv',temporal=False)
    precip = (kel_precip + pg_precip + lp_precip) / 3

    # mean temperature
    kel_tmean = get_clim_data('kel-month-tmean.csv',temporal=False)
    pg_tmean = get_clim_data('pg-month-tmean.csv',temporal=False)
    lp_tmean = get_clim_data('lp-month-tmean.csv',temporal=False)
    tmean = (kel_tmean + pg_tmean + lp_tmean) / 3

    # days w/ tmax > 32
    kel_tmax = get_clim_data('kel-tmax.csv')
    pg_tmax = get_clim_data('pg-tmax.csv')
    lp_tmax = get_clim_data('lp-tmax.csv')
    tmax = (kel_tmax + pg_tmax + lp_tmax) / 3

    # grab data of parameter kind
    crops = get_crop_data('crop-yields.csv')['British Columbia':'British Columbia'].set_index('type')\
                                                       .dropna()
    crop26 = crops.loc[kind:kind].set_index('time') # for rcp26
    crop45 = crop26.copy() # for rcp45
    crop85 = crop26.copy() # for rcp85

    # get last frost of spring
    crop26['spring'] = spring['rcp26']
    crop45['spring'] = spring['rcp45']
    crop85['spring'] = spring['rcp85']

    # get first frost of fall
    crop26['fall'] = fall['rcp26']
    crop45['fall'] = fall['rcp45']
    crop85['fall'] = fall['rcp85']

    # get days w/ temp > 32
    crop26['days > 32'] = tmax['rcp26']
    crop45['days > 32'] = tmax['rcp45']
    crop85['days > 32'] = tmax['rcp85']
    
    # get frost free days
    crop26['frost free days'] = frost['rcp26']
    crop45['frost free days'] = frost['rcp45']
    crop85['frost free days'] = frost['rcp85']

    # get precipitation data
    crop26['precipitation'] = get_precip(crop26,precip,'rcp26')
    crop45['precipitation'] = get_precip(crop45,precip,'rcp45')
    crop85['precipitation'] = get_precip(crop85,precip,'rcp85')

    # get_tmean
    crop26['mean temperature'] = get_tmean(crop26,tmean,'rcp26')
    crop45['mean temperature'] = get_tmean(crop45,tmean,'rcp45')
    crop85['mean temperature'] = get_tmean(crop85,tmean,'rcp85')

    # drop spring and fall columns
    # reset index and drop time column
    crop26 = crop26.drop(columns=['spring','fall'])#.reset_index()
    crop45 = crop45.drop(columns=['spring','fall'])#.reset_index()
    crop85 = crop85.drop(columns=['spring','fall'])#.reset_index()

    # dictionary of data frames
    df = dict()
    df['rcp26'] = crop26
    df['rcp45'] = crop45
    df['rcp85'] = crop85

    return df

def sk_crop(kind):
    # spring data
    spring_re = get_clim_data('re-last-spring-frost.csv')
    spring_lr = get_clim_data('lr-last-spring-frost.csv')
    spring_sr = get_clim_data('sr-last-spring-frost.csv')
    spring = (spring_re + spring_lr + spring_sr) / 3

    # fall data
    fall_re = get_clim_data('re-first-fall-frost.csv')
    fall_lr = get_clim_data('lr-first-fall-frost.csv')
    fall_sr = get_clim_data('sr-first-fall-frost.csv')
    fall = (fall_re + fall_lr + fall_sr) / 3

    # frost free days
    frost = fall - spring

    # convert from day of year to month of year
    to_month(spring)
    to_month(fall)

    # precipitation 
    re_precip = get_clim_data('re-month-total-precip.csv',temporal=False)
    lr_precip = get_clim_data('lr-month-total-precip.csv',temporal=False)
    sr_precip = get_clim_data('sr-month-total-precip.csv',temporal=False)
    precip = (re_precip + lr_precip + sr_precip) / 3

    # mean temperature
    re_tmean = get_clim_data('re-month-tmean.csv',temporal=False)
    lr_tmean = get_clim_data('lr-month-tmean.csv',temporal=False)
    sr_tmean = get_clim_data('sr-month-tmean.csv',temporal=False)
    tmean = (re_tmean + lr_tmean + sr_tmean) / 3

    # days w/ tmax > 32
    re_tmax = get_clim_data('re-tmax.csv')
    lr_tmax = get_clim_data('lr-tmax.csv')
    sr_tmax = get_clim_data('sr-tmax.csv')
    tmax = (re_tmax + lr_tmax + sr_tmax) / 3

    # grab data of parameter kind
    crops = get_crop_data('crop-yields.csv')['Saskatchewan':'Saskatchewan'].set_index('type')\
                                                       .dropna()
    crop26 = crops.loc[kind:kind].set_index('time') # for rcp26
    crop45 = crop26.copy() # for rcp45
    crop85 = crop26.copy() # for rcp85

    # get last frost of spring
    crop26['spring'] = spring['rcp26']
    crop45['spring'] = spring['rcp45']
    crop85['spring'] = spring['rcp85']

    # get first frost of fall
    crop26['fall'] = fall['rcp26']
    crop45['fall'] = fall['rcp45']
    crop85['fall'] = fall['rcp85']

    # get days w/ temp > 32
    crop26['days > 32'] = tmax['rcp26']
    crop45['days > 32'] = tmax['rcp45']
    crop85['days > 32'] = tmax['rcp85']
    
    # get frost free days
    crop26['frost free days'] = frost['rcp26']
    crop45['frost free days'] = frost['rcp45']
    crop85['frost free days'] = frost['rcp85']

    # get precipitation data
    crop26['precipitation'] = get_precip(crop26,precip,'rcp26')
    crop45['precipitation'] = get_precip(crop45,precip,'rcp45')
    crop85['precipitation'] = get_precip(crop85,precip,'rcp85')

    # get_tmean
    crop26['mean temperature'] = get_tmean(crop26,tmean,'rcp26')
    crop45['mean temperature'] = get_tmean(crop45,tmean,'rcp45')
    crop85['mean temperature'] = get_tmean(crop85,tmean,'rcp85')

    # drop spring and fall columns
    # reset index and drop time column
    crop26 = crop26.drop(columns=['spring','fall'])#.reset_index()
    crop45 = crop45.drop(columns=['spring','fall'])#.reset_index()
    crop85 = crop85.drop(columns=['spring','fall'])#.reset_index()

    # dictionary of data frames
    df = dict()
    df['rcp26'] = crop26
    df['rcp45'] = crop45
    df['rcp85'] = crop85

    return df

def crop_dict(kind,prov):
    if prov == 'Alberta':
        return ab_crop(kind)
    if prov == 'British Columbia':
        return bc_crop(kind)
    if prov == 'Saskatchewan':
        return sk_crop(kind)

def crop_plot(df,all=False):
    if all:
        fig, ax = plt.subplots(5,1,figsize=(14,30))
        plt.subplots_adjust(wspace = 0.2)
        
        ax[0].scatter(df.index,df['kg/hec'],color='green')
        ax[0].plot(df.index,df['kg/hec'],color='yellow')
        ax[0].set_xlabel('Years')
        ax[0].set_ylabel('Yield (Kg/h)')

        b = df[['kg/hec','days > 32']].copy().sort_values('days > 32')
        ax[1].scatter(b['days > 32'],b['kg/hec'],color='green')
        ax[1].plot(b['days > 32'],b['kg/hec'],color='yellow')
        ax[1].set_xlabel('Days > 32 Degrees (C)')
        ax[1].set_ylabel('Yield (Kg/h)')

        b = df[['kg/hec','frost free days']].copy().sort_values('frost free days')
        ax[2].scatter(b['frost free days'],b['kg/hec'],color='green')
        ax[2].plot(b['frost free days'],b['kg/hec'],color='yellow')
        ax[2].set_xlabel('Frost Free Days')
        ax[2].set_ylabel('Yield (Kg/h)')

        b = df[['kg/hec','precipitation']].copy().sort_values('precipitation')
        ax[3].scatter(b['precipitation'],b['kg/hec'],color='green')
        ax[3].plot(b['precipitation'],b['kg/hec'],color='yellow')
        ax[3].set_xlabel('Cummulative Precipitation (mm)')
        ax[3].set_ylabel('Yield (Kg/h)')

        b = df[['kg/hec','mean temperature']].copy().sort_values('mean temperature')
        ax[4].scatter(b['mean temperature'],b['kg/hec'],color='green')
        ax[4].plot(b['mean temperature'],b['kg/hec'],color='yellow')
        ax[4].set_xlabel('Cummulative Monthly Mean Temperature')
        ax[4].set_ylabel('Yield (Kg/h)')
    else:
        fig, ax = plt.subplots(1,1,figsize=(14,6))
        
        ax.scatter(df.index,df['kg/hec'],color='green')
        ax.plot(df.index,df['kg/hec'],color='yellow')
        ax.set_xlabel('Years')
        ax.set_ylabel('Yield (Kg/h)')
