
import numpy as np
import pandas as pd
from datetime import date
from copy import deepcopy
# from excel_plot import charts_backtest
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

class Evaluate(object):
    def __init__(self, df):
        self.df = df.copy()
        self.df['benchmark_ret']= self.wealth2return(self.df['benchmark'].values)
        self.df['strategy_ret']= self.wealth2return(self.df['strategy'].values)
        self.df['strategy_rela_ret']= self.df['strategy_ret'] - self.df['benchmark_ret']
        self.df['strategy_rela'] = self.df['strategy_rela_ret'].add(1).cumprod()
        self.zh_eng = {'RetTol':'总收益(%)','AnnRet':'年化收益(%)','AnnVol':'年化波动率(%)','SR':'夏普比率','WinRatio':'相对日胜率(%)',
                  'WinRatioPos':'上涨市相对日胜率(%)','WinRatioNeg':'下跌市相对日胜率(%)','MaxDD':'最大回撤(%)','MaxDDdays':'最大回撤天数',
                  'MaxDD_start':'最大回撤起始日','MaxDD_end':'最大回撤结束日','ExtRetTol':'总超额收益(%)','AnnExtRet':'年化超额收益(%)',
                  'AnnExtVol':'年化超额波动率(%)','IR':'信息率','sortinoR':'索提诺比率','calmarR':'卡玛比率'}

    def compute_metric(self, freq='Mon', hedge_signal=None, ifprint=True):
        def compute_abs(df,index):
            df_metric = pd.DataFrame(0, columns=['RetTol', 'AnnRet', 'AnnVol', 'SR', 'sortinoR','calmarR','WinRatio', 'WinRatioPos','WinRatioNeg', 'MaxDD',
                                                 'MaxDDdays','MaxDD_start', 'MaxDD_end'],index=[index])
            df_metric.loc[index, 'RetTol'] = self.profit(df['strategy'].values)
            df_metric.loc[index, 'AnnRet'] = self.profit_annual(df['strategy'].values, freq)
            df_metric.loc[index, ['AnnVol', 'SR']] = self.sharpe_ratio(df['strategy_ret'].values,df_metric.loc[index, 'AnnRet'], freq)
            df_metric.loc[index, 'sortinoR'] = self.sortino_ratio(df['strategy_ret'].values,df_metric.loc[index, 'AnnRet'], freq)
            df_metric.loc[index, 'MaxDD'], df_metric.loc[index, 'MaxDDdays'], df_metric.loc[index, 'MaxDD_start'], df_metric.loc[index, 'MaxDD_end'] = self.maxdd(df['strategy'])
            df_metric.loc[index, 'calmarR'] = self.calmar_ratio(df_metric.loc[index, 'AnnRet'], df_metric.loc[index, 'MaxDD'])
            if hedge_signal is None:
                df_metric.loc[index, 'WinRatio'], df_metric.loc[index, 'WinRatioPos'], df_metric.loc[index, 'WinRatioNeg'] = self.win_ratio_ret(df['strategy_ret'], df['benchmark_ret'])
            else:
                df_metric.loc[index, 'WinRatio'], df_metric.loc[index, 'WinRatioPos'], df_metric.loc[index, 'WinRatioNeg'] = self.win_ratio_signal(hedge_signal, df[['benchmark_ret']])
            return df_metric

        def compute_benchmark(df,index):
            df_metric = pd.DataFrame(0, columns=['RetTol', 'AnnRet', 'AnnVol', 'SR', 'sortinoR','calmarR', 'MaxDD',
                                                 'MaxDDdays','MaxDD_start', 'MaxDD_end'],index=[index])
            df_metric.loc[index, 'RetTol'] = self.profit(df['benchmark'].values)
            df_metric.loc[index, 'AnnRet'] = self.profit_annual(df['benchmark'].values, freq)
            df_metric.loc[index, ['AnnVol', 'SR']] = self.sharpe_ratio(df['benchmark_ret'].values,df_metric.loc[index, 'AnnRet'], freq)
            df_metric.loc[index, 'sortinoR'] = self.sortino_ratio(df['benchmark_ret'].values,df_metric.loc[index, 'AnnRet'],freq)
            df_metric.loc[index, 'MaxDD'], df_metric.loc[index, 'MaxDDdays'], df_metric.loc[index, 'MaxDD_start'], df_metric.loc[index, 'MaxDD_end'] = self.maxdd(df['benchmark'])
            df_metric.loc[index, 'calmarR'] = self.calmar_ratio(df_metric.loc[index, 'AnnRet'], df_metric.loc[index, 'MaxDD'])
            return df_metric

		def compute_rela(df,index):
			df_metric = pd.DataFrame(0, columns=['ExtRetTol', 'AnnExtRet', 'AnnExtVol', 'IR',
			                                     'MaxDD','MaxDDdays','MaxDD_start', 'MaxDD_end'],index=[index])
			df_metric.loc[index, 'ExtRetTol'] = self.profit(df['strategy_rela'].values)
			df_metric.loc[index, ['AnnExtRet', 'AnnExtVol', 'IR']] = self.information_ratio(df['strategy_rela_ret'], freq)
            df_metric.loc[index, 'sortinoR'] = self.sortino_ratio(df['strategy_rela_ret'].values,df_metric.loc[index, 'AnnExtRet'],freq)
			df_metric.loc[index, 'MaxDD'], df_metric.loc[index, 'MaxDDdays'], df_metric.loc[index, 'MaxDD_start'], df_metric.loc[index, 'MaxDD_end'] = self.maxdd(df['strategy_rela'])
			# df_metric.loc[index, 'WinRatio'], df_metric.loc[index, 'WinRatioPos'], df_metric.loc[index, 'WinRatioNeg'] = self.win_ratio(df['strategy_ret'], df['benchmark_ret'])
			return df_metric
        df_all_metric_bm = compute_benchmark(self.df.copy(),'overall')
		df_all_metric_abs = compute_abs(self.df.copy(),'overall')
		df_all_metric_rela = compute_rela(self.df.copy(),'overall')

		start_y = int(self.df.sort_index().index[0][:4])
		end_y = int(self.df.sort_index().index[-1][:4])
		years = list(range(end_y,start_y-1,-1))
		df_metrics_bm = [df_all_metric_bm]
		df_metrics_abs = [df_all_metric_abs]
		df_metrics_rela = [df_all_metric_rela]
		for y in years:
			df_y = self.df.loc[str(y)+'-01-01':str(y+1)+'-01-01']
			df_metrics_bm.append(compute_benchmark(df_y,str(y)))
			df_metrics_abs.append(compute_abs(df_y,str(y)))
			df_metrics_rela.append(compute_rela(df_y,str(y)))

		self.df_metrics_bm = pd.concat(df_metrics_bm)
		self.df_metrics_abs = pd.concat(df_metrics_abs)
		self.df_metrics_rela = pd.concat(df_metrics_rela)


    def save(self, df=None, df_metric_abs=None, df_metric_bm=None, df_metric_rela=None):
        if (df is not None) or (df is not None):
            if df is not None:
                df.to_excel('alyData/杠杆增强净值.xlsx')
            writer = pd.ExcelWriter('alyData/杠杆增强策略表现.xlsx', engine='xlsxwriter')

            if df_metric_abs is not None:
                columns = [(c, self.zh_eng[c]) for c in df_metric_abs.columns.values]
                df_metric_abs = df_metric_abs.rename(columns=dict(columns))
                df_metric_abs.to_excel(writer,sheet_name='策略绝对表现')
            if df_metric_bm is not None:
                columns = [(c, self.zh_eng[c]) for c in df_metric_bm.columns.values]
                df_metric_bm = df_metric_bm.rename(columns=dict(columns))
                df_metric_bm.to_excel(writer,sheet_name='基准表现')
            if df_metric_rela is not None:
                columns = [(c, self.zh_eng[c]) for c in df_metric_rela.columns.values]
                df_metric_rela = df_metric_rela.rename(columns=dict(columns))
                df_metric_rela.to_excel(writer, sheet_name='策略相对表现')
            df_metric = df_metric_abs[['总收益(%)','最大回撤(%)','最大回撤天数','夏普比率','索提诺比率','卡玛比率','相对日胜率(%)','上涨市相对日胜率(%)','下跌市相对日胜率(%)']].copy()
            df_metric.insert(1,'超额收益(%)',df_metric_rela['总超额收益(%)'])
            df_metric.loc['overall','总收益(%)'] = df_metric_abs.loc['overall','年化收益(%)']
            df_metric.loc['overall','超额收益(%)'] = df_metric_rela.loc['overall','年化超额收益(%)']
            df_metric = df_metric.rename(index={'overall':'overall(年化)'})
            df_metric.to_excel(writer, sheet_name='综合评价')
            writer.save()

    def year_metric(self):
        pass

    def wealth2return(self, wealth_seq):
        profits = np.diff(wealth_seq)
        ret = (profits + 0.0) / wealth_seq[:-1]
        ret = np.append([0.], ret)
        return ret

    def profit(self, wealth_seq):
        return (wealth_seq[-1] - wealth_seq[0]) / wealth_seq[0] * 100

    def profit_annual(self, wealth_seq, freq):
        if freq == 'Mon':
            return ((wealth_seq[-1] / wealth_seq[0]) ** (12. / (len(wealth_seq)-1)) - 1) * 100
        elif freq == 'Day':
            return ((wealth_seq[-1] / wealth_seq[0]) ** (252. / (len(wealth_seq)-1)) - 1) * 100

    def sharpe_ratio(self, return_seq, ann_ret, freq):
        if freq == 'Mon':
            ret_ann = (1+ann_ret - 1.04)
            std_ann = np.std(return_seq + 1e-8) * np.sqrt(12.)*100
            return [std_ann, ret_ann / std_ann]
        elif freq == 'Day':
            ret_ann = (1+ann_ret - 1.04)
            std_ann = (np.std(return_seq) + 1e-8) * np.sqrt(252.)*100
            return  [std_ann, ret_ann / std_ann]

    def sortino_ratio(self, return_seq, ann_ret, freq):
        if freq == 'Mon':
            ret_ann = (1+ann_ret - 1.04)
            std_ann = np.sqrt(np.mean(np.clip(return_seq,a_max=0,a_min=-np.inf)**2)) * np.sqrt(12.)*100
            return ret_ann / std_ann
        elif freq == 'Day':
            ret_ann = (1+ann_ret - 1.04)
            std_ann = np.sqrt(np.mean(np.clip(return_seq,a_max=0,a_min=-np.inf)**2)) * np.sqrt(252.)*100
            return  ret_ann / std_ann

    def calmar_ratio(self, ann_ret, maxdd):
        return ann_ret / (maxdd+1e-8)

    def information_ratio(self, return_seq, freq):
        total_wealth = np.cumprod(return_seq+1)
        ret_ann = self.profit_annual(total_wealth, freq)
        if freq == 'Mon':
            std_ann = np.std(return_seq + 1e-4) * np.sqrt(12.)*100
            return [ret_ann, std_ann, ret_ann / std_ann]
        elif freq == 'Day':
            std_ann = (np.std(return_seq) + 1e-4) * np.sqrt(252.)*100
            return  [ret_ann, std_ann, ret_ann/ std_ann]

    def maxdd(self, wealth_seq):  # maximum drawdown
        mdd = 0
        peak = wealth_seq.iloc[0]
        start_date = wealth_seq.index[0]
        tmp_start_date = start_date
        end_date = wealth_seq.index[0]
        for i in range(len(wealth_seq)):
            x = wealth_seq.iloc[i]
            if x > peak:
                peak = x
                tmp_start_date = wealth_seq.index[i]
            dd = (peak - x) / peak
            if dd > mdd:
                mdd = dd
                start_date = tmp_start_date
                end_date = wealth_seq.index[i]
        maxdd_days = len(wealth_seq.loc[start_date:end_date])+1
        return [mdd * 100, maxdd_days, start_date, end_date]

    def win_ratio_ret(self, strategy_ret, benchmark_ret):
        win_all = np.sum(strategy_ret >benchmark_ret) / (len(benchmark_ret) + 0.0) * 100
        win_pos = np.sum((strategy_ret > benchmark_ret ) & (benchmark_ret > 0)) / sum(benchmark_ret > 0) * 100
        win_neg = np.sum((strategy_ret > benchmark_ret) & (benchmark_ret < 0)) / sum(benchmark_ret < 0) * 100
        return win_all, win_pos, win_neg

    def win_ratio_signal(self, hedge_signal, benchmark_ret):
        hedge_signal['benchmark_ret'] = benchmark_ret > 0
        hedge_signal['benchmark_ret'] = hedge_signal['benchmark_ret'].shift(-1).fillna(1).astype(int)

        win_all = np.sum(hedge_signal['hedge'] == hedge_signal['benchmark_ret']) / (len(hedge_signal) + 0.0) * 100
        win_pos = np.sum((hedge_signal['hedge'] == 1) & (hedge_signal['benchmark_ret'] == 1)) / sum(hedge_signal['benchmark_ret'] == 1) * 100
        win_neg = np.sum((hedge_signal['hedge'] == 0) & (hedge_signal['benchmark_ret'] == 0)) / sum(hedge_signal['benchmark_ret'] == 0) * 100
        return win_all, win_pos, win_neg

    def plot_net_curve(self, df, name):
        dates = df.index.astype(str)
        x_date = [date(int(x[:4]), int(x[4:6]), int(x[6:])) for x in dates]
        x_date = np.array([x_date, x_date, x_date]).T
        lines = df.values
        labels = df.columns

        fig = plt.figure()
        ax = fig.add_subplot(111)

        plt.plot_date(x_date, lines, '-')

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        months = mdates.MonthLocator(interval=6)
        ax.xaxis.set_major_locator(months)
        fig.autofmt_xdate()

        plt.grid()
        plt.legend(labels)
        plt.savefig(name)
        plt.close()